use super::*;
use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::route_proto::ExecLeaseMetadata;
use capanix_app_sdk::runtime::{
    ControlFrame, LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
    in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeHostDescriptor,
    RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState, RuntimeHostObjectType,
    RuntimeObjectDescriptor, RuntimeUnitTick, encode_runtime_exec_control,
    encode_runtime_host_grant_change, encode_runtime_unit_tick,
};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedWorkerInit, TypedWorkerRpc,
};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::ControlEvent;
use crate::FileMetaRecord;
use crate::query::models::QueryNode;
use crate::query::path::is_under_query_path;
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::execution_units::{
    SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use crate::runtime::routes::{
    METHOD_SOURCE_RESCAN, ROUTE_KEY_QUERY, ROUTE_KEY_SOURCE_RESCAN_CONTROL,
    ROUTE_KEY_SOURCE_RESCAN_INTERNAL, ROUTE_KEY_SOURCE_ROOTS_CONTROL, ROUTE_TOKEN_FS_META_INTERNAL,
    source_rescan_request_route_for, source_rescan_route_bindings_for,
};
use crate::sink::SinkFileMeta;
use crate::source::SourceLogicalRootHealthSnapshot;
use crate::state::cell::SignalCell;
use crate::workers::sink::SinkWorkerClientHandle;
#[cfg(target_os = "linux")]
mod real_nfs_lab {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app_real_nfs_lab.rs"
    ));
}

#[derive(Default)]
struct LoopbackWorkerBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: StdMutex<HashSet<String>>,
    close_history: StdMutex<Vec<String>>,
    changed: Notify,
}

impl LoopbackWorkerBoundary {
    fn closed_channels_snapshot(&self) -> Vec<String> {
        let mut channels = self
            .closed
            .lock()
            .expect("loopback closed lock")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        channels.sort();
        channels
    }

    fn close_history_snapshot(&self) -> Vec<String> {
        self.close_history
            .lock()
            .expect("loopback close history lock")
            .clone()
    }
}

struct SourceWorkerUpdateRootsHookReset;

impl Drop for SourceWorkerUpdateRootsHookReset {
    fn drop(&mut self) {
        clear_source_worker_update_roots_hook();
    }
}

struct SourceWorkerUpdateRootsErrorHookReset;

impl Drop for SourceWorkerUpdateRootsErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_update_roots_error_hook();
    }
}

struct SourceWorkerLogicalRootsSnapshotHookReset;

impl Drop for SourceWorkerLogicalRootsSnapshotHookReset {
    fn drop(&mut self) {
        clear_source_worker_logical_roots_snapshot_hook();
    }
}

#[test]
fn source_operation_attempt_timeout_clips_remaining_budget() {
    let deadline = clip_retry_deadline(
        std::time::Instant::now() + Duration::from_millis(250),
        Duration::from_secs(5),
    );
    let timeout = source_operation_attempt_timeout(deadline, Duration::from_secs(5))
        .expect("remaining budget should still admit an attempt timeout");

    assert!(
        timeout <= Duration::from_millis(250),
        "source operation timeout budgeting should clip each attempt to the remaining deadline instead of letting per-call loops overshoot their caller budget",
    );
}

#[test]
fn source_operation_attempt_timeout_owns_timeout_exhaustion_lane() {
    assert!(
        matches!(
            source_operation_attempt_timeout(
                std::time::Instant::now() - Duration::from_millis(1),
                Duration::from_secs(5),
            ),
            Err(SourceFailure {
                cause: CnxError::Timeout,
                reason: SourceFailureReason::RetryBudgetExhausted(
                    SourceRetryBudgetExhaustionKind::OperationAttempt,
                ),
            })
        ),
        "source operation timeout budgeting should own deadline exhaustion instead of leaving each operation loop to hand-roll timeout terminal lanes",
    );
}

#[test]
fn source_progress_state_only_releases_relevant_wait_reason() {
    let mut state = SourceProgressState::default();
    let baseline = state;
    state.bump(SourceProgressReason::ObservabilityUpdated);

    assert!(
        SourceWaitFor::Observability.satisfied_by(state, baseline),
        "observability progress should release observability waits",
    );
    assert!(
        !SourceWaitFor::ControlFlowRetry.satisfied_by(state, baseline),
        "observability refresh should not accidentally release control-flow retries",
    );

    let observability_only = state;
    state.bump(SourceProgressReason::WorkerClientReplaced);
    assert!(
        SourceWaitFor::ControlFlowRetry.satisfied_by(state, observability_only),
        "worker replacement should release control-flow retries",
    );
    assert!(
        SourceWaitFor::LogicalRoots.satisfied_by(state, observability_only),
        "worker replacement should release update-roots retries",
    );
}

#[test]
fn source_control_frame_machine_owns_retained_tick_entry_lane() {
    let mut machine = source_control_frame_test_machine();
    let action = SourceControlFrameStep::run(RetainedTickFastPathLane {
        signals: vec![source_control_state_test_activate_signal(
            SourceRuntimeUnit::Source,
            ROUTE_KEY_QUERY,
            1,
            vec![],
        )],
    });
    let complete = machine
        .advance(SourceControlFrameEvent::RetainedTickFastPathApplied)
        .expect("retained-tick fast-path completion should produce a terminal complete action");

    assert!(
        matches!(
            action.inspect(),
            Some(SourceControlFrameLaneInspect::RetainedTickFastPath)
        ) && complete.is_complete(),
        "source control-frame machine should own the retained-tick fast-path lane instead of leaving on_control_frame_with_timeouts to branch directly on retained_tick_only_wave",
    );
}

#[test]
fn source_control_frame_machine_owns_reconnect_retained_tick_replay_resume_lane() {
    let mut machine = source_control_frame_test_machine();
    machine.pending_reconnect_resume = Some(SourceControlFrameReconnectResume::RetainedTickReplay);
    let reconnect = SourceControlFrameStep::run(ReconnectLane);
    let replay = machine
        .advance(SourceControlFrameEvent::ReconnectCompleted)
        .expect("reconnect completion should resume into retained-tick replay");
    let complete = machine
        .advance(SourceControlFrameEvent::RetainedTickReplayCompleted)
        .expect("retained-tick replay completion should produce a terminal complete action");

    assert!(
        matches!(
            reconnect.inspect(),
            Some(SourceControlFrameLaneInspect::Reconnect)
        ) && matches!(
            replay.inspect(),
            Some(SourceControlFrameLaneInspect::RetainedTickReplay)
        ) && complete.is_complete(),
        "source control-frame machine should own retained-tick replay resumption through machine-held reconnect state instead of encoding reconnect continuation in the effect variant",
    );
}

#[test]
fn source_control_frame_machine_owns_generation_one_timeout_reset_retry_lane() {
    let mut machine = source_control_frame_test_machine();
    machine.timeout_reset_policy = SourceControlFrameTimeoutResetPolicy::GenerationOneActivateWave;
    let action = machine.action_after_error(CnxError::Timeout, SourceProgressState::default());
    let retry_action = machine
        .advance(SourceControlFrameEvent::ReconnectCompleted)
        .expect("generation-one timeout-like reset should resume with another attempt");
    let terminal_reconnect =
        machine.action_after_error(CnxError::ChannelClosed, SourceProgressState::default());
    let terminal_action = machine
        .advance(SourceControlFrameEvent::ReconnectCompleted)
        .expect("bridge-reset fail-fast reconnect should resume into a terminal action");

    assert!(
        matches!(
            action.inspect(),
            Some(SourceControlFrameLaneInspect::Reconnect)
        ) && matches!(
            retry_action.inspect(),
            Some(SourceControlFrameLaneInspect::Attempt)
        ) && matches!(
            terminal_reconnect.inspect(),
            Some(SourceControlFrameLaneInspect::FailClosedReconnect)
        ) && matches!(
            terminal_action.terminal_error(),
            Some(CnxError::ChannelClosed)
        ),
        "source control-frame machine should own generation-one reconnect retry and terminal bridge-reset resumes via machine-held reconnect state instead of composite reconnect effect variants",
    );
}

#[test]
fn source_control_frame_machine_owns_retry_resume_followup_lane() {
    let mut machine = source_control_frame_test_machine();
    let reconnect = machine.action_after_error(CnxError::Timeout, SourceProgressState::default());
    let wait = machine
        .advance(SourceControlFrameEvent::ReconnectCompleted)
        .expect("retry reconnect should resume into a wait-for-progress action");
    let action = machine
        .advance(SourceControlFrameEvent::WaitCompleted)
        .expect("wait completion should produce the next attempt action");

    assert!(
        matches!(
            reconnect.inspect(),
            Some(SourceControlFrameLaneInspect::Reconnect)
        ) && machine.existing_client_present
            && matches!(
                wait.inspect(),
                Some(SourceControlFrameLaneInspect::Wait { .. })
            )
            && matches!(
                action.inspect(),
                Some(SourceControlFrameLaneInspect::Attempt)
            ),
        "source control-frame machine should resume reconnect retry into wait and attempt lanes through machine-held reconnect state instead of composite reconnect effect variants",
    );
}

#[test]
fn source_control_frame_machine_fails_fast_restart_deferred_timeout_like_reset() {
    let mut machine = source_control_frame_test_machine();
    machine.bridge_reset_policy = SourceControlFrameBridgeResetPolicy::ReconnectThenFail;

    let reconnect = machine.action_after_error(
        CnxError::Internal("operation timed out".to_string()),
        SourceProgressState::default(),
    );
    let terminal = machine
        .advance(SourceControlFrameEvent::ReconnectCompleted)
        .expect("cleanup timeout-like reset should reconnect once before surfacing fail-closed");

    assert!(
        matches!(
            reconnect.inspect(),
            Some(SourceControlFrameLaneInspect::FailClosedReconnect)
        ) && matches!(
            terminal.terminal_error(),
            Some(CnxError::Internal(message)) if message.contains("operation timed out")
        ),
        "restart-deferred cleanup timeout-like stale bridge evidence must fail closed after one reconnect instead of spending the whole source control retry budget",
    );
}

#[test]
fn source_control_frame_machine_owns_non_retryable_terminal_lane() {
    let mut machine = source_control_frame_test_machine();

    assert!(
        matches!(
            machine.action_after_error(
                CnxError::InvalidInput("bad request".into()),
                SourceProgressState::default(),
            ),
            step if matches!(
                step.terminal_error(),
                Some(CnxError::InvalidInput(message)) if message == "bad request"
            )
        ),
        "source control-frame machine should own the non-retryable terminal lane instead of leaking a generic SourceOperationPhase::Failed through the wait executor",
    );
}

#[test]
fn source_control_frame_machine_owns_unexpected_response_protocol_failure_lane() {
    let mut machine = source_control_frame_test_machine();

    assert!(
        matches!(
            machine
                .advance(SourceControlFrameEvent::RpcCompleted {
                    rpc_result: Ok(SourceWorkerResponse::ScheduledGroupIds(Some(vec![
                        "group-a".to_string(),
                    ]))),
                    after: SourceProgressState::default(),
                })
                .expect("unexpected source worker response should still map into a terminal action"),
            step if matches!(
                step.terminal_error(),
                Some(CnxError::ProtocolViolation(message))
                    if message.contains("unexpected source worker response for on_control_frame")
            )
        ),
        "source control-frame machine should own unexpected-response protocol failure classification instead of leaving on_control_frame_with_timeouts to hand-roll a separate Ok(other) terminal branch",
    );
}

#[test]
fn source_control_frame_machine_owns_post_ack_schedule_refresh_followup_lane() {
    let mut machine = source_control_frame_test_machine();

    machine.decoded_signals =
        SourceControlFrameDecodedSignals::Decoded(vec![source_control_state_test_activate_signal(
            SourceRuntimeUnit::Source,
            ROUTE_KEY_QUERY,
            1,
            vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["node-a::nfs1".to_string()],
            }],
        )]);
    machine.post_ack_refresh_requirement = SourceControlFramePostAckRefreshRequirement::Required;
    machine.primed_local_schedule = Some(PrimedLocalScheduleSummary {
        saw_activate_with_bound_scopes: true,
        has_local_runnable_groups: true,
    });
    let post_ack = machine
        .advance(SourceControlFrameEvent::RpcCompleted {
            rpc_result: Ok(SourceWorkerResponse::Ack),
            after: SourceProgressState::default(),
        })
        .expect("ack should produce a post-ack cache side-effects action");
    let arm_replay = machine
        .advance(SourceControlFrameEvent::PostAckCacheAndSignalsRetained)
        .expect("post-ack side effects should resume into replay arming before scheduled-groups refresh");
    let pending_refresh = matches!(
        machine.pending_refresh,
        Some(SourceControlFramePendingRefresh {
            scheduled_groups_expectation:
                SourceScheduledGroupsExpectation::ExpectLocalRunnableGroups,
            next_step: SourceControlFramePendingRefreshNextStep::ArmReplay,
        })
    );
    let refresh = machine
        .advance(SourceControlFrameEvent::ArmReplayCompleted)
        .expect("replay arming should resume into scheduled-groups refresh");

    assert!(
        matches!(
            post_ack.inspect(),
            Some(
                SourceControlFrameLaneInspect::ApplyPostAckCacheAndRetainSignals {
                    cache_priming: SourceControlFramePostAckCachePriming::ReplayRecovery,
                }
            )
        ) && matches!(
            arm_replay.inspect(),
            Some(SourceControlFrameLaneInspect::ArmReplay)
        ) && pending_refresh
            && matches!(
                refresh.inspect(),
                Some(SourceControlFrameLaneInspect::RefreshScheduledGroups {
                    scheduled_groups_expectation:
                        SourceScheduledGroupsExpectation::ExpectLocalRunnableGroups,
                    ..
                })
            ),
        "source control-frame machine should keep post-ack refresh continuation in machine-held resume state instead of encoding it in composite effect names",
    );
}

#[test]
fn source_control_frame_machine_skips_replay_recovery_cache_priming_without_refresh_followup() {
    let mut machine = source_control_frame_test_machine();

    machine.decoded_signals =
        SourceControlFrameDecodedSignals::Decoded(vec![source_control_state_test_activate_signal(
            SourceRuntimeUnit::Source,
            ROUTE_KEY_QUERY,
            1,
            vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["node-a::nfs1".to_string()],
            }],
        )]);
    machine.post_ack_refresh_requirement = SourceControlFramePostAckRefreshRequirement::NotRequired;
    machine.primed_local_schedule = Some(PrimedLocalScheduleSummary {
        saw_activate_with_bound_scopes: true,
        has_local_runnable_groups: true,
    });

    let post_ack = machine
        .advance(SourceControlFrameEvent::RpcCompleted {
            rpc_result: Ok(SourceWorkerResponse::Ack),
            after: SourceProgressState::default(),
        })
        .expect("ack without refresh followup should still produce a post-ack cache action");

    assert!(
        matches!(
            post_ack.inspect(),
            Some(
                SourceControlFrameLaneInspect::ApplyPostAckCacheAndRetainSignals {
                    cache_priming: SourceControlFramePostAckCachePriming::Standard,
                }
            )
        ) && machine.pending_refresh.is_none(),
        "source control-frame machine should keep replay-recovery cache priming inside explicit post-ack effect payload instead of running replay-recovery priming unconditionally",
    );
}

#[test]
fn source_control_frame_machine_routes_decode_failed_ack_refresh_without_replay_recovery() {
    let mut machine = source_control_frame_test_machine();

    let refresh = machine
        .advance(SourceControlFrameEvent::RpcCompleted {
            rpc_result: Ok(SourceWorkerResponse::Ack),
            after: SourceProgressState::default(),
        })
        .expect("decode-failed ack should still route into scheduled-groups refresh");

    assert!(
        matches!(
            refresh.inspect(),
            Some(SourceControlFrameLaneInspect::RefreshScheduledGroups {
                scheduled_groups_expectation:
                    SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups,
                ..
            })
        ) && matches!(
            machine.pending_refresh,
            Some(SourceControlFramePendingRefresh {
                scheduled_groups_expectation:
                    SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups,
                next_step: SourceControlFramePendingRefreshNextStep::RefreshScheduledGroups,
            })
        ),
        "source control-frame machine should keep decode-failed refresh planning as explicit next-step plus classification truth instead of overloading one replay-recovery bool",
    );
}

#[test]
fn source_control_frame_machine_completes_post_ack_without_pending_refresh_followup() {
    let mut machine = source_control_frame_test_machine();

    let step = machine
        .advance(SourceControlFrameEvent::PostAckCacheAndSignalsRetained)
        .expect("post-ack completion without pending refresh should complete directly");

    assert!(
        step.is_complete(),
        "source control-frame machine should use machine-held pending refresh truth instead of duplicating post-ack followup state in the completion event",
    );
}

#[test]
fn source_control_frame_machine_observes_post_ack_summary_before_cache_side_effects() {
    let source_impl = include_str!("../source.rs");

    for required_symbol in [
        "prime_cached_control_summary_from_control_signals(",
        "cache.observability_control_summary_override_by_node = None;",
        "cache_priming: SourceControlFramePostAckCachePriming",
    ] {
        assert!(
            source_impl.contains(required_symbol),
            "workers/source control-frame hard cut regressed; ACK planning should use explicit observe/apply split before side effects: {required_symbol}",
        );
    }
    assert!(
        !source_impl.contains("fn observe_post_ack_control_frame_cache("),
        "workers/source control-frame hard cut regressed; single-use post-ack observe helper returned to production",
    );

    for legacy_symbol in [
        "fn prime_control_frame_summary_cache(",
        "fn clear_observability_control_summary_override(",
        "fn set_observability_control_summary_override_from_last_signals(",
        "fn prime_post_ack_schedule_cache(",
        "fn prime_post_ack_replay_recovery_cache(",
        "fn prime_post_ack_control_frame_cache(",
        "self.prime_post_ack_control_frame_cache(signals)",
        "fn apply_post_ack_control_frame_cache_side_effects(",
        "fn apply_control_frame_fast_path_signals(",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source control-frame hard cut regressed; ACK planning still mutates cache before the machine selects its followup lane: {legacy_symbol}",
        );
    }
}

fn source_control_frame_test_machine() -> SourceControlFrameMachine {
    SourceControlFrameMachine {
        envelopes: std::sync::Arc::<[ControlEnvelope]>::from([]),
        operation_deadline: clip_retry_deadline(
            std::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        ),
        rpc_timeout: Duration::from_millis(250),
        decoded_signals: SourceControlFrameDecodedSignals::DecodeFailed,
        primed_local_schedule: None,
        post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement::NotRequired,
        bootstrap_disposition: SourceControlFrameBootstrapDisposition::PostInitial,
        existing_client_present: false,
        bridge_reset_policy: SourceControlFrameBridgeResetPolicy::RetryAfterReconnect,
        timeout_reset_policy: SourceControlFrameTimeoutResetPolicy::Standard,
        attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy::Standard,
        timeout_reset_observation: SourceControlFrameTimeoutResetObservation::Clear,
        pending_reconnect_resume: None,
        pending_refresh: None,
    }
}

fn source_machine_test_future_deadline() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(1)
}

fn source_machine_test_expired_deadline() -> std::time::Instant {
    std::time::Instant::now() - Duration::from_millis(1)
}

fn source_retry_budget_exhausted(
    failure: &SourceFailure,
    kind: SourceRetryBudgetExhaustionKind,
) -> bool {
    matches!(
        failure.reason,
        SourceFailureReason::RetryBudgetExhausted(actual) if actual == kind
    ) && matches!(failure.as_error(), CnxError::Timeout)
}

#[test]
fn source_force_find_machine_owns_backoff_retry_and_timeout_terminal_lanes() {
    let machine = SourceForceFindMachine::new(source_machine_test_future_deadline());
    let wait = machine
        .advance(SourceForceFindEvent::AttemptCompleted {
            rpc_result: Err(CnxError::ProtocolViolation(
                "unexpected correlation_id in reply batch".into(),
            )),
        })
        .expect("retryable force-find correlation mismatch should enter explicit backoff lane");
    let retry = machine
        .advance(SourceForceFindEvent::WaitCompleted)
        .expect("backoff completion should resume the force-find attempt lane");
    let expired_machine = SourceForceFindMachine::new(source_machine_test_expired_deadline());
    let terminal = expired_machine
        .advance(SourceForceFindEvent::AttemptCompleted {
            rpc_result: Err(CnxError::ProtocolViolation(
                "unexpected correlation_id in reply batch".into(),
            )),
        })
        .expect("expired force-find deadline should produce a terminal timeout lane");

    assert!(
        matches!(wait, SourceForceFindEffect::RetryBackoff { .. })
            && matches!(retry, SourceForceFindEffect::Attempt { .. })
            && matches!(
                terminal,
                SourceForceFindEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    )
            ),
        "source force-find machine should own explicit backoff-retry and deadline-timeout lanes instead of drifting back to an ad hoc retry loop",
    );
}

#[test]
fn source_scheduled_group_ids_machine_owns_reconnect_retry_and_timeout_terminal_lanes() {
    let mut machine = SourceScheduledGroupIdsMachine::new(source_machine_test_future_deadline());
    let reconnect = machine
        .advance(SourceScheduledGroupIdsEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("retryable scheduled-group-ids timeout should enter explicit reconnect lane");
    let wait = machine
        .advance(SourceScheduledGroupIdsEvent::ReconnectCompleted)
        .expect("reconnect completion should enter explicit scheduled-group-ids wait lane");
    let retry = machine
        .advance(SourceScheduledGroupIdsEvent::WaitCompleted)
        .expect("wait completion should resume scheduled-group-ids attempt lane");
    let mut expired_machine =
        SourceScheduledGroupIdsMachine::new(source_machine_test_expired_deadline());
    let terminal = expired_machine
        .advance(SourceScheduledGroupIdsEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("expired scheduled-group-ids deadline should produce a terminal timeout lane");

    assert!(
        matches!(reconnect, SourceScheduledGroupIdsEffect::Reconnect)
            && matches!(wait, SourceScheduledGroupIdsEffect::Wait { .. })
            && matches!(retry, SourceScheduledGroupIdsEffect::Attempt { .. })
            && matches!(
                terminal,
                SourceScheduledGroupIdsEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    )
            ),
        "source scheduled-group-ids machine should own explicit reconnect and wait retry lanes instead of composite reconnect continuation",
    );
}

#[test]
fn source_start_machine_owns_reconnect_retry_and_timeout_terminal_lanes() {
    let mut machine = SourceStartMachine::new(source_machine_test_future_deadline());
    let reconnect = machine
        .advance(SourceStartEvent::AttemptCompleted {
            start_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("retryable start timeout should enter explicit reconnect lane");
    let wait = machine
        .advance(SourceStartEvent::ReconnectCompleted)
        .expect("reconnect completion should enter explicit start wait lane");
    let retry = machine
        .advance(SourceStartEvent::WaitCompleted)
        .expect("wait completion should resume start attempt lane");
    let mut expired_machine = SourceStartMachine::new(source_machine_test_expired_deadline());
    let terminal = expired_machine
        .advance(SourceStartEvent::AttemptCompleted {
            start_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("expired start deadline should produce a terminal timeout lane");

    assert!(
        matches!(reconnect, SourceStartEffect::Reconnect)
            && matches!(wait, SourceStartEffect::Wait { .. })
            && matches!(retry, SourceStartEffect::Attempt { .. })
            && matches!(
                terminal,
                SourceStartEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    )
            ),
        "source start machine should own explicit reconnect and wait retry lanes instead of composite reconnect continuation",
    );
}

#[test]
fn source_scheduled_groups_refresh_machine_owns_reconnect_retry_and_timeout_terminal_lanes() {
    let mut machine = SourceScheduledGroupsRefreshMachine::new(
        source_machine_test_future_deadline(),
        SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups,
        SourceScheduledGroupsRefreshDisposition::OrdinaryScheduleRefresh,
    );
    let reconnect = machine.advance(SourceScheduledGroupsRefreshEvent::EnsureStartedCompleted {
        result: Err(SourceFailure::timeout_reset()),
        after: SourceProgressState::default(),
    });
    let wait = machine.advance(SourceScheduledGroupsRefreshEvent::ReconnectCompleted);
    let retry = machine.advance(SourceScheduledGroupsRefreshEvent::WaitCompleted);
    let mut expired_machine = SourceScheduledGroupsRefreshMachine::new(
        source_machine_test_expired_deadline(),
        SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups,
        SourceScheduledGroupsRefreshDisposition::OrdinaryScheduleRefresh,
    );
    let terminal =
        expired_machine.advance(SourceScheduledGroupsRefreshEvent::EnsureStartedCompleted {
            result: Err(SourceFailure::timeout_reset()),
            after: SourceProgressState::default(),
        });

    assert!(
        matches!(reconnect, SourceScheduledGroupsRefreshEffect::Reconnect)
            && matches!(wait, SourceScheduledGroupsRefreshEffect::Wait { .. })
            && matches!(retry, SourceScheduledGroupsRefreshEffect::EnsureStarted)
            && matches!(
                terminal,
                SourceScheduledGroupsRefreshEffect::Fail(SourceFailure {
                    cause: CnxError::Internal(_),
                    reason: SourceFailureReason::RefreshExhausted(
                        SourceScheduledGroupsRefreshExhaustionReason::Timeout
                    ),
                })
            ),
        "source scheduled-groups refresh machine should own explicit reconnect and wait retry lanes instead of drifting back to composite reconnect helpers",
    );
}

#[test]
fn source_update_logical_roots_machine_owns_wait_retry_and_timeout_terminal_lanes() {
    let machine = SourceUpdateLogicalRootsMachine::new(source_machine_test_future_deadline());
    let wait = machine
        .advance(SourceUpdateLogicalRootsEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("retryable update-logical-roots timeout should enter explicit wait lane");
    let retry = machine
        .advance(SourceUpdateLogicalRootsEvent::WaitCompleted)
        .expect("wait completion should resume update-logical-roots attempt lane");
    let expired_machine =
        SourceUpdateLogicalRootsMachine::new(source_machine_test_expired_deadline());
    let terminal = expired_machine
        .advance(SourceUpdateLogicalRootsEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("expired update-logical-roots deadline should produce a terminal timeout lane");

    assert!(
        matches!(wait, SourceUpdateLogicalRootsEffect::Wait { .. })
            && matches!(retry, SourceUpdateLogicalRootsEffect::Attempt { .. })
            && matches!(
                terminal,
                SourceUpdateLogicalRootsEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    )
            ),
        "source update-logical-roots machine should own explicit wait-retry and deadline-timeout lanes instead of drifting back to an ad hoc retry loop",
    );
}

#[test]
fn source_observability_snapshot_machine_owns_reconnect_retry_and_timeout_terminal_lanes() {
    let machine = SourceObservabilitySnapshotMachine::new(
        source_machine_test_future_deadline(),
        Duration::from_millis(250),
    );
    let reconnect = machine
        .advance(SourceObservabilitySnapshotEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("retryable observability timeout should enter explicit reconnect lane");
    let retry = machine
        .advance(SourceObservabilitySnapshotEvent::ReconnectCompleted)
        .expect("reconnect completion should resume observability attempt lane");
    let expired_machine = SourceObservabilitySnapshotMachine::new(
        source_machine_test_expired_deadline(),
        Duration::from_millis(250),
    );
    let terminal = expired_machine
        .advance(SourceObservabilitySnapshotEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        })
        .expect("expired observability deadline should produce a terminal timeout lane");

    assert!(
        matches!(reconnect, SourceObservabilitySnapshotEffect::Reconnect)
            && matches!(retry, SourceObservabilitySnapshotEffect::Attempt { .. })
            && matches!(
                terminal,
                SourceObservabilitySnapshotEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::OperationWait,
                    )
            ),
        "source observability machine should own explicit reconnect-retry and deadline-timeout lanes instead of helper-owned retry state",
    );
}

#[test]
fn source_replay_retained_control_state_machine_reconnects_once_then_fails_closed() {
    let mut machine = SourceReplayRetainedControlStateMachine::new(
        source_machine_test_future_deadline(),
        SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT,
    );
    let reconnect = machine.advance(SourceReplayRetainedControlStateEvent::AttemptCompleted {
        rpc_result: Err(CnxError::Timeout),
        after: SourceProgressState::default(),
    });
    let fail = machine.advance(SourceReplayRetainedControlStateEvent::ReconnectCompleted);
    let mut expired_machine = SourceReplayRetainedControlStateMachine::new(
        source_machine_test_expired_deadline(),
        SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT,
    );
    let terminal =
        expired_machine.advance(SourceReplayRetainedControlStateEvent::AttemptCompleted {
            rpc_result: Err(CnxError::Timeout),
            after: SourceProgressState::default(),
        });

    assert!(
        matches!(reconnect, SourceReplayRetainedControlStateEffect::Reconnect)
            && matches!(
                fail,
                SourceReplayRetainedControlStateEffect::Fail(SourceFailure {
                    cause: CnxError::Timeout,
                    reason: SourceFailureReason::ControlReset(
                        SourceWorkerControlResetKind::Timeout
                    ),
                })
            )
            && matches!(
                terminal,
                SourceReplayRetainedControlStateEffect::Fail(err)
                    if source_retry_budget_exhausted(
                        &err,
                        SourceRetryBudgetExhaustionKind::ControlFrameRetry,
                    )
            ),
        "source retained-control replay recovery must reconnect once and fail closed back to the app boundary instead of retrying retained route-state replay inside the worker-client loop",
    );
}

#[test]
fn source_replay_retained_control_state_attempt_timeout_keeps_refresh_recovery_margin() {
    assert!(
        SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT < SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT,
        "post-ack scheduled-group refresh needs retry margin beyond any single retained-control replay attempt"
    );

    let machine = SourceReplayRetainedControlStateMachine::new(
        std::time::Instant::now() + SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT,
        SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT,
    );
    let SourceReplayRetainedControlStateEffect::Attempt { timeout } = machine.start() else {
        panic!("fresh retained-control replay should start with an attempt");
    };

    assert!(
        timeout <= SOURCE_WORKER_SCHEDULE_REFRESH_RPC_TIMEOUT,
        "retained-control replay inside post-ack schedule refresh must use the short per-RPC cap, got {timeout:?}"
    );
}

#[test]
fn source_replay_retained_control_state_attempt_timeout_uses_control_budget_for_repair() {
    let machine = SourceReplayRetainedControlStateMachine::new(
        std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    );
    let SourceReplayRetainedControlStateEffect::Attempt { timeout } = machine.start() else {
        panic!("fresh retained-control repair replay should start with an attempt");
    };

    assert_eq!(
        timeout, SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        "source repair retained-control replay must use the standard control RPC budget instead of the short schedule-refresh probe cap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_scheduled_groups_refresh_total_deadline_bounds_delayed_fetch_lane() {
    struct SourceWorkerScheduledGroupsRefreshDelayHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshDelayHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_delay_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");
    let worker_instance_id = client.worker_instance_id_for_tests().await;

    let _delay_reset = SourceWorkerScheduledGroupsRefreshDelayHookReset;
    install_source_worker_scheduled_groups_refresh_delay_hook(
        SourceWorkerScheduledGroupsRefreshDelayHook {
            delays: std::collections::VecDeque::from([Duration::from_millis(500)]),
            sticky_worker_instance_id: Some(worker_instance_id),
        },
    );

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        client.refresh_cached_scheduled_groups_from_live_worker_until(
            std::time::Instant::now() + Duration::from_millis(50),
            SourceScheduledGroupsExpectation::ExpectLocalRunnableGroups,
        ),
    )
    .await
    .expect("scheduled-groups refresh must honor its own total deadline");
    let err = result.expect_err("delayed fetch lane should exhaust the refresh deadline");
    let CnxError::Internal(message) = err.into_error() else {
        panic!("deadline exhaustion should surface as a sharp refresh exhaustion error");
    };
    assert!(
        message.contains("post-ack scheduled-groups refresh exhausted before scheduled groups converged (timeout)"),
        "delayed fetch lane should be classified as refresh timeout, got: {message}"
    );

    client.close().await.expect("close source worker");
}

#[test]
fn source_operations_hard_cut_removed_generic_operation_machine_family() {
    let source_impl = include_str!("../source.rs");

    for required_symbol in [
        "fn source_operation_attempt_timeout(",
        "enum SourceOperationLoopStep<",
        "async fn drive_source_machine_loop<",
        "struct SourceForceFindMachine {",
        "struct SourceScheduledGroupIdsMachine {",
        "struct SourceUpdateLogicalRootsMachine {",
        "struct SourceObservabilitySnapshotMachine {",
        "struct SourceStartMachine {",
        "struct SourceReplayRetainedControlStateMachine {",
        "struct SourceScheduledGroupsRefreshMachine {",
        "enum SourceScheduledGroupsRefreshDisposition {",
        "enum SourceScheduledGroupsRefreshRecoveryObservation {",
        "enum SourceScheduledGroupsRefreshExhaustionReason {",
        "enum SourceFailureReason {",
        "struct SourceFailure {",
        "fn classify_source_scheduled_groups_refresh_exhaustion(",
        "fn shape_source_scheduled_groups_refresh_exhaustion(",
        "enum SourceRetryBudgetExhaustionKind {",
        "fn shape_source_retry_budget_exhaustion(",
        "fn into_error(self) -> CnxError {",
        "enum SourceReconnectRetryDisposition {",
        "enum SourceWaitRetryDisposition {",
        "struct SourceRetryMachine {",
        "fn reconnect_after_update_roots_gap(",
        "fn wait_after_retryable_gap(",
        "fn take_pending_reconnect_wait_after(",
        "fn missing_source_reconnect_wait_after(",
        "fn missing_control_frame_pending_reconnect_resume(",
        "fn missing_control_frame_pending_refresh_state(",
        "fn expect_source_worker_ack(",
        "fn unexpected_source_worker_response_failure(",
        "fn unexpected_source_worker_response_result<",
        "fn map_source_timeout_result_with_failure<T>(",
        "enum SourceRetryBudgetDisposition {",
        "fn classify_source_retry_budget(",
        "enum SourceWorkerControlResetKind {",
        "fn classify_source_worker_control_reset(",
        "fn can_use_cached_grant_derived_snapshot(",
        "fn replay_state(&self) -> SourceControlReplayState {",
        "fn take_replay_state(&mut self) -> SourceControlReplayState {",
        "enum SourceForceFindEffect {",
        "enum SourceForceFindEvent {",
        "enum SourceScheduledGroupIdsEffect {",
        "enum SourceScheduledGroupIdsEvent {",
        "enum SourceUpdateLogicalRootsEffect {",
        "enum SourceUpdateLogicalRootsEvent {",
        "enum SourceObservabilitySnapshotEffect {",
        "enum SourceObservabilitySnapshotEvent {",
        "SourceObservabilitySnapshotEffect::Reconnect",
        "SourceObservabilitySnapshotEvent::ReconnectCompleted",
        "enum SourceStartEffect {",
        "enum SourceStartEvent {",
        "enum SourceReplayRetainedControlStateEffect {",
        "enum SourceReplayRetainedControlStateEvent {",
        "enum SourceScheduledGroupsRefreshEffect {",
        "enum SourceScheduledGroupsRefreshEvent {",
        "EnsureStartedCompleted {\n        result: std::result::Result<(), SourceFailure>,",
        "ClientAcquired {\n        result: std::result::Result<TypedWorkerClient<SourceWorkerRpc>, SourceFailure>,",
        "GrantsRefreshed {\n        result: std::result::Result<SourceScheduledGroupsRefreshGrantedClient, SourceFailure>,",
        "FetchScheduledGroupsCompleted {\n        result: std::result::Result<SourceScheduledGroupsRefreshFetchedGroups, SourceFailure>,",
        "pending_reconnect_wait_after: Option<SourceProgressState>",
        "SourceScheduledGroupIdsEffect::Reconnect",
        "SourceScheduledGroupIdsEffect::Wait {",
        "SourceScheduledGroupIdsEvent::ReconnectCompleted",
        "SourceScheduledGroupIdsEvent::WaitCompleted",
        "SourceStartEffect::Reconnect",
        "SourceStartEffect::Wait {",
        "SourceStartEvent::ReconnectCompleted",
        "SourceStartEvent::WaitCompleted",
        "SourceReplayRetainedControlStateEffect::Reconnect",
        "SourceReplayRetainedControlStateEffect::Wait {",
        "SourceReplayRetainedControlStateEvent::ReconnectCompleted",
        "SourceReplayRetainedControlStateEvent::WaitCompleted",
        "SourceScheduledGroupsRefreshEffect::Reconnect",
        "SourceScheduledGroupsRefreshDisposition::OrdinaryScheduleRefresh",
        "SourceScheduledGroupsRefreshDisposition::ReplayRequiredRecovery",
        "SourceScheduledGroupsRefreshEffect::Wait {",
        "SourceScheduledGroupsRefreshEvent::ReconnectCompleted",
        "SourceScheduledGroupsRefreshEvent::WaitCompleted",
        "async fn execute_force_find_attempt(",
        "async fn execute_scheduled_group_ids_attempt(",
        "async fn execute_update_logical_roots_attempt(",
        "async fn execute_update_logical_roots_wait(",
        "async fn execute_observability_snapshot_attempt(",
        "async fn execute_start_attempt(",
        "async fn execute_scheduled_groups_refresh_ensure_started(",
        "async fn execute_scheduled_groups_refresh_acquire_client(",
        "async fn execute_scheduled_groups_refresh_grants_snapshot(",
        "async fn execute_scheduled_groups_refresh_grants(",
        "async fn execute_scheduled_groups_refresh_fetch(",
        "fn execute_scheduled_groups_refresh_commit_groups(",
    ] {
        assert!(
            source_impl.contains(required_symbol),
            "workers/source hard cut regressed; operation-local retry ownership should stay inside dedicated source machines and handle-level executors: {required_symbol}",
        );
    }

    assert_eq!(
        source_impl
            .matches("unexpected source worker response ")
            .count(),
        1,
        "workers/source hard cut regressed; unexpected source-worker response protocol-violation formatting should stay centralized in typed unexpected-response helpers instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches(" reconnect completion without pending wait")
            .count(),
        1,
        "workers/source hard cut regressed; missing reconnect-wait protocol-violation formatting should stay centralized in missing_source_reconnect_wait_after(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches(" without pending control-frame resume")
            .count(),
        1,
        "workers/source hard cut regressed; missing control-frame reconnect-resume protocol-violation formatting should stay centralized in missing_control_frame_pending_reconnect_resume(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches(" without pending control-frame refresh state")
            .count(),
        1,
        "workers/source hard cut regressed; missing control-frame refresh-state protocol-violation formatting should stay centralized in missing_control_frame_pending_refresh_state(...) instead of drifting back into per-callsite strings",
    );
    assert!(
        !source_impl.contains("fn map_source_timeout_result<"),
        "workers/source hard cut regressed; zero-call raw timeout result helper should stay removed",
    );
    assert_eq!(
        source_impl
            .matches("saturating_duration_since(std::time::Instant::now())")
            .count(),
        1,
        "workers/source hard cut regressed; deadline exhaustion calculation should stay centralized in classify_source_retry_budget(...) instead of drifting back into per-machine now-vs-deadline checks",
    );
    assert_eq!(
        source_impl.matches("worker not initialized").count(),
        1,
        "workers/source hard cut regressed; worker-not-initialized taxonomy should stay centralized in classify_source_worker_control_reset(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches("unexpected correlation_id in reply batch")
            .count(),
        1,
        "workers/source hard cut regressed; unexpected-correlation taxonomy should stay centralized in classify_source_worker_control_reset(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches("invalid or revoked grant attachment token")
            .count(),
        1,
        "workers/source hard cut regressed; invalid-grant taxonomy should stay centralized in classify_source_worker_control_reset(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches("missing route state for channel_buffer")
            .count(),
        1,
        "workers/source hard cut regressed; missing-route-state taxonomy should stay centralized in classify_source_worker_control_reset(...) instead of drifting back into per-callsite strings",
    );
    assert_eq!(
        source_impl
            .matches(
                "replay-required scheduled-groups refresh exhausted before a live worker recovered"
            )
            .count(),
        1,
        "workers/source hard cut regressed; replay-required scheduled-groups refresh exhaustion should stay centralized in shape_source_scheduled_groups_refresh_exhaustion(...) instead of drifting back into per-callsite branches",
    );
    assert_eq!(
        source_impl
            .matches(
                "post-ack scheduled-groups refresh exhausted before scheduled groups converged ("
            )
            .count(),
        1,
        "workers/source hard cut regressed; post-ack scheduled-groups refresh exhaustion should stay centralized in shape_source_scheduled_groups_refresh_exhaustion(...) instead of drifting back into per-callsite branches",
    );

    for legacy_symbol in [
        "struct SourceOperationRetryPlan {",
        "fn source_operation_retry_plan(",
        "async fn apply_source_operation_retry_plan(",
        "enum SourceOperationKind {",
        "enum SourceOperationPhase {",
        "struct SourceOperationMachine {",
        "fn entry_phase(self) -> SourceOperationPhase",
        "fn phase_after_error(",
        "fn phase_after_wait(self) -> SourceOperationPhase",
        "fn apply_source_operation_wait(",
        "fn can_retry_on_control_frame(",
        "fn is_source_worker_not_initialized(",
        "fn is_source_worker_unexpected_correlation_reply(",
        "fn is_source_worker_grant_attachments_drained_or_fenced(",
        "fn is_source_worker_invalid_grant_attachment_token(",
        "fn is_source_worker_missing_channel_buffer_route_state(",
        "fn is_retryable_worker_bridge_peer_error(",
        "fn is_bridge_reset_like_control_error(",
        "fn is_timeout_like_worker_control_reset(",
        "pub async fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {\n        let target_node = self.node_id.clone();\n        let deadline = clip_retry_deadline(\n            std::time::Instant::now() + SOURCE_WORKER_FORCE_FIND_TIMEOUT,\n            SOURCE_WORKER_FORCE_FIND_TIMEOUT,\n        );\n        loop {",
        "async fn scheduled_group_ids_with_timeout(\n        &self,\n        request: SourceWorkerRequest,\n    ) -> std::result::Result<SourceWorkerResponse, SourceFailure> {\n        let deadline = clip_retry_deadline(\n            std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,\n            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,\n        );\n        loop {",
        "fn fail_exhausted(&self) -> SourceScheduledGroupsRefreshEffect {",
        "EnsureStartedCompleted {\n        result: std::result::Result<(), CnxError>,",
        "ClientAcquired {\n        result: std::result::Result<TypedWorkerClient<SourceWorkerRpc>, CnxError>,",
        "GrantsRefreshed {\n        result: std::result::Result<SourceScheduledGroupsRefreshGrantedClient, CnxError>,",
        "FetchScheduledGroupsCompleted {\n        result: std::result::Result<SourceScheduledGroupsRefreshFetchedGroups, CnxError>,",
        "pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {\n        let _inflight = self.begin_control_op();\n        let _serial = self.control_ops_serial.lock().await;\n        eprintln!(\n            \"fs_meta_source_worker_client: update_logical_roots begin node={} roots={}\",\n            self.node_id.0,\n            roots.len()\n        );\n        let deadline = clip_retry_deadline(\n            std::time::Instant::now() + SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT,\n            SOURCE_WORKER_UPDATE_ROOTS_TOTAL_TIMEOUT,\n        );\n        loop {",
        "async fn observability_snapshot_with_timeout(\n        &self,\n        timeout: Duration,\n    ) -> Result<SourceObservabilitySnapshot> {\n        let deadline = clip_retry_deadline(std::time::Instant::now() + timeout, timeout);\n        let response = loop {",
        "pub async fn start(&self) -> Result<()> {\n        let _start_serial = self.start_serial.lock().await;\n        let deadline = clip_retry_deadline(\n            std::time::Instant::now() + SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,\n            SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,\n        );\n        loop {",
        "async fn replay_retained_control_state_if_needed_for_refresh_until(\n        &self,\n        deadline: std::time::Instant,\n    ) -> Result<()> {\n        let envelopes = {\n            let mut control_state = self.control_state.lock().await;\n            if !matches!(\n                control_state.take_replay_state(),\n                SourceControlReplayState::Required\n            ) {\n                return Ok(());\n            }\n            control_state.replay_envelopes()\n        };\n        if envelopes.is_empty() {\n            return Ok(());\n        }\n\n        let deadline = clip_retry_deadline(deadline, SOURCE_WORKER_SCHEDULE_REFRESH_TOTAL_TIMEOUT);\n        loop {",
        "async fn execute_scheduled_groups_refresh_client(",
        "enum ScheduledGroupsRefreshStep<T> {",
        "async fn execute_scheduled_groups_refresh_ensure_started_or_retry(",
        "async fn execute_scheduled_groups_refresh_client_or_retry(",
        "async fn execute_scheduled_groups_refresh_grants_or_retry(",
        "async fn execute_scheduled_groups_refresh_fetch_or_retry(",
        "async fn execute_scheduled_groups_refresh_retry_or_continue<T>(",
        "async fn execute_scheduled_groups_refresh_retry_or_fail(",
        "SourceScheduledGroupIdsEffect::ReconnectThenWait {",
        "SourceScheduledGroupIdsEvent::ReconnectThenWaitCompleted",
        "SourceStartEffect::ReconnectThenWait {",
        "SourceStartEvent::ReconnectThenWaitCompleted",
        "SourceReplayRetainedControlStateEffect::ReconnectThenWait {",
        "SourceReplayRetainedControlStateEvent::ReconnectThenWaitCompleted",
        "SourceScheduledGroupsRefreshEffect::ReconnectThenWait {",
        "SourceScheduledGroupsRefreshEvent::ReconnectThenWaitCompleted",
        "async fn execute_scheduled_group_ids_reconnect_then_wait(",
        "async fn execute_start_reconnect_then_wait(",
        "async fn execute_replay_retained_control_state_reconnect_then_wait(",
        "async fn execute_scheduled_groups_refresh_reconnect_then_wait(",
        "async fn execute_replay_retained_control_state_attempt(",
        "fn replay_required(&self) -> bool {",
        "fn take_replay_requirement(&mut self) -> bool {",
        "fn can_use_cached_host_object_grants_snapshot(",
        "fn can_use_cached_logical_roots_snapshot(",
        "fn classify_control_frame_schedule_refresh_error(",
        "fn classify_replay_required_control_frame_schedule_refresh_error(",
        "fn post_ack_schedule_refresh_exhaustion_error(",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source hard cut regressed; generic source operation phase family returned to production: {legacy_symbol}",
        );
    }

    for legacy_symbol in [
        "struct ScheduledGroupsRefreshFailure {",
        "ScheduledGroupsRefreshFailure::new(",
        "exhaustion_reason: Option<SourceScheduledGroupsRefreshExhaustionReason>",
        "SourceReconnectRetryDisposition::BudgetExhausted",
        "SourceWaitRetryDisposition::BudgetExhausted",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source hard cut regressed; legacy source refresh/budget carrier returned to production: {legacy_symbol}",
        );
    }
}

#[test]
fn source_control_frame_machine_owns_rpc_phase_dispatch_loop() {
    let source_impl = include_str!("../source.rs");

    for required_symbol in [
        "struct SourceControlFrameFacts {",
        "enum SourceControlFrameLaneInspect {",
        "trait SourceControlFrameLane: Send {",
        "type SourceControlFrameRun = Box<dyn SourceControlFrameLane>;",
        "struct RetainedTickFastPathLane {",
        "struct RetainedTickReplayLane {",
        "struct AttemptLane {",
        "struct ReconnectLane;",
        "struct WaitLane {",
        "struct ApplyPostAckCacheAndRetainSignalsLane {",
        "struct ArmReplayLane;",
        "struct RefreshScheduledGroupsLane {",
        "enum SourceControlFrameTerminal {",
        "enum SourceControlFrameAttemptTimeoutPolicy {",
        "enum SourceControlFrameBridgeResetPolicy {",
        "enum SourceControlFrameTimeoutResetPolicy {",
        "enum SourceControlFrameTimeoutResetObservation {",
        "enum SourceControlFramePostAckRefreshRequirement {",
        "struct SourceControlFrameSignalPolicies {",
        "fn from_signals(signals: &[SourceControlSignal]) -> Self {",
        "fn map_source_timeout_result_with_failure<T>(",
        "enum SourceRetryBudgetDisposition {",
        "enum SourceRetryBudgetExhaustionKind {",
        "fn classify_source_retry_budget(",
        "fn shape_source_retry_budget_exhaustion(",
        "enum SourceScheduledGroupsRefreshExhaustionReason {",
        "enum SourceFailureReason {",
        "struct SourceFailure {",
        "fn classify_source_scheduled_groups_refresh_exhaustion(",
        "fn shape_source_scheduled_groups_refresh_exhaustion(",
        "enum SourceWorkerControlResetKind {",
        "fn classify_source_worker_control_reset(",
        "enum SourceControlFrameRequestAttemptKind {",
        "enum SourceControlFramePostAckCachePriming {",
        "enum SourceControlFrameDecodedSignals {",
        "enum SourceControlFrameTickOnlyDisposition {",
        "enum SourceControlFrameReconnectResume {",
        "enum SourceControlFramePendingRefreshNextStep {",
        "enum SourceScheduledGroupsExpectation {",
        "struct SourceControlFramePendingRefresh {",
        "enum SourceControlFrameEvent {",
        "enum SourceControlFrameStep {",
        "enum SourceControlFrameInitialStep {",
        "Run(SourceControlFrameRun)",
        "Terminal(SourceControlFrameTerminal)",
        "fn log_control_frame_terminal_outcome(",
        "fn start(\n        facts: SourceControlFrameFacts,\n    ) -> std::result::Result<(Self, SourceControlFrameStep), SourceFailure> {",
        "fn rpc_attempt_plan(",
        "async fn execute_on_control_frame_request_attempt(",
        "fn apply_control_frame_retained_tick_fast_path(",
        "async fn replay_control_frame_retained_tick(",
        "async fn apply_post_ack_cache_and_retain_signals(",
        "async fn perform_control_frame_attempt(",
        "async fn reconnect_for_control_frame(",
        "async fn wait_control_frame_retry_after(",
        "async fn arm_control_frame_replay(",
        "async fn refresh_scheduled_groups_for_control_frame(",
        "async fn execute_control_frame(",
        "let event = lane.execute(self.handle).await?;",
        "fn missing_control_frame_pending_reconnect_resume(",
        "fn missing_control_frame_pending_refresh_state(",
        "prime_cached_control_summary_from_control_signals(",
        "cache.observability_control_summary_override_by_node = None;",
        "fn action_after_error(",
        "fn advance(",
        "fn schedule_post_ack_cache_and_signal_retention(\n        &mut self,\n        signals: Vec<SourceControlSignal>,",
        "fn queue_pending_refresh(\n        &mut self,\n        scheduled_groups_expectation: SourceScheduledGroupsExpectation,\n        next_step: SourceControlFramePendingRefreshNextStep,",
        "initial_step: Option<SourceControlFrameInitialStep>",
        "pending_reconnect_resume: Option<SourceControlFrameReconnectResume>",
        "pending_refresh: Option<SourceControlFramePendingRefresh>",
        "post_ack_refresh_requirement: SourceControlFramePostAckRefreshRequirement",
        "bridge_reset_policy: SourceControlFrameBridgeResetPolicy",
        "timeout_reset_policy: SourceControlFrameTimeoutResetPolicy",
        "timeout_reset_observation: SourceControlFrameTimeoutResetObservation",
        "attempt_timeout_policy: SourceControlFrameAttemptTimeoutPolicy",
        "cause: CnxError",
        "reason: SourceFailureReason",
        "fn bridge_reset_policy_after_timeout_reset(",
        "scheduled_groups_expectation: SourceScheduledGroupsExpectation",
        "refresh_disposition: SourceScheduledGroupsRefreshDisposition",
        "next_step: SourceControlFramePendingRefreshNextStep",
        "SourceScheduledGroupsExpectation::DoNotExpectLocalRunnableGroups",
        "SourceScheduledGroupsExpectation::ExpectLocalRunnableGroups",
        "async fn refresh_cached_scheduled_groups_from_live_worker_until(\n        &self,\n        deadline: std::time::Instant,\n        scheduled_groups_expectation: SourceScheduledGroupsExpectation,",
        "SourceScheduledGroupsRefreshDisposition::OrdinaryScheduleRefresh",
        "SourceScheduledGroupsRefreshDisposition::ReplayRequiredRecovery",
        "SourceControlFrameDecodedSignals::DecodeFailed",
        "cache_priming: SourceControlFramePostAckCachePriming",
        "SourceControlFramePostAckCachePriming::Standard",
        "SourceControlFramePostAckCachePriming::ReplayRecovery",
        "fn apply_to_cache(",
        "SourceControlFrameStep::run(",
        "SourceControlFrameStep::complete()",
        "SourceControlFrameStep::fail(",
        "SourceControlFrameInitialStep::run(",
        "SourceControlFrameEvent::RetainedTickFastPathApplied",
        "SourceControlFrameEvent::RetainedTickReplayCompleted",
        "SourceControlFrameEvent::RpcCompleted {",
        "SourceControlFrameEvent::ReconnectCompleted",
        "SourceControlFrameEvent::WaitCompleted",
        "SourceControlFrameEvent::PostAckCacheAndSignalsRetained",
        "SourceControlFrameEvent::ArmReplayCompleted",
        "SourceControlFrameEvent::RefreshScheduledGroupsCompleted {",
    ] {
        assert!(
            source_impl.contains(required_symbol),
            "workers/source control-frame hard cut regressed; RPC loop should be owned by dedicated machine-driven helpers instead of inlined in the outer shell: {required_symbol}",
        );
    }

    for legacy_symbol in [
        "enum SourceControlFrameErrorPhase {",
        "fn operation_machine(self, existing_client_present: bool) -> SourceOperationMachine {",
        "enum SourceControlFrameRpcPhase {",
        "enum SourceControlFrameLoopProgress {",
        "struct SourceControlFrameRpcAttemptOutcome {",
        "enum SourceControlFrameEffect {",
        "impl SourceControlFrameEffect {",
        "async fn dispatch(self, handle: &SourceWorkerClientHandle)",
        "fn execute_control_frame_attempt_io(",
        "fn execute_control_frame_reconnect_io(",
        "fn execute_control_frame_wait_io(",
        "fn call_control_frame_rpc_once(",
        "fn execute_control_frame_rpc_attempt(",
        "fn execute_control_frame_loop_action(",
        "fn execute_control_frame_rpc_phase(",
        "fn run_control_frame_rpc_loop(",
        "fn execute_control_frame_loop(",
        "SourceControlFrameExecutor::new(",
        "struct SourceControlFrameWaitPlan {",
        "fn retry_rpc_attempt_plan(",
        "enum SourceControlFrameEvent<'a> {",
        "rpc_result: std::result::Result<SourceWorkerResponse, CnxError>,\n        after: SourceProgressState,\n        decoded_signals: &'a SourceControlFrameDecodedSignals,",
        "clear_observability_override:",
        "refresh_required_by_signals: Option<bool>",
        "refresh_required_by_signals: bool",
        "reconnect_followup: Option<SourceControlFrameReconnectFollowup>",
        "enum SourceControlFrameReconnectFollowup {",
        "fn reconnect_with_followup(",
        "prefer_short_existing_client_attempt: bool",
        "fail_fast_on_control_frame_bridge_reset: bool",
        "generation_one_activate_only: bool",
        "fail_fast_short_caller_budget_bridge_reset: bool",
        "generation_one_timeout_like_reset_seen: bool",
        "fn source_control_signals_prefer_short_existing_client_attempt(",
        "fn source_control_signals_require_post_ack_schedule_refresh(",
        "fn source_control_signals_are_generation_one_activate_only(",
        "enum SourceControlFrameRefreshFailureClassification {",
        "enum SourceScheduledGroupsRefreshReplayRequirement {",
        "enum SourceControlFramePostAckResume {",
        "enum SourceControlFramePostAckRefreshPlan {",
        "SourceControlFrameStep::Effect(",
        "SourceControlFrameStep::Complete",
        "SourceControlFrameStep::Fail(",
        "SourceControlFrameInitialStep::Effect(",
        "let event = effect.dispatch(self.handle).await?;",
        "SourceControlFrameEffect::ReconnectThenAttempt",
        "SourceControlFrameEffect::ReconnectThenWait {",
        "SourceControlFrameEffect::ReconnectThenReplayRetainedTick",
        "SourceControlFrameEffect::ReconnectThenFail(",
        "enum SourceControlFrameInitialEffect {",
        "SourceControlFrameInitialEffect::ReconnectThenReplayRetainedTick",
        "SourceControlFrameInitialEffect::ReconnectBeforeRetainedTickReplay",
        "initial_effect: Option<SourceControlFrameEffect>",
        "initial_reconnect_resume: Option<SourceControlFrameReconnectResume>",
        "SourceControlFrameEvent::ReconnectThenAttemptCompleted",
        "SourceControlFrameEvent::ReconnectThenWaitCompleted {",
        "SourceControlFrameEvent::ReconnectThenReplayRetainedTickCompleted",
        "SourceControlFrameEvent::ReconnectThenFailCompleted(",
        "SourceControlFrameEffect::ApplyPostAckSignalsThenComplete",
        "SourceControlFrameEffect::ApplyPostAckSignalsThenArmReplayAndRefreshScheduledGroups {",
        "SourceControlFrameEvent::ApplyPostAckSignalsThenCompleteCompleted",
        "SourceControlFrameEvent::ApplyPostAckSignalsThenArmReplayAndRefreshScheduledGroupsCompleted {",
        "SourceControlFrameEffect::ArmReplayThenRefreshScheduledGroups {",
        "SourceControlFrameEvent::ArmReplayThenRefreshScheduledGroupsCompleted {",
        "SourceControlFrameEffect::Wait {\n        after: SourceProgressState,\n        wait_for: SourceWaitFor,",
        "fn execute_control_frame_reconnect_step(",
        "fn execute_control_frame_wait_step(",
        "run_control_frame_test_hooks: bool",
        "prime_replay_recovery_cache: bool",
        "refresh_followup_pending: bool",
        "async fn execute_control_frame_reconnect_then_attempt(",
        "async fn execute_control_frame_reconnect_then_wait(",
        "async fn execute_control_frame_reconnect_then_replay_retained_tick(",
        "async fn execute_control_frame_reconnect_then_fail(",
        "async fn execute_control_frame_reconnect(\n        &self,\n        action: SourceControlFrameEffect,",
        "async fn execute_control_frame_refresh_effect(\n        &self,\n        action: SourceControlFrameEffect,",
        "async fn execute_control_frame_post_ack_effect(",
        "async fn execute_control_frame_post_ack_signals(&self, signals: &[SourceControlSignal])",
        "fn execute_control_frame_apply_post_ack_cache(&self, signals: &[SourceControlSignal])",
        "async fn execute_control_frame_reconnect(&self) -> Result<()>",
        "async fn execute_control_frame_arm_replay(&self)",
        "async fn execute_control_frame_refresh_effect(",
        "async fn execute_control_frame_wait(",
        "async fn execute_control_frame_retained_tick_replay(",
        "fn execute_control_frame_retained_tick_fast_path(&self, signals: &[SourceControlSignal])",
        "fn execute_control_frame_apply_post_ack_cache(",
        "fn refresh_scheduled_groups_effect_from_pending_refresh(",
        "fn schedule_refresh_scheduled_groups(",
        "fn refresh_scheduled_groups_effect(",
        "fn decoded_signal_payload(",
        "SourceControlFrameDecodedSignals::DecodeFailed => Vec::new(),",
        "post_ack_completion_pending: bool",
        "fn prime_control_frame_summary_cache(",
        "fn clear_observability_control_summary_override(",
        "fn apply_control_frame_fast_path_signals(",
        "fn apply_post_ack_control_frame_cache_side_effects(",
        "struct SourceControlFrameAttemptCompletion {",
        "async fn execute_control_frame_attempt(",
        "fn classify_control_frame_schedule_refresh_error(",
        "fn classify_replay_required_control_frame_schedule_refresh_error(",
        "fn post_ack_schedule_refresh_exhaustion_error(",
        "SourceControlFrameEvent::PostAckSignalsApplied",
        "SourceControlFrameEffect::ApplyPostAckSignals {",
        "SourceControlFrameEffect::RetainControlSignals {",
        "SourceControlFrameEvent::ControlSignalsRetained",
        "SourceControlFrameEffect::ApplyPostAckCache {",
        "SourceControlFrameEvent::PostAckCacheApplied",
        "SourceControlFrameEvent::PostAckCacheAndSignalsRetained {\n        followup:",
        "enum SourceControlFramePostAckFollowup {",
        "enum SourceControlFrameScheduledGroupsExpectation {",
        "refresh_followup_requires_replay_recovery: bool",
        "fn post_ack_refresh_plan(",
        "fn schedule_post_ack_cache_and_signal_retention(\n        &mut self,\n        signals: Vec<SourceControlSignal>,\n        scheduled_groups_expectation: SourceControlFrameScheduledGroupsExpectation,\n        refresh_followup_requires_replay_recovery: bool,",
        "fn schedule_post_ack_cache(\n        &mut self,\n        signals: Vec<SourceControlSignal>,",
        "pending_refresh_arm_replay: Option<bool>",
        "SourceControlFramePostAckResume::RefreshScheduledGroups {\n            expect_local_runnable_groups: bool,\n            arm_replay: bool,",
        "SourceControlFrameEffect::RefreshScheduledGroups {\n        expect_local_runnable_groups: bool,\n        deadline: std::time::Instant,\n        arm_replay: bool,",
        "fn queue_pending_refresh(\n        &mut self,\n        expect_local_runnable_groups: bool,\n        replay_required_recovery: bool,",
        "struct SourceControlFramePendingRefresh {\n    expect_local_runnable_groups: bool,",
        "replay_required_recovery: bool,\n}",
        "recovered_live_worker_during_refresh: bool,\n}",
        "struct SourceScheduledGroupsRefreshMachine {\n    deadline: std::time::Instant,\n    scheduled_groups_expectation: SourceScheduledGroupsExpectation,\n    replay_required_recovery: bool,",
        "fn new(\n        deadline: std::time::Instant,\n        scheduled_groups_expectation: SourceScheduledGroupsExpectation,\n        replay_required_recovery: bool,",
        "recovered_live_worker_during_refresh: bool",
        "replay_required_recovery_classification: bool",
        "if pending_refresh.replay_required_recovery_classification {",
        "SourceControlFrameEvent::ArmReplayCompleted => Ok(SourceControlFrameStep::Effect(\n                self.refresh_scheduled_groups_effect_from_pending_refresh()?,",
        "async fn execute_control_frame_attempt(\n        &self,\n        envelopes: &[ControlEnvelope],\n        timeout: Duration,\n    ) -> SourceControlFrameEvent {",
        "async fn execute_control_frame_wait(\n        &self,\n        after: SourceProgressState,\n        operation_deadline: std::time::Instant,\n    ) -> SourceControlFrameEvent {",
        "retained_tick_fast_path=true",
        "retained_tick_replay=true",
        "action @ SourceControlFrameEffect::ReconnectThenAttempt",
        "unexpected control-frame reconnect effect dispatched via executor reconnect lane",
        "fn into_reconnect_completion_event(self) -> Option<SourceControlFrameEvent> {",
        "fn into_refresh_completion_event(",
        "async fn execute_control_frame_attempt_completion(",
        "async fn execute_control_frame_apply_post_ack_then_complete(",
        "async fn execute_control_frame_apply_post_ack_then_arm_replay_and_refresh_scheduled_groups(",
        "async fn execute_control_frame_refresh_scheduled_groups_completion(",
        "async fn execute_control_frame_arm_replay_then_refresh_scheduled_groups_completion(",
        "async fn execute_control_frame_scheduled_groups_refresh(",
        "async fn execute_control_frame_arm_replay_then_scheduled_groups_refresh(",
        "SourceControlFrameEffect::Attempt(",
        "SourceControlFrameEffect::Complete",
        "SourceControlFrameEffect::Fail(",
        "fn action_after_retry_resume(",
        "fn action_after_rpc_result(",
        "SourceControlFrameEffect::Wait(",
        "async fn execute_control_frame_immediate_completion(",
        "async fn execute_control_frame_event_completion(",
        "async fn execute_control_frame_retry_resume_completion(",
        "async fn execute_control_frame_reconnect_completion(",
        "async fn execute_control_frame_post_ack_completion(",
        "async fn execute_control_frame_refresh_completion(",
        "enum SourceControlFrameExecutorOutcome {",
        "SourceControlFrameExecutorOutcome::Complete",
        "SourceControlFrameExecutorOutcome::Fail(",
        "SourceControlFrameEffect::Complete => {\n                eprintln!(",
        "SourceControlFrameEffect::Fail(err) => {\n                eprintln!(",
        "\"unexpected terminal control-frame action in executor dispatch\"",
        "terminal control-frame effect dispatched via executor event lane",
        "fn advance_after_reconnect(",
        "fn advance_after_wait_completion(",
        "fn advance_after_post_ack_side_effects(",
        "fn action_after_scheduled_groups_refresh(",
        "fn next_control_frame_loop_action(",
        "self.next_control_frame_loop_action(",
        "post_ack_followup: Option<SourceControlFrameEffect>",
        "fn queue_post_ack_followup(",
        "SourceControlFrameEffect::ApplyPostAckSignals =>",
        "action = SourceControlFrameEffect::Complete;",
        "action = SourceControlFrameEffect::ArmReplayThenRefreshScheduledGroups {",
        "replay_deadline: std::time::Instant,",
        "deadline: std::time::Instant,\n    },\n    ExecuteAckPlan(",
        "fn retry_timeout_cap(",
        "fn retry_followup_action(",
        "fn attempt_action_from_deadline(",
        "fn rpc_attempt_timeout_cap(",
        "enum SourceControlFrameScheduleRefreshRequirement {",
        "fn plan_after_ack(",
        "fn as_slice(&self) -> Option<&[SourceControlSignal]> {",
        "enum SourceControlFrameEntryPlan {",
        "fn entry_plan(",
        "async fn execute_control_frame_entry_plan(",
        "fn entry_action(",
        "enum SourceControlTickDisposition {",
        "async fn generation_one_activate_wave_matches_retained_state(",
        "async fn classify_tick_only_wave_against_retained_state(",
        "fn initial_effect_or_rpc_attempt(",
        "control_machine.initial_effect.take()",
        "SourceControlFrameMachine::new(",
        "enum SourceControlFrameAckPlan {",
        "SourceControlFrameEffect::ExecuteAckPlan(",
        "async fn execute_control_frame_ack_action(",
        "async fn refresh_control_frame_scheduled_groups(",
        "apply_decoded_signals:",
        "arm_replay_before_refresh:",
        "let refresh = match plan {",
        "let Some((expect_local_runnable_groups, operation_deadline, arm_replay_before_refresh)) =",
        "self.handle.execute_control_frame_ack_plan(",
        "self.handle.execute_control_frame_loop(",
        "let existing_client_present = self",
        "struct SourceControlFrameObservation {",
        "SourceControlFrameObservation {",
        "decoded_signals: Option<&[SourceControlSignal]>",
        "enum SourceControlFrameRawIoOutcome {",
        "fn observe_control_frame_io_outcome(",
        "struct SourceControlFrameIoRunner<'a> {",
        "fn io_runner(&self) -> SourceControlFrameIoRunner<'a> {",
        "enum SourceControlFrameIoOutcome {",
        "fn observe_control_frame_attempt_progress(",
        "fn observe_control_frame_attempt_ack_summary(",
        "fn call_control_frame_rpc_with_client(",
        "async fn existing_client_present(",
        "async fn reconnect_once(",
        "async fn wait_for_progress_once(",
        "pub(crate) async fn reconnect_after_fail_closed_control_error(&self) -> Result<()> {\n        match self {\n            Self::Local(_) => Ok(()),\n            Self::Worker(client) => client.reconnect_shared_worker_client().await,\n        }\n    }",
        "pub(crate) async fn reconnect_after_fail_closed_control_error(&self) -> Result<()> {\n        let stale_client = self.replace_shared_worker_client().await?;",
        "pub(crate) async fn reconnect_after_retryable_control_reset(&self) -> Result<()>",
        "enum SourceControlFrameIoPlan {",
        "fn execute_control_frame_io_step(",
        "SourceControlFrameEvent::ScheduledGroupsRefreshCompleted {",
        "replay_required_before_refresh: bool,",
        "enum SourceControlFrameReconnectCompletion {",
        "SourceControlFrameEvent::ReconnectCompleted(",
        "fn next_loop_action(",
        "fn action_after_wait(",
        "let worker = self.handle.worker_client().await;\n        #[cfg(test)]",
        "control_machine.entry_plan(decoded_signals.is_some())",
        "control_machine.next_loop_action(existing_client_present)?",
        "control_machine.rpc_attempt_plan(self.handle.existing_client().await?.is_some())?",
        "control_machine.rpc_attempt_plan(true)?",
        "if control_machine.initial_effect.is_none() {\n            control_machine.existing_client_present = self.handle.existing_client().await?.is_some();\n        }",
        "self.handle.reconnect_shared_worker_client().await?;\n                    control_machine.existing_client_present = true;\n                    action = control_machine.rpc_attempt_plan()?;",
        "wait_for_source_progress_after(\n                            after,\n                            SourceWaitFor::ControlFlowRetry,\n                            control_machine.operation_deadline,\n                        )\n                        .await;\n                    control_machine.existing_client_present = true;\n                    action = control_machine.rpc_attempt_plan()?;",
        "Attempt {\n        timeout: Duration,\n        reconnect_before: bool,",
        "control_machine.next_loop_action(true)?",
        "SourceControlFrameEffect::ExecuteIo(",
        "fn action_after_io_outcome(",
        "SourceControlFrameEffect::ReconnectThenRetainedTickReplay",
        "async fn execute_control_frame_event_effect(",
        "async fn execute_control_frame_immediate_effect(",
        "self.handle.reconnect_shared_worker_client().await?;\n                    action = control_machine.action_after_retry_resume()?;",
        "wait_for_source_progress_after(\n                            after,\n                            SourceWaitFor::ControlFlowRetry,\n                            control_machine.operation_deadline,\n                        )\n                        .await;\n                    action = control_machine.action_after_retry_resume()?;",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source control-frame hard cut regressed; legacy helper-owned RPC phase dispatch returned to production: {legacy_symbol}",
        );
    }
}

#[test]
fn source_retained_replay_control_rpc_uses_single_started_attempt() {
    let source_impl = include_str!("../source.rs");
    let start = source_impl
        .find("async fn execute_on_control_frame_request_attempt(")
        .expect("source control-frame request executor should exist");
    let end = start
        + source_impl[start..]
            .find("async fn execute_update_logical_roots_attempt(")
            .expect("source update-roots executor should follow control-frame request executor");
    let executor = &source_impl[start..end];

    assert!(
        executor.contains("SourceControlFrameRequestAttemptKind::ControlFrame => {")
            && executor.contains("self.with_started_retry_with_failure(|client|")
            && executor.contains("SourceControlFrameRequestAttemptKind::RetainedReplay => {")
            && executor.contains("self.with_started_once_with_failure(|client|"),
        "source retained control replay must use one started worker attempt and leave retry/reconnect policy to the source replay state machine",
    );
}

#[test]
fn source_control_frame_machine_owns_schedule_refresh_fail_closed_lane() {
    assert!(
        matches!(
            SourceFailure::refresh_exhausted(
                SourceScheduledGroupsRefreshExhaustionReason::NoLiveWorkerRecovery
            )
            .into_error(),
            CnxError::Internal(message)
                if message.contains(
                    "replay-required scheduled-groups refresh exhausted before a live worker recovered"
                )
        ),
        "source control-frame lane should own the replay-required scheduled-groups refresh fail-closed classification instead of leaving on_control_frame_with_timeouts to hand-roll the same timeout branch",
    );
}

#[test]
fn source_control_frame_executor_shell_dispatches_handle_primitives_only() {
    let source_impl = include_str!("../source.rs");
    let loop_start = source_impl
        .find("async fn execute_control_frame(")
        .expect("control-frame outer loop should exist");
    let loop_end = source_impl[loop_start..]
        .find("}\n}\n\n#[derive(Clone)]")
        .map(|offset| loop_start + offset + 2)
        .expect("control-frame outer loop should end before source facade definition");
    let effect_shell = &source_impl[loop_start..loop_end];
    let lanes_start = source_impl
        .find("struct RetainedTickFastPathLane {")
        .expect("control-frame lane impls should exist");
    let lanes_end = source_impl[lanes_start..]
        .find("enum SourceControlFrameTerminal {")
        .map(|offset| lanes_start + offset)
        .expect("control-frame terminal carrier should follow lane impls");
    let lane_impls = &source_impl[lanes_start..lanes_end];

    assert!(
        effect_shell.contains("let event = lane.execute(self.handle).await?;"),
        "control-frame outer loop should execute lane-owned dispatch directly instead of routing through an extra forwarding shell",
    );

    for forbidden_shell_call in [
        "async fn execute_control_frame_effect(",
        "apply_control_frame_retained_tick_fast_path(&signals)",
        "replay_control_frame_retained_tick(deadline)",
        "perform_control_frame_attempt(&envelopes, timeout)",
        "reconnect_for_control_frame().await?;",
        "wait_control_frame_retry_after(after, deadline)",
        "apply_post_ack_cache_and_retain_signals(&signals, cache_priming)",
        "arm_control_frame_replay().await;",
        "refresh_scheduled_groups_for_control_frame(",
    ] {
        assert!(
            !effect_shell.contains(forbidden_shell_call),
            "control-frame outer loop regressed; handle-level primitive dispatch or forwarding-shell legacy leaked back outside lane-owned execution: {forbidden_shell_call}",
        );
    }

    for required_dispatch in [
        "handle.apply_control_frame_retained_tick_fast_path(&self.signals)",
        "replay_control_frame_retained_tick(self.deadline)",
        "perform_control_frame_attempt(&self.envelopes, self.timeout)",
        "handle.reconnect_for_control_frame().await?",
        "wait_control_frame_retry_after(self.after, self.deadline)",
        "apply_post_ack_cache_and_retain_signals(&self.signals, self.cache_priming)",
        "handle.arm_control_frame_replay().await",
        "refresh_scheduled_groups_for_control_frame(",
    ] {
        assert!(
            lane_impls.contains(required_dispatch),
            "control-frame lane execution should route through handle-level primitives only: {required_dispatch}",
        );
    }

    for forbidden_raw_call in [
        "with_cache_mut(",
        "reconnect_shared_worker_client().await?;",
        "wait_for_source_progress_after(",
        "refresh_cached_scheduled_groups_from_live_worker_until(",
        "control_state.lock().await.arm_replay()",
        "execute_on_control_frame_request_attempt(",
        "retain_control_signals(signals).await",
        "prime_cached_control_summary_from_control_signals(",
        "replay_retained_control_state_if_needed_for_refresh_until(",
    ] {
        assert!(
            !lane_impls.contains(forbidden_raw_call),
            "control-frame lane execution regressed; raw source-worker side effects leaked back outside handle-level primitives: {forbidden_raw_call}",
        );
    }
}

#[test]
fn source_progress_snapshot_published_expected_groups_since_requires_group_epoch_at_request_boundary()
 {
    let expected_groups =
        std::collections::BTreeSet::from(["group-a".to_string(), "group-b".to_string()]);
    let snapshot = crate::source::SourceProgressSnapshot {
        rescan_observed_epoch: 7,
        scheduled_source_groups: std::collections::BTreeSet::new(),
        scheduled_scan_groups: std::collections::BTreeSet::new(),
        published_group_epoch: std::collections::BTreeMap::from([
            ("group-a".to_string(), 7),
            ("group-b".to_string(), 6),
        ]),
    };

    assert!(
        !snapshot.published_expected_groups_since(7, &expected_groups),
        "source progress must not report publication satisfied when any expected group still trails the request epoch",
    );
    assert!(
        snapshot.published_expected_groups_since(6, &expected_groups),
        "source progress should report publication satisfied once every expected group has reached the request epoch",
    );
}

#[test]
fn source_progress_snapshot_cache_coupling_to_observability_is_removed() {
    let source_impl = include_str!("../source.rs");

    for legacy_symbol in [
        "invalidate_cached_progress_snapshot_for_explicit_zero_nodes(",
        "last_progress_snapshot: Option<crate::source::SourceProgressSnapshot>",
        "cache.last_progress_snapshot = Some(snapshot.clone());",
        "cache.last_progress_snapshot = None;",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source progress hard cut regressed; progress truth still rides an observability-driven cached snapshot seam: {legacy_symbol}",
        );
    }
}

#[test]
fn source_legacy_retry_kernel_symbols_are_removed() {
    let source_impl = include_str!("../source.rs");
    for legacy_symbol in [
        "SourceRefreshRuntimeAction",
        "SourceRefreshFollowup",
        "SourceProgressMask",
        "SourceProgressCursor",
        "continue_source_refresh_after(",
        "apply_source_refresh_runtime_action(",
        "apply_source_refresh_followup(",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source hard cut regressed; legacy retry-kernel symbol still present: {legacy_symbol}",
        );
    }
}

#[test]
fn source_progress_snapshot_machine_owned_truth_removes_origin_count_adapter_symbol() {
    let source_impl = include_str!("../source.rs");
    for legacy_symbol in [
        "fn source_progress_snapshot_from_status(",
        "fn source_progress_snapshot_from_observability(",
        ".published_origin_counts_by_node\n        .values()",
        "fn update_cached_published_group_epoch_from_snapshot(",
        "fn clear_cached_published_group_epoch_for_explicit_zero_nodes(",
        "fn maybe_mark_rescan_observed_from_snapshot(",
        "fn progress_snapshot_from_cache(",
        "let snapshot = client.observability_snapshot_nonblocking().await;\n                client.progress_snapshot_from_cache(&snapshot)",
        "if self.control_op_inflight() {\n            return Ok(self.cached_progress_snapshot());\n        }",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol),
            "workers/source progress hard cut regressed; published_group_epoch still derives from the legacy observability-origin-count adapter: {legacy_symbol}",
        );
    }
}

#[test]
fn source_worker_rescan_epoch_truth_uses_worker_owned_epoch_request() {
    let source_impl = include_str!("../source.rs");
    let source_ipc = include_str!("../source_ipc.rs");
    let source_server = include_str!("../source_server.rs");

    for required_symbol in [
        "TriggerRescanWhenReadyEpoch",
        "RescanRequestEpoch(u64)",
        "SourceWorkerRequest::TriggerRescanWhenReadyEpoch",
        "SourceWorkerResponse::RescanRequestEpoch(epoch)",
        "source.trigger_rescan_when_ready_epoch_with_failure().await",
    ] {
        assert!(
            source_impl.contains(required_symbol)
                || source_ipc.contains(required_symbol)
                || source_server.contains(required_symbol),
            "workers/source rescan epoch hard cut regressed; worker-owned rescan epoch symbol missing: {required_symbol}",
        );
    }

    for legacy_symbol in [
        "TriggerRescanWhenReady,",
        "let epoch = self.begin_rescan_request_epoch();\n        self.trigger_rescan_when_ready().await?;",
        "SourceWorkerRequest::TriggerRescanWhenReady,\n                SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,",
        "cache.rescan_observed_epoch = cache.rescan_observed_epoch.max(epoch);",
        "async fn try_progress_snapshot_nonblocking(",
        "async fn request_trigger_rescan_when_ready_epoch(",
        "pub async fn trigger_rescan_when_ready(&self) -> Result<()>",
    ] {
        assert!(
            !source_impl.contains(legacy_symbol)
                && !source_ipc.contains(legacy_symbol)
                && !source_server.contains(legacy_symbol),
            "workers/source rescan epoch hard cut regressed; client still synthesizes or conflates worker-local epoch truth: {legacy_symbol}",
        );
    }
}

#[test]
fn source_progress_snapshot_uses_live_worker_retry_before_cached_fallback() {
    let source_impl = include_str!("../source.rs");

    for required_symbol in [
        "pub(crate) async fn progress_snapshot_with_failure(",
        "async fn progress_snapshot_with_timeout_with_failure(",
        ".with_started_retry_with_failure(|client| async move {",
        "SourceWorkerRequest::ProgressSnapshot",
    ] {
        assert!(
            source_impl.contains(required_symbol),
            "workers/source progress hard cut regressed; progress snapshot should drive through live worker retry before considering cached fallback: {required_symbol}",
        );
    }

    assert!(
        !source_impl.contains(
            "let Some(_client) = self.existing_client().await? else {\n            return Ok(self.cached_progress_snapshot());\n        };"
        ),
        "workers/source progress hard cut regressed; progress snapshot still short-circuits to cached truth when no live worker client exists",
    );
}

#[test]
fn source_worker_client_retry_helper_uses_typed_runtime_adapter_mapping() {
    let source_impl = include_str!("../source.rs");

    assert!(
        source_impl.contains(".with_started_retry_mapped(op, SourceFailure::into_error)"),
        "workers/source hard cut regressed; source worker typed retry helper should use the runtime typed adapter mapping surface",
    );

    assert!(
        !source_impl.contains(".with_started_retry(|client| {"),
        "workers/source hard cut regressed; source worker typed retry helper bounced back through the raw runtime retry shell",
    );
}

#[test]
fn source_worker_timeout_mapping_uses_typed_timeout_carrier() {
    let source_impl = include_str!("../source.rs");

    assert!(
        source_impl.contains("fn timeout_reset() -> Self {"),
        "workers/source hard cut regressed; source worker timeout reset should keep an explicit typed carrier constructor",
    );

    assert!(
        source_impl.contains("CnxError::Timeout => Self::timeout_reset(),"),
        "workers/source hard cut regressed; source worker timeout cause classification should stay on the explicit typed timeout carrier",
    );

    assert!(
        source_impl.contains("Err(_) => Err(SourceFailure::timeout_reset()),"),
        "workers/source hard cut regressed; source worker timeout mapping should stay on the typed timeout carrier",
    );

    assert!(
        !source_impl.contains("Err(_) => Err(SourceFailure::from(CnxError::Timeout)),"),
        "workers/source hard cut regressed; source worker timeout mapping bounced back through raw timeout materialization",
    );

    assert!(
        !source_impl.contains("map_source_timeout_result(result).map_err(SourceFailure::from)"),
        "workers/source hard cut regressed; source worker timeout helper bounced back through a raw timeout result before rebuilding typed failure",
    );
}

#[test]
fn source_scheduled_groups_refresh_cached_grants_fallback_uses_typed_helper() {
    let source_impl = include_str!("../source.rs");

    assert!(
        source_impl.contains(
            ".cached_host_object_grants_snapshot_with_failure()\n                .map(|grants| self.scheduled_groups_refresh_stable_host_ref(&grants))?"
        ),
        "workers/source hard cut regressed; scheduled-groups refresh cached-grants fallback should stay on the typed cache helper",
    );

    assert!(
        !source_impl.contains(
            ".cached_host_object_grants_snapshot()\n                .map(|grants| self.scheduled_groups_refresh_stable_host_ref(&grants))\n                .map_err(SourceFailure::from)?"
        ),
        "workers/source hard cut regressed; scheduled-groups refresh cached-grants fallback bounced back through the raw cache wrapper",
    );
}

#[test]
fn source_post_recovery_rescan_gate_uses_live_progress_snapshot() {
    let source_impl = include_str!("../source.rs");
    let runtime_impl = include_str!("../../runtime_app.rs");

    assert!(
        !source_impl.contains(
            "pub(crate) async fn current_rescan_observed_epoch_nonblocking(&self) -> u64"
        ),
        "workers/source rescan hard cut regressed; production source facade still exposes cached nonblocking rescan epoch truth",
    );
    assert!(
        runtime_impl
            .contains("if let Ok(snapshot) = source.progress_snapshot_with_failure().await {")
            && runtime_impl.contains("if snapshot.rescan_observed_epoch >= request_epoch {"),
        "runtime post-recovery rescan gate should clear request state only from live source progress truth",
    );
    assert!(
        !runtime_impl
            .contains("source.current_rescan_observed_epoch_nonblocking().await >= request_epoch"),
        "runtime post-recovery rescan gate regressed to cached source rescan observation truth",
    );
}

#[test]
fn source_facade_degraded_progress_surface_is_removed() {
    let source_impl = include_str!("../source.rs");

    assert!(
        source_impl.contains("pub(crate) async fn progress_snapshot_with_failure("),
        "workers/source progress hard cut regressed; facade should continue exposing the live progress truth API",
    );
    assert!(
        !source_impl.contains(
            "pub(crate) async fn progress_snapshot_best_effort(\n        &self,\n    ) -> crate::source::SourceProgressSnapshot {"
        ),
        "workers/source progress hard cut regressed; facade still exposes a degraded cached progress surface",
    );
    assert!(
        !source_impl.contains(
            "pub(crate) async fn progress_snapshot_nonblocking(&self) -> crate::source::SourceProgressSnapshot {"
        ),
        "workers/source progress hard cut regressed; facade still exposes degraded cached progress under a truth-like API name",
    );
}

#[test]
fn source_local_control_frame_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains(
            "self.on_control_frame(&envelopes)\n            .await\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local control-frame typed helper bounced back through the raw wrapper",
    );
    for typed_surface in [
        "let signals =\n            source_control_signals_from_envelopes(&envelopes).map_err(SourceFailure::from)?;",
        "self.apply_control_frame_signals(&signals)\n            .await\n            .map_err(SourceFailure::from)",
    ] {
        assert!(
            source_impl.contains(typed_surface),
            "workers/source hard cut regressed; local control-frame typed helper drifted away from the direct typed path: {typed_surface}",
        );
    }
    assert!(
        source_runtime_impl.contains("pub(crate) async fn apply_control_frame_signals("),
        "workers/source hard cut regressed; source runtime should expose a direct control-signal helper instead of forcing typed callers back through the raw envelope wrapper",
    );
}

#[test]
fn source_local_update_roots_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains(
            "self.update_logical_roots(roots)\n            .await\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local update-roots typed helper bounced back through the raw wrapper",
    );
    assert!(
        source_impl.contains(
            "self.apply_logical_roots_update(roots)\n            .await\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local update-roots typed helper should drive the direct typed path",
    );
    assert!(
        source_runtime_impl.contains("pub(crate) async fn apply_logical_roots_update("),
        "workers/source hard cut regressed; source runtime should expose a direct logical-roots update helper instead of forcing typed callers back through the raw wrapper",
    );
}

#[test]
fn source_local_manual_rescan_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains(
            "self.publish_manual_rescan_signal()\n            .await\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local manual-rescan typed helper bounced back through the raw wrapper",
    );
    assert!(
        source_impl.contains(
            "self.emit_manual_rescan_signal()\n            .await\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local manual-rescan typed helper should drive the direct typed path",
    );
    assert!(
        source_runtime_impl.contains("pub(crate) async fn emit_manual_rescan_signal("),
        "workers/source hard cut regressed; source runtime should expose a direct manual-rescan helper instead of forcing typed callers back through the raw wrapper",
    );
}

#[test]
fn source_local_close_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains("self.close().await.map_err(SourceFailure::from)"),
        "workers/source hard cut regressed; local close typed helper bounced back through the raw wrapper",
    );
    assert!(
        source_impl.contains("self.perform_close().await.map_err(SourceFailure::from)"),
        "workers/source hard cut regressed; local close typed helper should drive the direct typed path",
    );
    assert!(
        source_runtime_impl.contains("pub(crate) async fn perform_close(&self) -> Result<()> {"),
        "workers/source hard cut regressed; source runtime should expose a direct close helper instead of forcing typed callers back through the raw wrapper",
    );
}

#[test]
fn source_local_scheduled_group_snapshot_helpers_do_not_bounce_through_raw_wrappers() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for legacy_bounce in [
        "self.scheduled_source_group_ids()\n            .map_err(SourceFailure::from)",
        "self.scheduled_scan_group_ids().map_err(SourceFailure::from)",
    ] {
        assert!(
            !source_impl.contains(legacy_bounce),
            "workers/source hard cut regressed; local scheduled-group typed helper bounced back through a raw wrapper: {legacy_bounce}",
        );
    }
    for typed_surface in [
        "self.snapshot_scheduled_source_group_ids()\n            .map_err(SourceFailure::from)",
        "self.snapshot_scheduled_scan_group_ids()\n            .map_err(SourceFailure::from)",
        "pub(crate) fn snapshot_scheduled_source_group_ids(",
        "pub(crate) fn snapshot_scheduled_scan_group_ids(",
        "pub(crate) fn scheduled_source_group_ids_snapshot(&self) -> Result<Option<BTreeSet<String>>> {",
        "pub(crate) fn scheduled_scan_group_ids_snapshot(&self) -> Result<Option<BTreeSet<String>>> {",
    ] {
        let haystack = if typed_surface.starts_with("pub(crate) fn ") {
            source_runtime_impl
        } else {
            source_impl
        };
        assert!(
            haystack.contains(typed_surface),
            "workers/source hard cut regressed; local scheduled-group typed helper drifted away from the direct typed path: {typed_surface}",
        );
    }
}

#[test]
fn source_server_snapshot_requests_use_local_typed_helpers() {
    let source_impl = include_str!("../source.rs");
    let source_server_impl = include_str!("../source_server.rs");

    for typed_surface in [
        "pub(crate) fn logical_roots_snapshot_with_failure(",
        "pub(crate) fn host_object_grants_snapshot_with_failure(",
        "pub(crate) fn host_object_grants_version_snapshot_with_failure(",
        "pub(crate) fn status_snapshot_with_failure(",
        "pub(crate) fn progress_snapshot_with_failure(",
        "fn source_observability_snapshot_with_failure(",
        "match source.logical_roots_snapshot_with_failure() {",
        "match source.host_object_grants_snapshot_with_failure() {",
        "match source.host_object_grants_version_snapshot_with_failure() {",
        "match source.status_snapshot_with_failure() {",
        "match source.progress_snapshot_with_failure() {",
        "match source_observability_snapshot_with_failure(",
    ] {
        assert!(
            source_impl.contains(typed_surface) || source_server_impl.contains(typed_surface),
            "workers/source hard cut regressed; source server snapshot request path should stay on local typed helpers: {typed_surface}",
        );
    }

    for legacy_surface in [
        "SourceWorkerResponse::LogicalRoots(source.logical_roots_snapshot())",
        "SourceWorkerResponse::HostObjectGrants(source.host_object_grants_snapshot())",
        "source.host_object_grants_version_snapshot(),",
        "SourceWorkerResponse::StatusSnapshot(source.status_snapshot())",
        "SourceWorkerResponse::ProgressSnapshot(source.progress_snapshot())",
        "SourceWorkerResponse::ObservabilitySnapshot(source_observability_snapshot(",
    ] {
        assert!(
            !source_server_impl.contains(legacy_surface),
            "workers/source hard cut regressed; source server snapshot request path bounced back through a raw local wrapper: {legacy_surface}",
        );
    }
}

#[test]
fn source_local_snapshot_typed_helpers_do_not_bounce_through_raw_wrappers() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for legacy_bounce in [
        "Ok(self.logical_roots_snapshot())",
        "Ok(self.host_object_grants_snapshot())",
        "Ok(self.host_object_grants_version_snapshot())",
        "Ok(self.status_snapshot())",
        "Ok(self.progress_snapshot())",
    ] {
        assert!(
            !source_impl.contains(legacy_bounce),
            "workers/source hard cut regressed; local snapshot typed helper bounced back through a raw wrapper: {legacy_bounce}",
        );
    }

    assert!(
        !source_impl
            .contains("pub async fn host_object_grants_version_snapshot(&self) -> Result<u64> {"),
        "workers/source hard cut regressed; zero-call raw grants-version wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub async fn host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {"
        ),
        "workers/source hard cut regressed; zero-call raw grants wrapper should stay removed",
    );
    assert!(
        !source_impl
            .contains("pub async fn status_snapshot(&self) -> Result<SourceStatusSnapshot> {"),
        "workers/source hard cut regressed; zero-call raw status wrapper should stay removed",
    );
    assert!(
        !source_impl
            .contains("pub async fn logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {"),
        "workers/source hard cut regressed; zero-call raw logical-roots wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn last_force_find_runner_by_group_snapshot("),
        "workers/source hard cut regressed; zero-call raw last-runner wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub async fn force_find_inflight_groups_snapshot(&self) -> Result<Vec<String>> {"
        ),
        "workers/source hard cut regressed; zero-call raw inflight-groups wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn resolve_group_id_for_object_ref("),
        "workers/source hard cut regressed; zero-call raw resolve-group wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn lifecycle_state_label(&self) -> Result<String> {"),
        "workers/source hard cut regressed; zero-call raw lifecycle-state wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn source_primary_by_group_snapshot("),
        "workers/source hard cut regressed; zero-call raw source-primary wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("async fn reconnect_shared_worker_client(&self) -> Result<()> {"),
        "workers/source hard cut regressed; zero-call raw reconnect-shared-worker wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn force_find_via_node("),
        "workers/source hard cut regressed; zero-call raw force-find-via-node wrapper should stay removed",
    );
    assert!(
        !source_impl.contains("pub async fn publish_manual_rescan_signal(&self) -> Result<()> {"),
        "workers/source hard cut regressed; zero-call raw manual-rescan wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub async fn update_logical_roots(&self, roots: Vec<RootSpec>) -> Result<()> {"
        ),
        "workers/source hard cut regressed; zero-call raw update-roots wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub(crate) async fn observability_snapshot(&self) -> Result<SourceObservabilitySnapshot> {"
        ),
        "workers/source hard cut regressed; zero-call raw observability wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub async fn force_find(&self, params: InternalQueryRequest) -> Result<Vec<Event>> {"
        ),
        "workers/source hard cut regressed; zero-call raw worker force-find wrapper should stay removed",
    );
    assert!(
        !source_impl.contains(
            "pub async fn force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {"
        ),
        "workers/source hard cut regressed; zero-call raw facade force-find wrapper should stay removed",
    );

    for typed_surface in [
        "Ok(self.snapshot_logical_roots())",
        "Ok(self.snapshot_host_object_grants())",
        "Ok(self.snapshot_host_object_grants_version())",
        "Ok(self.build_status_snapshot())",
        "Ok(self.build_progress_snapshot())",
        "pub(crate) fn snapshot_logical_roots(&self) -> Vec<RootSpec> {",
        "pub(crate) fn snapshot_host_object_grants(&self) -> Vec<GrantedMountRoot> {",
        "pub(crate) fn snapshot_host_object_grants_version(&self) -> u64 {",
        "pub(crate) fn build_status_snapshot(&self) -> SourceStatusSnapshot {",
        "pub(crate) fn build_progress_snapshot(&self) -> SourceProgressSnapshot {",
    ] {
        let haystack = if typed_surface.starts_with("pub(crate) fn ") {
            source_runtime_impl
        } else {
            source_impl
        };
        assert!(
            haystack.contains(typed_surface),
            "workers/source hard cut regressed; local snapshot typed helper drifted away from the direct typed path: {typed_surface}",
        );
    }
}

#[test]
fn source_local_metadata_and_observability_sidecar_use_direct_internal_snapshots() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for legacy_bounce in [
        "Ok(format!(\"{:?}\", self.state()).to_ascii_lowercase())",
        "Ok(self.source_primary_by_group_snapshot())",
        "Ok(self.last_force_find_runner_by_group_snapshot())",
        "Ok(self.force_find_inflight_groups_snapshot())",
        "Ok(self.resolve_group_id_for_object_ref(object_ref))",
        "lifecycle_state: format!(\"{:?}\", source.state()).to_ascii_lowercase(),",
        "let grants = source.host_object_grants_snapshot();",
        "let status = source.status_snapshot();",
        "source.last_force_find_runner_by_group_snapshot();",
        "source.force_find_inflight_groups_snapshot();",
        "source.scheduled_source_group_ids()",
        "source.scheduled_scan_group_ids()",
        "host_object_grants_version: source.host_object_grants_version_snapshot(),",
        "logical_roots: source.logical_roots_snapshot(),",
        "source_primary_by_group: source.source_primary_by_group_snapshot(),",
        "last_control_frame_signals_snapshot: source.last_control_frame_signals_snapshot(),",
        "source.scheduled_source_group_ids_snapshot()",
        "source.scheduled_scan_group_ids_snapshot()",
    ] {
        assert!(
            !source_impl.contains(legacy_bounce),
            "workers/source hard cut regressed; local metadata/observability path bounced back through a raw wrapper: {legacy_bounce}",
        );
    }

    for typed_surface in [
        "Ok(self.snapshot_lifecycle_state_label())",
        "Ok(self.snapshot_source_primary_by_group())",
        "Ok(self.snapshot_last_force_find_runner_by_group())",
        "Ok(self.snapshot_force_find_inflight_groups())",
        "Ok(self.snapshot_group_id_for_object_ref(object_ref))",
        "lifecycle_state: source.snapshot_lifecycle_state_label(),",
        "let grants = source.snapshot_host_object_grants();",
        "let status = source.build_status_snapshot();",
        "let last_force_find_runner_by_group = source.snapshot_last_force_find_runner_by_group();",
        "let force_find_inflight_groups = source.snapshot_force_find_inflight_groups();",
        "source.snapshot_scheduled_source_group_ids()",
        "source.snapshot_scheduled_scan_group_ids()",
        "host_object_grants_version: source.snapshot_host_object_grants_version(),",
        "logical_roots: source.snapshot_logical_roots(),",
        "source_primary_by_group: source.snapshot_source_primary_by_group(),",
        "last_control_frame_signals_snapshot: source.snapshot_last_control_frame_signals(),",
        "pub(crate) fn snapshot_scheduled_source_group_ids(",
        "pub(crate) fn snapshot_scheduled_scan_group_ids(",
        "pub(crate) fn snapshot_lifecycle_state_label(&self) -> String {",
        "pub(crate) fn snapshot_group_id_for_object_ref(&self, object_ref: &str) -> Option<String> {",
        "pub(crate) fn snapshot_source_primary_by_group(&self) -> BTreeMap<String, String> {",
        "pub(crate) fn snapshot_force_find_inflight_groups(&self) -> Vec<String> {",
        "pub(crate) fn snapshot_last_force_find_runner_by_group(&self) -> BTreeMap<String, String> {",
        "pub(crate) fn snapshot_last_control_frame_signals(&self) -> Vec<String> {",
    ] {
        let haystack = if typed_surface.starts_with("pub(crate) fn snapshot_") {
            source_runtime_impl
        } else {
            source_impl
        };
        assert!(
            haystack.contains(typed_surface),
            "workers/source hard cut regressed; local metadata/observability path drifted away from the direct internal snapshot shape: {typed_surface}",
        );
    }
}

#[test]
fn source_server_metadata_requests_use_local_typed_helpers() {
    let source_impl = include_str!("../source.rs");
    let source_server_impl = include_str!("../source_server.rs");

    for typed_surface in [
        "pub(crate) fn lifecycle_state_label_with_failure(",
        "pub(crate) fn source_primary_by_group_snapshot_with_failure(",
        "pub(crate) fn last_force_find_runner_by_group_snapshot_with_failure(",
        "pub(crate) fn force_find_inflight_groups_snapshot_with_failure(",
        "pub(crate) fn resolve_group_id_for_object_ref_with_failure(",
        "pub(crate) async fn trigger_rescan_when_ready_epoch_with_failure(",
        "fn source_force_find_debug_metadata(",
        "match source.lifecycle_state_label_with_failure() {",
        "match source.source_primary_by_group_snapshot_with_failure() {",
        "match source.last_force_find_runner_by_group_snapshot_with_failure() {",
        "match source.force_find_inflight_groups_snapshot_with_failure() {",
        ".resolve_group_id_for_object_ref_with_failure(&object_ref)",
        "match source.trigger_rescan_when_ready_epoch_with_failure().await {",
        "source_force_find_debug_metadata(source)",
    ] {
        assert!(
            source_impl.contains(typed_surface) || source_server_impl.contains(typed_surface),
            "workers/source hard cut regressed; source server metadata request path should stay on local typed helpers: {typed_surface}",
        );
    }

    for legacy_surface in [
        "format!(\"{:?}\", source.state()).to_ascii_lowercase()",
        "SourceWorkerResponse::SourcePrimaryByGroup(\n                    source.source_primary_by_group_snapshot(),",
        "SourceWorkerResponse::LastForceFindRunnerByGroup(\n                    source.last_force_find_runner_by_group_snapshot(),",
        "SourceWorkerResponse::ForceFindInflightGroups(\n                    source.force_find_inflight_groups_snapshot(),",
        "SourceWorkerResponse::ResolveGroupIdForObjectRef(\n                        source.resolve_group_id_for_object_ref(&object_ref),",
        "let epoch = source.trigger_rescan_when_ready_epoch_with_failure().await;",
        "&source.last_force_find_runner_by_group_snapshot()",
        "source.force_find_inflight_groups_snapshot()",
    ] {
        assert!(
            !source_server_impl.contains(legacy_surface),
            "workers/source hard cut regressed; source server metadata request path bounced back through a raw local surface: {legacy_surface}",
        );
    }
}

#[test]
fn source_facade_local_snapshot_metadata_and_progress_helpers_use_local_typed_helpers() {
    let source_impl = include_str!("../source.rs");

    for typed_surface in [
        "Self::Local(source) => source.logical_roots_snapshot_with_failure(),",
        "Self::Local(source) => source.host_object_grants_snapshot_with_failure(),",
        "Self::Local(source) => source.host_object_grants_version_snapshot_with_failure(),",
        "Self::Local(source) => source.status_snapshot_with_failure(),",
        "Self::Local(source) => source.lifecycle_state_label_with_failure(),",
        "Self::Local(source) => source.source_primary_by_group_snapshot_with_failure(),",
        "Self::Local(source) => source.last_force_find_runner_by_group_snapshot_with_failure(),",
        "Self::Local(source) => source.force_find_inflight_groups_snapshot_with_failure(),",
        "Self::Local(source) => source.resolve_group_id_for_object_ref_with_failure(object_ref),",
        "Self::Local(source) => source.progress_snapshot_with_failure(),",
    ] {
        assert!(
            source_impl.contains(typed_surface),
            "workers/source hard cut regressed; source facade local snapshot/metadata/progress path should stay on local typed helpers: {typed_surface}",
        );
    }
}

#[test]
fn source_facade_cached_snapshot_local_branches_use_typed_helpers() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for typed_surface in [
        "pub(crate) fn cached_logical_roots_snapshot_with_failure(",
        "Self::Local(source) => source.cached_logical_roots_snapshot_with_failure(),",
        "pub(crate) fn cached_host_object_grants_snapshot_with_failure(",
        "Self::Local(source) => source.cached_host_object_grants_snapshot_with_failure(),",
        "Ok(self.snapshot_cached_logical_roots())",
        "Ok(self.snapshot_cached_host_object_grants())",
    ] {
        assert!(
            source_impl.contains(typed_surface),
            "workers/source hard cut regressed; source facade cached local branch should stay on a typed helper: {typed_surface}",
        );
    }

    for typed_surface in [
        "pub(crate) fn snapshot_cached_logical_roots(&self) -> Vec<RootSpec> {",
        "pub(crate) fn snapshot_cached_host_object_grants(&self) -> Vec<GrantedMountRoot> {",
    ] {
        assert!(
            source_runtime_impl.contains(typed_surface),
            "workers/source hard cut regressed; source facade cached local branch should stay on a typed helper: {typed_surface}",
        );
    }

    for legacy_surface in [
        "Self::Local(source) => Ok(source.logical_roots_snapshot()),",
        "Self::Local(source) => Ok(source.host_object_grants_snapshot()),",
    ] {
        assert!(
            !source_impl.contains(legacy_surface),
            "workers/source hard cut regressed; source facade cached local branch bounced back through a raw getter: {legacy_surface}",
        );
    }

    for removed_surface in [
        "self.cached_logical_roots_snapshot_with_failure()\n            .map_err(SourceFailure::into_error)",
        "self.cached_host_object_grants_snapshot_with_failure()\n            .map_err(SourceFailure::into_error)",
        "pub fn cached_logical_roots_snapshot(&self) -> Result<Vec<RootSpec>> {",
        "pub fn cached_host_object_grants_snapshot(&self) -> Result<Vec<GrantedMountRoot>> {",
    ] {
        assert!(
            !source_impl.contains(removed_surface),
            "workers/source hard cut regressed; zero-call raw cached source wrappers should stay removed: {removed_surface}",
        );
    }
}

#[test]
fn source_facade_trigger_rescan_local_branch_uses_typed_helper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for typed_surface in [
        "pub(crate) async fn trigger_rescan_when_ready_epoch_with_failure(\n        &self,\n    ) -> std::result::Result<u64, SourceFailure> {\n        Ok(self.perform_trigger_rescan_when_ready_epoch().await)\n    }",
        "Self::Local(source) => source.trigger_rescan_when_ready_epoch_with_failure().await,",
        "pub(crate) async fn perform_trigger_rescan_when_ready_epoch(&self) -> u64 {",
    ] {
        let haystack = if typed_surface.starts_with("pub(crate) async fn perform_") {
            source_runtime_impl
        } else {
            source_impl
        };
        assert!(
            haystack.contains(typed_surface),
            "workers/source hard cut regressed; source facade/local trigger-rescan path should stay on a typed helper: {typed_surface}",
        );
    }

    assert!(
        !source_impl
            .contains("Self::Local(source) => Ok(source.trigger_rescan_when_ready_epoch_with_failure().await),"),
        "workers/source hard cut regressed; source facade trigger-rescan local branch bounced back through the raw runtime method",
    );

    for removed_surface in [
        "pub async fn trigger_rescan_when_ready_epoch(&self) -> Result<u64> {",
        "pub(crate) async fn trigger_rescan_when_ready_epoch(&self) -> Result<u64> {",
    ] {
        assert!(
            !source_impl.contains(removed_surface),
            "workers/source hard cut regressed; zero-call raw trigger-rescan wrappers should stay removed: {removed_surface}",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_targeted_rescan_rejects_without_local_primary_scan_root() {
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        SourceConfig::default(),
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let err = client
        .submit_targeted_rescan_when_ready_epoch_with_failure()
        .await
        .expect_err("targeted source-rescan must reject an empty local target");
    let CnxError::InvalidInput(message) = err.into_error() else {
        panic!("targeted rescan without local primary root should be invalid input");
    };
    assert!(
        message.contains("no local source-primary scan root"),
        "targeted rescan should explain missing local source-primary scan root: {message}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_targeted_rescan_delivery_check_reports_non_target_without_error() {
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        SourceConfig::default(),
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let acceptance = client
        .check_targeted_rescan_delivery_acceptance_with_failure()
        .await
        .expect("targeted source-rescan delivery check should return domain acceptance state");
    assert_eq!(
        acceptance,
        SourceTargetedRescanDeliveryAcceptance::NotLocalSourcePrimary,
        "delivery acceptance check should report a non-target without turning status collection into an error"
    );

    client.close().await.expect("close source worker");
}

#[test]
fn source_facade_apply_orchestration_local_branch_uses_typed_helper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for typed_surface in [
        "pub(crate) async fn apply_orchestration_signals_with_failure(",
        "Self::Local(source) => map_source_failure_timeout_result(",
        "source.apply_orchestration_signals_with_failure(signals),",
        "self.perform_apply_orchestration_signals(signals)\n            .await\n            .map_err(SourceFailure::from)",
    ] {
        assert!(
            source_impl.contains(typed_surface) || source_runtime_impl.contains(typed_surface),
            "workers/source hard cut regressed; source facade local apply-orchestration lane should stay on a typed helper: {typed_surface}",
        );
    }

    for legacy_surface in [
        "Self::Local(source) => map_source_timeout_result_with_failure(",
        "source.apply_orchestration_signals(signals),",
        "self.apply_orchestration_signals(signals)\n            .await\n            .map_err(SourceFailure::from)",
        "pub(crate) async fn apply_orchestration_signals_with_total_timeout(",
    ] {
        assert!(
            !source_impl.contains(legacy_surface),
            "workers/source hard cut regressed; source facade local apply-orchestration lane bounced back through a raw helper: {legacy_surface}",
        );
    }
}

#[test]
fn source_facade_local_observability_branches_use_local_typed_helpers() {
    let source_impl = include_str!("../source.rs");

    for typed_surface in [
        "fn local_observability_snapshot(&self) -> SourceObservabilitySnapshot {",
        "pub(crate) fn observability_snapshot_with_failure(",
        "pub(crate) fn observability_snapshot_nonblocking_for_status_route(",
        "Self::Local(source) => source.observability_snapshot_with_failure(),",
        "Self::Local(source) => source.observability_snapshot_nonblocking_for_status_route(),",
    ] {
        assert!(
            source_impl.contains(typed_surface),
            "workers/source hard cut regressed; source facade/local observability path should stay on local typed helpers: {typed_surface}",
        );
    }

    for legacy_surface in [
        "let snapshot = build_local_source_observability_snapshot(source);",
        "source.maybe_mark_rescan_observed_if_publication_advanced(",
        "Self::Local(source) => {\n                source\n                    .observability_snapshot_nonblocking_for_status_route()\n                    .0\n            }",
    ] {
        let facade_region = &source_impl[8330..8460.min(source_impl.len())];
        assert!(
            !facade_region.contains(legacy_surface),
            "workers/source hard cut regressed; source facade local observability branch bounced back through inline local snapshot shaping: {legacy_surface}",
        );
    }

    for removed_surface in [
        "pub(crate) async fn observability_snapshot_nonblocking(&self) -> SourceObservabilitySnapshot {",
        "pub(crate) fn observability_snapshot_nonblocking(&self) -> SourceObservabilitySnapshot {",
        "Self::Local(source) => source.observability_snapshot_nonblocking(),",
        "Self::Worker(client) => client.observability_snapshot_nonblocking().await,",
    ] {
        assert!(
            !source_impl.contains(removed_surface),
            "workers/source hard cut regressed; raw observability nonblocking compatibility wrapper should stay removed: {removed_surface}",
        );
    }
}

#[test]
fn source_local_force_find_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains("self.force_find(params).map_err(SourceFailure::from)"),
        "workers/source hard cut regressed; local force-find typed helper bounced back through the raw wrapper",
    );
    assert!(
        source_impl.contains("self.perform_force_find(params).map_err(SourceFailure::from)"),
        "workers/source hard cut regressed; local force-find typed helper should call the direct runtime helper",
    );
    assert!(
        source_runtime_impl.contains("pub(crate) fn perform_force_find(&self, params: &InternalQueryRequest) -> Result<Vec<Event>> {"),
        "workers/source hard cut regressed; source runtime should expose a direct force-find helper instead of forcing typed callers back through the raw wrapper",
    );
}

#[test]
fn source_local_runtime_endpoint_typed_helpers_do_not_bounce_through_raw_wrappers() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    for legacy_bounce in [
        "self.pub_().await.map_err(SourceFailure::from)",
        "self.start_runtime_endpoints(boundary)\n            .await\n            .map_err(SourceFailure::from)",
    ] {
        assert!(
            !source_impl.contains(legacy_bounce),
            "workers/source hard cut regressed; local runtime-endpoint typed helper bounced back through a raw wrapper: {legacy_bounce}",
        );
    }
    for typed_surface in [
        "self.build_pub_stream().await.map_err(SourceFailure::from)",
        "self.start_runtime_endpoints_on_boundary(boundary)\n            .await\n            .map_err(SourceFailure::from)",
        "pub(crate) async fn build_pub_stream(",
        "pub(crate) async fn start_runtime_endpoints_on_boundary(",
    ] {
        let haystack = if typed_surface.starts_with("pub(crate) async fn") {
            source_runtime_impl
        } else {
            source_impl
        };
        assert!(
            haystack.contains(typed_surface),
            "workers/source hard cut regressed; local runtime-endpoint typed helper drifted away from the direct typed path: {typed_surface}",
        );
    }
}

#[test]
fn source_local_constructor_typed_helper_does_not_bounce_through_raw_wrapper() {
    let source_impl = include_str!("../source.rs");
    let source_runtime_impl = include_str!("../../source/mod.rs");

    assert!(
        !source_impl.contains(
            "Self::with_boundaries_and_state(config, node_id, boundary, state_boundary)\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local constructor typed helper bounced back through the raw wrapper",
    );
    assert!(
        source_impl.contains(
            "Self::with_boundaries_and_state_internal(config, node_id, boundary, state_boundary, false)\n            .map_err(SourceFailure::from)"
        ),
        "workers/source hard cut regressed; local constructor typed helper should call the direct runtime helper",
    );
    assert!(
        source_runtime_impl.contains("pub(crate) fn with_boundaries_and_state_internal("),
        "workers/source hard cut regressed; source runtime should expose a direct constructor helper instead of forcing typed callers back through the raw wrapper",
    );
}

struct SourceWorkerControlFrameErrorHookReset;

impl Drop for SourceWorkerControlFrameErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_error_hook();
    }
}

struct SourceWorkerControlFrameErrorQueueHookReset;

impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_error_queue_hook();
    }
}

struct SourceWorkerStatusErrorHookReset;

impl Drop for SourceWorkerStatusErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_status_error_hook();
    }
}

struct SourceWorkerObservabilityErrorHookReset;

impl Drop for SourceWorkerObservabilityErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_observability_error_hook();
    }
}

struct SourceWorkerObservabilityCallCountHookReset;

impl Drop for SourceWorkerObservabilityCallCountHookReset {
    fn drop(&mut self) {
        clear_source_worker_observability_call_count_hook();
    }
}

struct SourceWorkerRearmCallCountHookReset;

impl Drop for SourceWorkerRearmCallCountHookReset {
    fn drop(&mut self) {
        clear_source_worker_rearm_call_count_hook();
    }
}

struct SourceWorkerAcceptTargetedDeliveryCallCountHookReset;

impl Drop for SourceWorkerAcceptTargetedDeliveryCallCountHookReset {
    fn drop(&mut self) {
        clear_source_worker_accept_targeted_delivery_call_count_hook();
    }
}

struct SourceWorkerObservabilityDelayHookReset;

impl Drop for SourceWorkerObservabilityDelayHookReset {
    fn drop(&mut self) {
        clear_source_worker_observability_delay_hook();
    }
}

async fn source_worker_observability_hook_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(())).lock().await
}

fn clear_cached_worker_observability_debug_maps_for_test(client: &SourceWorkerClientHandle) {
    client.with_cache_mut(|cache| {
        cache.last_force_find_runner_by_group = Some(std::collections::BTreeMap::new());
        cache.force_find_inflight_groups = Some(Vec::new());
        cache.scheduled_source_groups_by_node = Some(std::collections::BTreeMap::new());
        cache.scheduled_scan_groups_by_node = Some(std::collections::BTreeMap::new());
        cache.last_control_frame_signals_by_node = Some(std::collections::BTreeMap::new());
        cache.published_batches_by_node = Some(std::collections::BTreeMap::new());
        cache.published_events_by_node = Some(std::collections::BTreeMap::new());
        cache.published_control_events_by_node = Some(std::collections::BTreeMap::new());
        cache.published_data_events_by_node = Some(std::collections::BTreeMap::new());
        cache.last_published_at_us_by_node = Some(std::collections::BTreeMap::new());
        cache.last_published_origins_by_node = Some(std::collections::BTreeMap::new());
        cache.published_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.published_path_capture_target = None;
        cache.enqueued_path_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.pending_path_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.yielded_path_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.summarized_path_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.published_path_origin_counts_by_node = Some(std::collections::BTreeMap::new());
    });
}

struct SourceWorkerScheduledGroupsErrorHookReset;

impl Drop for SourceWorkerScheduledGroupsErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_scheduled_groups_error_hook();
    }
}

struct SourceWorkerControlFramePauseHookReset;

impl Drop for SourceWorkerControlFramePauseHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_pause_hook();
    }
}

struct SourceWorkerStartPauseHookReset;

impl Drop for SourceWorkerStartPauseHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_pause_hook();
    }
}

struct SourceWorkerStartErrorQueueHookReset;

impl Drop for SourceWorkerStartErrorQueueHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_error_queue_hook();
    }
}

struct SourceWorkerStartDelayHookReset;

impl Drop for SourceWorkerStartDelayHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_delay_hook();
    }
}

#[async_trait]
impl ChannelIoSubset for LoopbackWorkerBoundary {
    async fn channel_send(&self, _ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    return Ok(events);
                }
            }
            {
                let closed = self.closed.lock().expect("loopback closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.close_history
            .lock()
            .expect("loopback close history lock")
            .push(channel.0.clone());
        self.closed
            .lock()
            .expect("loopback closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

impl ChannelBoundary for LoopbackWorkerBoundary {
    fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
}

impl StateBoundary for LoopbackWorkerBoundary {}

const WORKER_BOOTSTRAP_CONTROL_FRAME_KIND: &str = "capanix.worker.bootstrap:v1";

#[derive(Debug, Clone, serde::Serialize)]
enum TestWorkerBootstrapEnvelope<P> {
    Init { node_id: String, payload: P },
}

fn bootstrap_envelope<P: serde::Serialize>(
    message: &TestWorkerBootstrapEnvelope<P>,
) -> ControlEnvelope {
    ControlEnvelope::Frame(ControlFrame {
        kind: WORKER_BOOTSTRAP_CONTROL_FRAME_KIND.to_string(),
        payload: rmp_serde::to_vec_named(message).expect("encode bootstrap envelope"),
    })
}

fn fs_meta_runtime_lib_filename() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libfs_meta_runtime.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "fs_meta_runtime.dll"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "libfs_meta_runtime.so"
    }
}

fn fs_meta_runtime_workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn worker_socket_tmp_root() -> PathBuf {
    let repo_root = fs_meta_runtime_workspace_root()
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(fs_meta_runtime_workspace_root);
    let dir = repo_root.join(".tmp").join("w");
    std::fs::create_dir_all(&dir).expect("create worker socket temp root");
    dir
}

fn worker_socket_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("t")
        .tempdir_in(worker_socket_tmp_root())
        .expect("create worker socket dir")
}

fn fs_meta_worker_module_path_candidates(root: &Path, lib_name: &str) -> [PathBuf; 4] {
    [
        root.join("target/debug").join(lib_name),
        root.join("target/debug/deps").join(lib_name),
        root.join(".target/debug").join(lib_name),
        root.join(".target/debug/deps").join(lib_name),
    ]
}

fn newest_existing_worker_module_path(
    candidates: impl IntoIterator<Item = PathBuf>,
) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, usize, PathBuf)> = None;
    for (index, candidate) in candidates.into_iter().enumerate() {
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            continue;
        };
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let replace = match best.as_ref() {
            None => true,
            Some((best_modified, best_index, _)) => {
                modified > *best_modified || (modified == *best_modified && index < *best_index)
            }
        };
        if replace {
            best = Some((modified, index, candidate));
        }
    }
    best.map(|(_, _, path)| path)
}

fn resolve_fs_meta_worker_module_path_from_workspace_root(root: &Path) -> Option<PathBuf> {
    newest_existing_worker_module_path(fs_meta_worker_module_path_candidates(
        root,
        fs_meta_runtime_lib_filename(),
    ))
}

fn fs_meta_worker_module_path() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
            if let Ok(path) = std::env::var(name) {
                let resolved = PathBuf::from(path);
                if resolved.exists() {
                    return resolved;
                }
            }
        }
        resolve_fs_meta_worker_module_path_from_workspace_root(&fs_meta_runtime_workspace_root())
            .unwrap_or_else(|| {
                panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY")
            })
    })
    .clone()
}

fn external_source_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
    external_source_worker_binding_with_module_path(socket_dir, &fs_meta_worker_module_path())
}

fn external_source_worker_binding_with_module_path(
    socket_dir: &Path,
    module_path: &Path,
) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: "source".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(module_path.to_path_buf()),
        socket_dir: Some(socket_dir.to_path_buf()),
    }
}

fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: "sink".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(fs_meta_worker_module_path()),
        socket_dir: Some(socket_dir.to_path_buf()),
    }
}

fn worker_source_root(id: &str, path: &Path) -> RootSpec {
    let mut root = RootSpec::new(id, path);
    root.watch = false;
    root.scan = true;
    root
}

fn worker_watch_scan_root(id: &str, path: &Path) -> RootSpec {
    RootSpec::new(id, path)
}

#[test]
fn prime_cached_schedule_from_control_signals_respects_watch_and_scan_flags() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let roots = vec![
        worker_source_root("nfs1", &nfs1),
        worker_source_root("nfs2", &nfs2),
    ];
    let grants = vec![
        worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1),
        worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2),
    ];
    let signals = source_control_signals_from_envelopes(&[
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ])
    .expect("decode source control signals");

    let mut cache = SourceWorkerSnapshotCache::default();
    let summary = prime_cached_schedule_from_control_signals(
        &mut cache,
        &NodeId("node-a".to_string()),
        &signals,
        &roots,
        &grants,
    );

    assert!(
        summary.saw_activate_with_bound_scopes,
        "control-signal priming should observe local activate scopes"
    );
    assert!(
        summary.has_local_runnable_groups,
        "scan-enabled roots should still report local runnable groups"
    );
    assert!(
        cache
            .scheduled_source_groups_by_node
            .as_ref()
            .is_none_or(|groups| groups.is_empty()),
        "watch-disabled roots must not prime scheduled source groups from control signals: {:?}",
        cache.scheduled_source_groups_by_node
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "scan-enabled roots should still prime scheduled scan groups"
    );
}

#[test]
fn peer_only_activate_wave_does_not_clear_cached_local_runtime_scope() {
    let tmp = tempdir().expect("create temp dir");
    let node_b_nfs1 = tmp.path().join("node-b-nfs1");
    let node_c_nfs2 = tmp.path().join("node-c-nfs2");
    std::fs::create_dir_all(&node_b_nfs1).expect("create node-b nfs1 dir");
    std::fs::create_dir_all(&node_c_nfs2).expect("create node-c nfs2 dir");

    let roots = vec![
        worker_watch_scan_root("nfs1", &node_b_nfs1),
        worker_watch_scan_root("nfs2", &node_c_nfs2),
    ];
    let grants = vec![
        worker_source_export("node-b::nfs1", "node-b", "10.0.0.21", node_b_nfs1),
        worker_source_export("node-c::nfs2", "node-c", "10.0.0.32", node_c_nfs2),
    ];
    let signals = source_control_signals_from_envelopes(&[
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-c::nfs2"])],
        }))
        .expect("encode peer source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-c::nfs2"])],
        }))
        .expect("encode peer scan activate"),
    ])
    .expect("decode peer-only control signals");

    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    let summary = prime_cached_schedule_from_control_signals(
        &mut cache,
        &NodeId("node-b-29820128679397065901473793".to_string()),
        &signals,
        &roots,
        &grants,
    );

    assert!(
        summary.saw_activate_with_bound_scopes,
        "peer-only route activation should still be observed as an activate wave"
    );
    assert!(
        !summary.has_local_runnable_groups,
        "peer-only route activation should not claim local runnable groups"
    );
    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )])),
        "a peer-only source activation must not erase the previously accepted local source runtime-scope summary"
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )])),
        "a peer-only scan activation must not erase the previously accepted local scan runtime-scope summary"
    );
}

#[test]
fn replay_recovery_cached_schedule_primes_desired_local_groups_from_grants() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let roots = vec![worker_source_root("nfs1", &nfs1)];
    let grants = vec![
        worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1),
        worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2),
    ];
    let signals = source_control_signals_from_envelopes(&[
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ])
    .expect("decode source control signals");

    let mut cache = SourceWorkerSnapshotCache {
        logical_roots: Some(roots.clone()),
        grants: Some(grants.clone()),
        ..SourceWorkerSnapshotCache::default()
    };
    let _summary = prime_cached_schedule_from_control_signals(
        &mut cache,
        &NodeId("node-a".to_string()),
        &signals,
        &roots,
        &grants,
    );

    assert!(
        cache
            .scheduled_source_groups_by_node
            .as_ref()
            .is_none_or(|groups| groups.is_empty()),
        "control-signal priming should still respect watch-disabled roots before replay recovery"
    );

    prime_cached_schedule_from_control_signals_for_replay_recovery(
        &mut cache,
        &NodeId("node-a".to_string()),
        &signals,
        &roots,
        &grants,
    );

    assert!(
        cache
            .replay_recovery_scheduled_source_groups_by_node
            .as_ref()
            .is_none_or(|groups| groups.is_empty()),
        "replay-recovery bridge must keep watch-disabled roots out of source groups"
    );
    assert_eq!(
        cache.replay_recovery_scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )])),
        "replay-recovery bridge should only prime scan groups backed by known logical roots"
    );
}

#[test]
fn merge_cached_local_scheduled_groups_prefers_replay_recovery_bridge_over_live_empty() {
    let live_groups = Some(std::collections::BTreeSet::new());
    let cached = Some(std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["stale".to_string()],
    )]));
    let replay_recovery = Some(std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]));

    assert_eq!(
        merge_cached_local_scheduled_groups(
            &NodeId("node-a".to_string()),
            live_groups,
            &cached,
            &replay_recovery,
        ),
        Some(std::collections::BTreeSet::from([
            "nfs1".to_string(),
            "nfs2".to_string(),
        ])),
        "explicit empty live groups should not erase the replay-recovery schedule bridge"
    );
}

#[test]
fn prime_cached_schedule_from_control_signals_for_replay_recovery_replaces_prior_local_groups() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let roots = vec![worker_watch_scan_root("nfs1", &nfs1)];
    let grants = vec![worker_source_export(
        "node-a::nfs1",
        "node-a",
        "127.0.0.1",
        nfs1,
    )];
    let signals = source_control_signals_from_envelopes(&[encode_runtime_exec_control(
        &RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
        }),
    )
    .expect("encode source activate")])
    .expect("decode source control signals");

    let mut cache = SourceWorkerSnapshotCache {
        logical_roots: Some(roots),
        grants: Some(grants.clone()),
        replay_recovery_scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([
            ("node-a".to_string(), vec!["stale".to_string()]),
            ("node-b".to_string(), vec!["other".to_string()]),
        ])),
        ..SourceWorkerSnapshotCache::default()
    };
    let fallback_roots = cache.logical_roots.clone().unwrap_or_default();

    prime_cached_schedule_from_control_signals_for_replay_recovery(
        &mut cache,
        &NodeId("node-a".to_string()),
        &signals,
        &fallback_roots,
        &grants,
    );

    assert_eq!(
        cache.replay_recovery_scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([
            ("node-a".to_string(), vec!["nfs1".to_string()]),
            ("node-b".to_string(), vec!["other".to_string()]),
        ])),
        "latest replay-recovery priming should replace stale local groups without disturbing other nodes"
    );
}

#[test]
fn fs_meta_worker_module_path_prefers_newer_debug_deps_cdylib_over_stale_top_level_debug_cdylib() {
    let tmp = tempdir().expect("create temp dir");
    let lib_name = fs_meta_runtime_lib_filename();
    let stale = tmp.path().join("target/debug").join(lib_name);
    let fresh = tmp.path().join("target/debug/deps").join(lib_name);
    std::fs::create_dir_all(stale.parent().expect("stale parent")).expect("create stale dir");
    std::fs::create_dir_all(fresh.parent().expect("fresh parent")).expect("create fresh dir");
    std::fs::write(&stale, b"stale").expect("write stale module");
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(&fresh, b"fresh").expect("write fresh module");

    let resolved = resolve_fs_meta_worker_module_path_from_workspace_root(tmp.path())
        .expect("resolve worker module path");

    assert_eq!(
        resolved, fresh,
        "external worker tests must select the freshest built fs-meta cdylib instead of a stale top-level debug artifact"
    );
}

fn worker_fs_source_watch_scan_root(id: &str, fs_source: &str) -> RootSpec {
    RootSpec {
        id: id.to_string(),
        selector: crate::source::config::RootSelector {
            mount_point: None,
            fs_source: Some(fs_source.to_string()),
            fs_type: None,
            host_ip: None,
            host_ref: None,
        },
        subpath_scope: PathBuf::from("/"),
        watch: true,
        scan: true,
        audit_interval_ms: None,
    }
}

fn worker_source_export(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: PathBuf,
) -> GrantedMountRoot {
    GrantedMountRoot {
        object_ref: object_ref.to_string(),
        host_ref: host_ref.to_string(),
        host_ip: host_ip.to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point,
        fs_source: object_ref.to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }
}

fn worker_source_export_with_fs_source(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: PathBuf,
    fs_source: &str,
) -> GrantedMountRoot {
    GrantedMountRoot {
        object_ref: object_ref.to_string(),
        host_ref: host_ref.to_string(),
        host_ip: host_ip.to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point,
        fs_source: fs_source.to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }
}

fn worker_route_export(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: &Path,
) -> RuntimeHostGrant {
    RuntimeHostGrant {
        object_ref: object_ref.to_string(),
        object_type: RuntimeHostObjectType::MountRoot,
        interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
        host: RuntimeHostDescriptor {
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
        },
        object: RuntimeObjectDescriptor {
            mount_point: mount_point.display().to_string(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
        },
        grant_state: RuntimeHostGrantState::Active,
    }
}

fn worker_route_export_with_fs_source(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: &Path,
    fs_source: &str,
) -> RuntimeHostGrant {
    RuntimeHostGrant {
        object_ref: object_ref.to_string(),
        object_type: RuntimeHostObjectType::MountRoot,
        interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
        host: RuntimeHostDescriptor {
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
        },
        object: RuntimeObjectDescriptor {
            mount_point: mount_point.display().to_string(),
            fs_source: fs_source.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
        },
        grant_state: RuntimeHostGrantState::Active,
    }
}

fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
    RuntimeBoundScope {
        scope_id: scope_id.to_string(),
        resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
    }
}

fn selected_group_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
    InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: path.to_vec(),
            recursive: false,
            max_depth: Some(0),
            selected_group: Some(group_id.to_string()),
        },
        None,
    )
}

fn selected_group_force_find_request(group_id: &str) -> InternalQueryRequest {
    InternalQueryRequest::force_find(
        QueryOp::Tree,
        QueryScope {
            path: b"/".to_vec(),
            recursive: true,
            max_depth: None,
            selected_group: Some(group_id.to_string()),
        },
    )
}

fn selected_group_tree_contains_path(
    sink: &SinkFileMeta,
    group_id: &str,
    query_path: &[u8],
    expected_path: &[u8],
) -> bool {
    let events = sink
        .materialized_query_with_failure(&selected_group_request(query_path, group_id))
        .expect("selected-group materialized query");
    events.iter().any(|event| {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .expect("decode selected-group materialized payload");
        let MaterializedQueryPayload::Tree(response) = payload else {
            return false;
        };
        (response.root.exists && response.root.path.as_slice() == expected_path)
            || response
                .entries
                .iter()
                .any(|entry| entry.path.as_slice() == expected_path)
    })
}

fn decode_exact_query_node(events: Vec<Event>, path: &[u8]) -> Result<Option<QueryNode>> {
    let mut selected = None::<QueryNode>;
    for event in &events {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .map_err(|e| CnxError::Internal(format!("decode query response failed: {e}")))?;
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
                path: entry.path.clone(),
                file_name: root_file_name_bytes(&entry.path),
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

async fn recv_loopback_events(
    boundary: &LoopbackWorkerBoundary,
    timeout_ms: u64,
) -> Result<Vec<Event>> {
    boundary
        .channel_recv(
            BoundaryContext::default(),
            ChannelRecvRequest {
                channel_key: ChannelKey("fs-meta.events:v1.stream".to_string()),
                timeout_ms: Some(timeout_ms),
            },
        )
        .await
}

fn record_control_and_data_counts(
    control_counts: &mut std::collections::BTreeMap<String, usize>,
    data_counts: &mut std::collections::BTreeMap<String, usize>,
    batch: Vec<Event>,
) {
    for event in batch {
        let origin = event.metadata().origin_id.0.clone();
        if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
            *control_counts.entry(origin).or_insert(0) += 1;
        } else {
            *data_counts.entry(origin).or_insert(0) += 1;
        }
    }
}

fn record_path_data_counts(
    path_counts: &mut std::collections::BTreeMap<String, usize>,
    batch: &[Event],
    target: &[u8],
) {
    for event in batch {
        let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
            continue;
        };
        if is_under_query_path(&record.path, target) {
            *path_counts
                .entry(event.metadata().origin_id.0.clone())
                .or_insert(0) += 1;
        }
    }
}

#[test]
fn source_worker_rpc_preserves_invalid_input_response_category() {
    let err = SourceWorkerRpc::into_result(SourceWorkerResponse::InvalidInput(
        "duplicate source root id 'dup'".to_string(),
    ))
    .expect_err("invalid input response must become an error");
    match err {
        CnxError::InvalidInput(message) => assert!(message.contains("duplicate")),
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn degraded_worker_observability_uses_cached_snapshot() {
    let cache = SourceWorkerSnapshotCache {
        lifecycle_state: Some("ready".to_string()),
        last_live_observability_snapshot_at: None,
        host_object_grants_version: Some(7),
        grants: Some(vec![GrantedMountRoot {
            object_ref: "obj-a".to_string(),
            host_ref: "host-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: PathBuf::from("/mnt/nfs-a"),
            fs_source: "nfs://server/export".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: vec![],
            interfaces: vec![],
            active: true,
        }]),
        logical_roots: Some(vec![RootSpec::new("root-a", "/mnt/nfs-a")]),
        status: Some(SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: vec![],
            concrete_roots: vec![],
            degraded_roots: vec![("existing-root".to_string(), "already degraded".to_string())],
        }),
        source_primary_by_group: Some(std::collections::BTreeMap::from([(
            "group-a".to_string(),
            "obj-a".to_string(),
        )])),
        last_force_find_runner_by_group: Some(std::collections::BTreeMap::from([(
            "group-a".to_string(),
            "obj-a".to_string(),
        )])),
        force_find_inflight_groups: Some(vec!["group-a".to_string()]),
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["group-a".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["group-a".to_string()],
        )])),
        replay_recovery_scheduled_source_groups_by_node: None,
        replay_recovery_scheduled_scan_groups_by_node: None,
        last_control_frame_signals_by_node: Some(std::collections::BTreeMap::new()),
        observability_control_summary_override_by_node: None,
        published_batches_by_node: Some(std::collections::BTreeMap::new()),
        published_events_by_node: Some(std::collections::BTreeMap::new()),
        published_control_events_by_node: Some(std::collections::BTreeMap::new()),
        published_data_events_by_node: Some(std::collections::BTreeMap::new()),
        last_published_at_us_by_node: Some(std::collections::BTreeMap::new()),
        last_published_origins_by_node: Some(std::collections::BTreeMap::new()),
        published_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        pending_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        yielded_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        summarized_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        published_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        rescan_request_epoch: 0,
        rescan_request_published_batches: 0,
        rescan_request_last_published_at_us: 0,
        rescan_request_last_audit_completed_at_us: 0,
        rescan_observed_epoch: 0,
    };

    let snapshot =
        build_degraded_worker_observability_snapshot(&cache, "source worker unavailable");

    assert_eq!(snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE);
    assert_eq!(snapshot.host_object_grants_version, 7);
    assert_eq!(snapshot.grants.len(), 1);
    assert_eq!(snapshot.logical_roots.len(), 1);
    assert_eq!(
        snapshot.source_primary_by_group.get("group-a"),
        Some(&"obj-a".to_string())
    );
    assert_eq!(
        snapshot.last_force_find_runner_by_group.get("group-a"),
        Some(&"obj-a".to_string())
    );
    assert_eq!(
        snapshot.force_find_inflight_groups,
        vec!["group-a".to_string()]
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["group-a".to_string()])
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["group-a".to_string()])
    );
    assert_eq!(snapshot.status.degraded_roots.len(), 2);
    assert_eq!(
        snapshot.status.degraded_roots[1],
        (
            SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(),
            "source worker unavailable".to_string()
        )
    );
}

#[test]
fn successful_refresh_does_not_clear_cached_scheduled_groups_when_latest_publication_is_empty() {
    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    update_cached_scheduled_groups_from_refresh(
        &mut cache,
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::new(),
    );

    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not erase the last known non-empty scheduled source publication when the immediate refresh snapshot is transiently empty",
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not erase the last known non-empty scheduled scan publication when the immediate refresh snapshot is transiently empty",
    );
}

#[test]
fn successful_refresh_explicit_empty_node_clears_cached_scheduled_groups_for_that_node() {
    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    update_cached_scheduled_groups_from_refresh(
        &mut cache,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::new())]),
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::new())]),
    );

    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            Vec::<String>::new(),
        )])),
        "an explicit empty scheduled-source publication for a known node must clear stale cached groups instead of replaying them",
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            Vec::<String>::new(),
        )])),
        "an explicit empty scheduled-scan publication for a known node must clear stale cached groups instead of replaying them",
    );
}

#[test]
fn successful_refresh_does_not_drop_cached_scheduled_groups_when_latest_publication_is_partial() {
    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    update_cached_scheduled_groups_from_refresh(
        &mut cache,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
    );

    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not drop previously accepted source scopes when the immediate live publication is still a strict subset",
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not drop previously accepted scan scopes when the immediate live publication is still a strict subset",
    );
}

#[test]
fn merge_non_empty_cached_groups_preserves_explicit_empty_node_groups_in_live_snapshot() {
    let mut current = std::collections::BTreeMap::from([("node-b".to_string(), Vec::new())]);
    let cached = Some(std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]));

    merge_non_empty_cached_groups(&mut current, &cached);

    assert_eq!(
        current,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::<String>::new())]),
        "an explicit empty scheduled-group reply for a known node must survive live-snapshot cache merge instead of replaying stale cached groups",
    );
}

#[test]
fn normalize_node_groups_key_preserves_explicit_empty_groups_when_instance_suffix_collapses_to_stable_host_ref()
 {
    let mut groups_by_node = std::collections::BTreeMap::from([
        (
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        ),
        (
            "node-b-29775497172756365788053505".to_string(),
            Vec::<String>::new(),
        ),
    ]);

    normalize_node_groups_key(
        &mut groups_by_node,
        "node-b-29775497172756365788053505",
        "node-b",
    );

    assert_eq!(
        groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::<String>::new())]),
        "instance-suffixed explicit empty groups must clear the stable host-ref entry instead of replaying stale stable-host groups",
    );
}

#[test]
fn normalize_observability_snapshot_scheduled_group_keys_preserves_explicit_empty_groups_for_cached_stable_host_ref()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let node_id = NodeId("node-b-29775497172756365788053505".to_string());
    let cached_grants = vec![worker_source_export(
        "node-b::nfs1",
        "node-b",
        "10.0.0.21",
        nfs1,
    )];
    let mut snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 2,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::from([
            ("node-b".to_string(), vec!["nfs1".to_string()]),
            (
                "node-b-29775497172756365788053505".to_string(),
                Vec::<String>::new(),
            ),
        ]),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::from([
            ("node-b".to_string(), vec!["nfs1".to_string()]),
            (
                "node-b-29775497172756365788053505".to_string(),
                Vec::<String>::new(),
            ),
        ]),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    normalize_observability_snapshot_scheduled_group_keys(
        &mut snapshot,
        &node_id,
        Some(&cached_grants),
        None,
    );

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::<String>::new())]),
        "stable-host normalization must preserve an explicit empty source schedule when cached grants collapse an instance-suffixed node back to the stable host-ref",
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::<String>::new())]),
        "stable-host normalization must preserve an explicit empty scan schedule when cached grants collapse an instance-suffixed node back to the stable host-ref",
    );
}

#[test]
fn merge_cached_local_scheduled_groups_preserves_explicit_empty_live_groups_for_local_node() {
    let node_id = NodeId("node-b".to_string());
    let cached = Some(std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]));

    let merged = merge_cached_local_scheduled_groups(
        &node_id,
        Some(std::collections::BTreeSet::new()),
        &cached,
        &None,
    );

    assert_eq!(
        merged,
        Some(std::collections::BTreeSet::new()),
        "an explicit empty local scheduled-group reply must survive local cache merge instead of replaying stale cached groups",
    );
}

#[test]
fn merge_cached_local_scheduled_groups_preserves_explicit_empty_live_groups_without_cached_fallback()
 {
    let node_id = NodeId("node-b".to_string());

    let merged = merge_cached_local_scheduled_groups(
        &node_id,
        Some(std::collections::BTreeSet::new()),
        &None,
        &None,
    );

    assert_eq!(
        merged,
        Some(std::collections::BTreeSet::new()),
        "an explicit empty local scheduled-group reply must stay explicit even when there is no cached fallback",
    );
}

#[test]
fn scheduled_groups_by_node_preserves_explicit_empty_reply_for_known_node() {
    let node_id = NodeId("node-b".to_string());
    let grants = vec![GrantedMountRoot {
        object_ref: "node-b::nfs1".to_string(),
        host_ref: "node-b".to_string(),
        host_ip: "10.0.0.21".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point: PathBuf::from("/mnt/nfs1"),
        fs_source: "nfs://server/export1".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }];

    let groups = scheduled_groups_by_node(
        &node_id,
        &grants,
        Ok(Some(std::collections::BTreeSet::new())),
    );

    assert_eq!(
        groups,
        std::collections::BTreeMap::from([("node-b".to_string(), Vec::<String>::new())]),
        "an explicit empty scheduled-group reply for a known node must survive stable-host normalization instead of collapsing into omission",
    );
}

#[test]
fn post_ack_schedule_refresh_is_skipped_for_pure_host_grant_change_retries() {
    let signals = vec![SourceControlSignal::RuntimeHostGrantChange {
        changed: RuntimeHostGrantChange {
            version: 7,
            grants: Vec::new(),
        },
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.host-grant-change".to_string(),
            payload: Vec::new(),
        }),
    }];

    assert!(
        matches!(
            SourceControlFrameSignalPolicies::from_signals(&signals).post_ack_refresh_requirement,
            SourceControlFramePostAckRefreshRequirement::NotRequired
        ),
        "pure host-grant retry batches must stay grant-only and skip post-ack scheduled-group refresh work",
    );
}

#[test]
fn post_ack_schedule_refresh_is_skipped_for_cleanup_only_deactivate_tails() {
    let signals = vec![
        SourceControlSignal::Deactivate {
            unit: SourceRuntimeUnit::Source,
            route_key: ROUTE_KEY_QUERY.to_string(),
            generation: 5,
            envelope: ControlEnvelope::Frame(ControlFrame {
                kind: "test.cleanup-only.source".to_string(),
                payload: Vec::new(),
            }),
        },
        SourceControlSignal::Deactivate {
            unit: SourceRuntimeUnit::Scan,
            route_key: ROUTE_KEY_QUERY.to_string(),
            generation: 5,
            envelope: ControlEnvelope::Frame(ControlFrame {
                kind: "test.cleanup-only.scan".to_string(),
                payload: Vec::new(),
            }),
        },
    ];

    assert!(
        matches!(
            SourceControlFrameSignalPolicies::from_signals(&signals).post_ack_refresh_requirement,
            SourceControlFramePostAckRefreshRequirement::NotRequired
        ),
        "cleanup-only deactivate tails must stay local and skip post-ack scheduled-group refresh work",
    );
}

#[test]
fn post_ack_schedule_refresh_stays_enabled_for_activate_waves() {
    let signals = vec![SourceControlSignal::Activate {
        unit: SourceRuntimeUnit::Source,
        route_key: ROUTE_KEY_QUERY.to_string(),
        generation: 5,
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: "nfs1".to_string(),
            resource_ids: vec!["node-a::nfs1".to_string()],
        }],
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.activate".to_string(),
            payload: Vec::new(),
        }),
    }];

    assert!(
        matches!(
            SourceControlFrameSignalPolicies::from_signals(&signals).post_ack_refresh_requirement,
            SourceControlFramePostAckRefreshRequirement::Required
        ),
        "activate waves must keep driving post-ack scheduled-group refresh so stale shared clients are still discarded on replay-owned continuity gaps",
    );
}

fn source_control_state_test_activate_signal(
    unit: SourceRuntimeUnit,
    route_key: &str,
    generation: u64,
    bound_scopes: Vec<RuntimeBoundScope>,
) -> SourceControlSignal {
    SourceControlSignal::Activate {
        unit,
        route_key: route_key.to_string(),
        generation,
        bound_scopes,
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: format!("test.activate.{route_key}.{generation}"),
            payload: Vec::new(),
        }),
    }
}

fn source_control_state_test_tick_signal(
    unit: SourceRuntimeUnit,
    route_key: &str,
    generation: u64,
) -> SourceControlSignal {
    SourceControlSignal::Tick {
        unit,
        route_key: route_key.to_string(),
        generation,
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: format!("test.tick.{route_key}.{generation}"),
            payload: Vec::new(),
        }),
    }
}

fn source_control_state_envelope_kinds(envelopes: Vec<ControlEnvelope>) -> Vec<String> {
    envelopes
        .into_iter()
        .map(|envelope| match envelope {
            ControlEnvelope::Frame(frame) => frame.kind,
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn source_control_state_tracks_replay_requirement_and_retained_envelopes() {
    let host_grant = SourceControlSignal::RuntimeHostGrantChange {
        changed: RuntimeHostGrantChange {
            version: 7,
            grants: Vec::new(),
        },
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.host-grant".to_string(),
            payload: Vec::new(),
        }),
    };
    let source_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        "query.stream",
        2,
        vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
    );
    let scan_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Scan,
        "query.stream",
        2,
        vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
    );
    let source_deactivate = SourceControlSignal::Deactivate {
        unit: SourceRuntimeUnit::Source,
        route_key: "query.stream".to_string(),
        generation: 3,
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.deactivate.query.stream.3".to_string(),
            payload: Vec::new(),
        }),
    };

    let mut state = SourceControlState::default();
    assert_eq!(
        state.replay_state(),
        SourceControlReplayState::Clear,
        "fresh source control state must start replay-clear as typed replay state",
    );

    state.arm_replay();
    assert_eq!(
        state.replay_state(),
        SourceControlReplayState::Required,
        "replacing or recovering a shared source worker must arm replay on the canonical control state",
    );
    assert_eq!(
        state.take_replay_state(),
        SourceControlReplayState::Required,
        "the first retained replay attempt must observe and clear the replay requirement",
    );
    assert_eq!(
        state.replay_state(),
        SourceControlReplayState::Clear,
        "taking the replay requirement must clear it until a later reconnect re-arms it",
    );
    assert_eq!(
        state.take_replay_state(),
        SourceControlReplayState::Clear,
        "replay take must be edge-triggered rather than repeatedly reporting stale replay work",
    );

    state.retain_signals(&[
        host_grant.clone(),
        source_activate.clone(),
        scan_activate.clone(),
    ]);
    assert_eq!(
        source_control_state_envelope_kinds(state.replay_envelopes()),
        vec![
            "test.host-grant".to_string(),
            "test.activate.query.stream.2".to_string(),
            "test.activate.query.stream.2".to_string(),
        ],
        "retained replay must be rebuilt from the canonical control state in host-grant then active-route order",
    );

    state.retain_signals(&[source_deactivate]);
    assert_eq!(
        source_control_state_envelope_kinds(state.replay_envelopes()),
        vec![
            "test.host-grant".to_string(),
            "test.activate.query.stream.2".to_string(),
            "test.deactivate.query.stream.3".to_string(),
        ],
        "canonical source control state must retain the latest directive per route so replay can carry deactivates without a shadow retained-state machine",
    );

    state.arm_replay();
    state.arm_replay();
    assert_eq!(
        state.replay_state(),
        SourceControlReplayState::Required,
        "replay arming must stay sticky even if multiple reconnect paths race to require replay",
    );
}

#[test]
fn source_control_state_classifies_tick_only_replay_paths_from_canonical_state() {
    let activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        "query.stream",
        2,
        vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
    );
    let matching_tick =
        source_control_state_test_tick_signal(SourceRuntimeUnit::Source, "query.stream", 2);
    let advanced_tick =
        source_control_state_test_tick_signal(SourceRuntimeUnit::Source, "query.stream", 3);
    let stale_tick =
        source_control_state_test_tick_signal(SourceRuntimeUnit::Source, "query.stream", 1);

    let mut state = SourceControlState::default();
    state.retain_signals(&[activate.clone()]);

    assert!(
        matches!(
            state.classify_tick_only_wave(&[matching_tick.clone()]),
            Some(SourceControlFrameTickOnlyDisposition::RetainedTickFastPath { .. })
        ),
        "matching-generation tick-only followups should stay local when replay is already clear",
    );

    state.arm_replay();
    assert!(
        matches!(
            state.classify_tick_only_wave(&[matching_tick]),
            Some(SourceControlFrameTickOnlyDisposition::RetainedTickReplay)
        ),
        "matching-generation tick-only followups must replay retained control state when replay remains armed",
    );
    assert!(
        matches!(
            state.classify_tick_only_wave(&[advanced_tick.clone()]),
            Some(SourceControlFrameTickOnlyDisposition::RetainedTickReplay)
        ),
        "generation-advanced tick-only followups must replay retained control state when replay remains armed",
    );
    state.take_replay_state();
    match state.classify_tick_only_wave(&[advanced_tick.clone()]) {
        Some(SourceControlFrameTickOnlyDisposition::RetainedTickFastPath { signals }) => {
            assert_eq!(signals.len(), 2);
            assert!(
                matches!(
                    &signals[0],
                    SourceControlSignal::Activate {
                        unit: SourceRuntimeUnit::Source,
                        route_key,
                        generation: 2,
                        ..
                    } if route_key == "query.stream"
                ),
                "generation-advanced tick fast path must keep retained activate summary before appending the tick: {signals:?}"
            );
            match &signals[1] {
                SourceControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                    ..
                } => {
                    assert_eq!(*unit, SourceRuntimeUnit::Source);
                    assert_eq!(route_key, "query.stream");
                    assert_eq!(*generation, 3);
                }
                other => panic!("expected retained generation-advanced tick, got {other:?}"),
            }
        }
        other => panic!("expected generation-advanced source tick fast path, got {other:?}"),
    }
    match state.classify_tick_only_wave(&[stale_tick]) {
        Some(SourceControlFrameTickOnlyDisposition::RetainedTickFastPath { signals }) => {
            assert!(
                signals.is_empty(),
                "generation-regressed source tick must be ignored without reopening the worker path"
            );
        }
        other => panic!("expected stale source tick no-op fast path, got {other:?}"),
    }
    assert!(
        matches!(
            state.classify_tick_only_wave(&[source_control_state_test_tick_signal(
                SourceRuntimeUnit::Scan,
                "missing.stream",
                3,
            )]),
            None
        ),
        "tick-only followups for unknown routes must not be fast-pathed",
    );
    assert!(
        matches!(state.classify_tick_only_wave(&[activate]), None),
        "non-tick waves must not be misclassified as tick-only replay paths",
    );
}

#[test]
fn source_control_state_tick_fast_path_carries_retained_scoped_source_route_activation() {
    let node_id = "node-d-29821409674168713353363457";
    let scoped_source_route = format!("{}.req", source_rescan_route_key_for(node_id));
    let scan_route = format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL);
    let source_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        &scoped_source_route,
        1777494559668,
        vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
    );
    let scan_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Scan,
        &scan_route,
        1777494559668,
        vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
    );
    let scan_tick =
        source_control_state_test_tick_signal(SourceRuntimeUnit::Scan, &scan_route, 1777494602653);

    let mut state = SourceControlState::default();
    state.retain_signals(&[source_activate, scan_activate]);

    match state.classify_tick_only_wave(&[scan_tick]) {
        Some(SourceControlFrameTickOnlyDisposition::RetainedTickFastPath { signals }) => {
            assert!(
                signals.iter().any(|signal| {
                    matches!(
                        signal,
                        SourceControlSignal::Activate {
                            unit: SourceRuntimeUnit::Source,
                            route_key,
                            generation: 1777494559668,
                            ..
                        } if route_key == &scoped_source_route
                    )
                }),
                "tick-only fast path must not erase the retained scoped source route activation needed by manual-rescan readiness: {signals:?}"
            );
            assert!(
                signals
                    .iter()
                    .any(|signal| matches!(signal, SourceControlSignal::Tick { .. })),
                "tick-only fast path should still expose the accepted tick: {signals:?}"
            );
        }
        other => panic!("expected retained tick fast path, got {other:?}"),
    }
}

#[test]
fn source_control_state_retains_latest_deactivate_directive_for_replay() {
    let source_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        "query.stream",
        2,
        vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
    );
    let source_deactivate = SourceControlSignal::Deactivate {
        unit: SourceRuntimeUnit::Source,
        route_key: "query.stream".to_string(),
        generation: 3,
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.deactivate.query.stream.3".to_string(),
            payload: Vec::new(),
        }),
    };

    let mut state = SourceControlState::default();
    state.retain_signals(std::slice::from_ref(&source_activate));
    state.retain_signals(std::slice::from_ref(&source_deactivate));

    assert_eq!(
        source_control_state_envelope_kinds(state.replay_envelopes()),
        vec!["test.deactivate.query.stream.3".to_string()],
        "canonical source control state must retain the latest deactivate directive so downstream runtime recovery does not need a shadow retained-state machine",
    );
}

#[test]
fn source_control_state_matches_generation_one_activate_wave_from_canonical_state() {
    let retained_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        "query.stream",
        1,
        vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
    );
    let mismatched_activate = source_control_state_test_activate_signal(
        SourceRuntimeUnit::Source,
        "query.stream",
        1,
        vec![bound_scope_with_resources("nfs2", &["node-a::nfs2"])],
    );

    let mut state = SourceControlState::default();
    state.retain_signals(std::slice::from_ref(&retained_activate));

    assert!(
        state.matches_generation_one_activate_wave(std::slice::from_ref(&retained_activate)),
        "generation-one activate replays should match directly against the canonical source control state",
    );
    assert!(
        !state.matches_generation_one_activate_wave(std::slice::from_ref(&mismatched_activate)),
        "generation-one fast-path replay must reject activate waves whose bound scopes diverge from retained canonical state",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_start_converges_on_fresh_handle() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fresh external source worker start timed out: stderr={}",
                worker_stderr_excerpt(worker_socket_dir.path())
            )
        })
        .expect("start source worker");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rearm_source_rescan_endpoints_replays_retained_control_before_ready_proof() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let scoped_route = source_rescan_request_route_for("node-b").0;
    let source_activate = |generation| {
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: scoped_route.clone(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
        }))
        .expect("encode source scoped rescan activate")
    };
    let scan_activate = |generation| {
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
        }))
        .expect("encode scan rescan activate")
    };

    client
        .on_control_frame(vec![
            source_activate(1777519450001),
            scan_activate(1777519450001),
        ])
        .await
        .expect("apply initial source rescan control");
    client
        .rearm_source_rescan_request_endpoints_with_failure()
        .await
        .expect("record initial route-ready proof");

    let retained_control = vec![source_activate(1777519455077), scan_activate(1777519455077)];
    let retained_signals = source_control_signals_from_envelopes(&retained_control)
        .expect("decode retained source rescan control");
    client.retain_control_signals(&retained_signals).await;
    client.control_state.lock().await.arm_replay();

    client
        .rearm_source_rescan_request_endpoints_with_failure()
        .await
        .expect("rearm should first replay retained control then record ready proof");
    let snapshot = client
        .observability_snapshot_with_timeout_with_failure(Duration::from_secs(5))
        .await
        .expect("read source snapshot after replay-aware rearm");
    let ready =
        format!("ready unit=runtime.exec.source route={scoped_route} generation=1777519455077");
    assert!(
        snapshot
            .last_control_frame_signals_by_node
            .get("node-b")
            .is_some_and(|signals| signals.iter().any(|signal| signal == &ready)),
        "source rescan ready proof must survive retained replay and match the latest activation: expected={ready} control={:?}",
        snapshot.last_control_frame_signals_by_node
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_publish_manual_rescan_signal_runs_through_started_worker_signal_cell() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary.clone());
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    let signal = SignalCell::from_state_boundary(
        crate::runtime::execution_units::SOURCE_RUNTIME_UNIT_ID,
        "manual_rescan",
        state_boundary,
    )
    .expect("construct manual rescan signal cell");
    let offset = signal.current_seq();
    assert!(
        client
            .existing_client()
            .await
            .expect("check existing client before publish")
            .is_none(),
        "test starts with no live worker client so a local signal-cell write would be invisible as worker-owned publication"
    );
    let cache_epoch_before = client.cached_materialized_read_cache_epoch();

    client
        .publish_manual_rescan_signal_with_failure()
        .await
        .expect("publish manual rescan signal through source worker");
    assert!(
        client.cached_materialized_read_cache_epoch() > cache_epoch_before,
        "manual rescan publication must invalidate same-authority materialized read caches before the worker starts"
    );
    assert!(
        client
            .existing_client()
            .await
            .expect("check existing client after publish")
            .is_some(),
        "worker-backed manual rescan signal publication must be owned by the started source worker, not by an app-local signal-cell write"
    );

    let (_next, updates) = signal
        .watch_since(offset)
        .await
        .expect("watch signal updates");
    assert_eq!(
        updates.len(),
        1,
        "manual rescan signal should emit exactly one update"
    );
    assert_eq!(updates[0].requested_by, "node-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_manual_rescan_replays_baseline_batches_for_fresh_sink() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: {
            let mut root1 = RootSpec::new("nfs1", &nfs1);
            root1.watch = true;
            root1.scan = true;
            let mut root2 = RootSpec::new("nfs2", &nfs2);
            root2.watch = true;
            root2.scan = true;
            vec![root1, root2]
        },
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg.clone(),
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
        .expect("init sink");
    let query_dir = b"/force-find-stress";
    let query_root = b"/force-find-stress";

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => sink.send(&batch).await.expect("apply initial batch"),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        let nfs1_ready = selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root);
        let nfs2_ready = selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(selected_group_tree_contains_path(
        &sink, "nfs1", query_dir, query_root
    ));
    assert!(selected_group_tree_contains_path(
        &sink, "nfs2", query_dir, query_root
    ));

    let fresh_sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
        .expect("init fresh sink");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal_with_failure(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let replay_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < replay_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => fresh_sink.send(&batch).await.expect("apply replay batch"),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("baseline replay recv failed: {err}"),
        }
        let nfs1_ready =
            selected_group_tree_contains_path(&fresh_sink, "nfs1", query_dir, query_root);
        let nfs2_ready =
            selected_group_tree_contains_path(&fresh_sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        selected_group_tree_contains_path(&fresh_sink, "nfs1", query_dir, query_root),
        "manual rescan should replay baseline nfs1 entries for a fresh sink"
    );
    assert!(
        selected_group_tree_contains_path(&fresh_sink, "nfs2", query_dir, query_root),
        "manual rescan should replay baseline nfs2 entries for a fresh sink"
    );

    client.close().await.expect("close source worker");
    sink.close().await.expect("close sink");
    fresh_sink.close().await.expect("close fresh sink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_emits_initial_and_manual_rescan_batches_for_each_primary_root() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: {
            let mut root1 = RootSpec::new("nfs1", &nfs1);
            root1.watch = true;
            root1.scan = true;
            let mut root2 = RootSpec::new("nfs2", &nfs2);
            root2.watch = true;
            root2.scan = true;
            vec![root1, root2]
        },
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial batch recv failed: {err}"),
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        if complete {
            break;
        }
    }

    let nfs1_initial_data = data_counts.get("node-a::nfs1").copied().unwrap_or(0);
    let nfs2_initial_data = data_counts.get("node-a::nfs2").copied().unwrap_or(0);
    assert_eq!(control_counts.get("node-a::nfs1").copied(), Some(2));
    assert_eq!(control_counts.get("node-a::nfs2").copied(), Some(2));
    assert!(nfs1_initial_data > 0, "nfs1 should emit initial data");
    assert!(nfs2_initial_data > 0, "nfs2 should emit initial data");

    std::fs::write(
        nfs1.join("force-find-stress").join("after-rescan.txt"),
        b"aa",
    )
    .expect("append nfs1 file");
    std::fs::write(
        nfs2.join("force-find-stress").join("after-rescan.txt"),
        b"bb",
    )
    .expect("append nfs2 file");
    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal_with_failure(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan batch recv failed: {err}"),
        }
        let complete = control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4
            && control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4
            && data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data
            && data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data;
        if complete {
            break;
        }
    }

    assert!(
        control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4,
        "nfs1 should emit a second epoch after manual rescan: {control_counts:?}"
    );
    assert!(
        control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4,
        "nfs2 should emit a second epoch after manual rescan: {control_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data,
        "nfs1 should emit additional data after manual rescan: {data_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data,
        "nfs2 should emit additional data after manual rescan: {data_counts:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_updates_last_runner_snapshot_and_observability() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let params = selected_group_force_find_request("nfs1");
    let first = client
        .force_find_with_failure(params.clone())
        .await
        .expect("first force-find over worker");
    assert!(
        !first.is_empty(),
        "first worker force-find should return at least one response event"
    );
    let first_runner = client
        .last_force_find_runner_by_group_snapshot_with_failure()
        .await
        .expect("first last-runner snapshot");
    assert_eq!(
        first_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1")
    );

    let second = client
        .force_find_with_failure(params.clone())
        .await
        .expect("second force-find over worker");
    assert!(
        !second.is_empty(),
        "second worker force-find should return at least one response event"
    );
    let second_runner = client
        .last_force_find_runner_by_group_snapshot_with_failure()
        .await
        .expect("second last-runner snapshot");
    assert_eq!(
        second_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp2")
    );

    let third = client
        .force_find_with_failure(params)
        .await
        .expect("third force-find over worker");
    assert!(
        !third.is_empty(),
        "third worker force-find should return at least one response event"
    );
    let third_runner = client
        .last_force_find_runner_by_group_snapshot_with_failure()
        .await
        .expect("third last-runner snapshot");
    assert_eq!(
        third_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1")
    );

    let observability = SourceFacade::Worker(client.clone().into())
        .observability_snapshot_with_failure()
        .await
        .expect("worker observability snapshot after force-find");
    assert_eq!(
        observability
            .last_force_find_runner_by_group
            .get("nfs1")
            .map(String::as_str),
        Some("node-a::exp1")
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_preserves_last_runner_in_degraded_observability_after_worker_shutdown()
 {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-cache-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let events = client
        .force_find_with_failure(selected_group_force_find_request("nfs1"))
        .await
        .expect("force-find over worker");
    assert!(
        !events.is_empty(),
        "worker force-find should return at least one response event"
    );

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker after force-find");

    let observability = SourceFacade::Worker(client.clone().into())
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        observability
            .last_force_find_runner_by_group
            .get("nfs1")
            .map(String::as_str),
        Some("node-a::exp1"),
        "degraded observability should preserve the last force-find runner captured before the worker became unavailable: {observability:?}"
    );
    assert_eq!(
        observability.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "worker shutdown after force-find should serve a degraded cached observability snapshot: {observability:?}"
    );

    let _ = client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_retries_unexpected_correlation_protocol_violation() {
    struct SourceWorkerForceFindErrorQueueHookReset;

    impl Drop for SourceWorkerForceFindErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_force_find_error_queue_hook();
        }
    }

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir =
        std::env::temp_dir().join(format!("fs-meta-worker-force-find-correlation-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerForceFindErrorQueueHookReset;
    install_source_worker_force_find_error_queue_hook(SourceWorkerForceFindErrorQueueHook {
        errs: std::collections::VecDeque::from([CnxError::PeerError(
            "bound route protocol violation: unexpected correlation_id in reply batch (78)"
                .to_string(),
        )]),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let events = tokio::time::timeout(
            Duration::from_secs(4),
            client.force_find_with_failure(selected_group_force_find_request("nfs1")),
        )
        .await
        .expect(
            "force_find unexpected-correlation recovery should settle promptly instead of hanging on the stale worker instance",
        )
        .expect(
            "force_find should recover when a live shared source worker hits a transient unexpected-correlation protocol violation",
        );
    assert!(
        !events.is_empty(),
        "force_find should still return response events after unexpected-correlation recovery"
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_eq!(
        next_instance_id, previous_instance_id,
        "force_find unexpected-correlation recovery should retry on the live shared source worker instance instead of forcing a rebind"
    );

    let runner = client
        .last_force_find_runner_by_group_snapshot_with_failure()
        .await
        .expect("last runner snapshot after unexpected-correlation recovery");
    assert_eq!(
        runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1"),
        "force_find should still update last-runner state after recovering from unexpected correlation"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_roots_snapshot_uses_cached_roots_when_worker_resets_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("initial logical roots snapshot");
    assert_eq!(
        initial_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "initial worker snapshot should expose the configured root"
    );

    client.close().await.expect("close source worker");

    let cached_roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("cached logical roots snapshot after worker reset");
    assert_eq!(
        cached_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "logical_roots_snapshot should fall back to cached roots during restart handoff"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_object_grants_snapshot_uses_cached_grants_when_worker_resets_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_grants = client
        .host_object_grants_snapshot_with_failure()
        .await
        .expect("initial host-object grants snapshot");
    assert_eq!(
        initial_grants
            .iter()
            .map(|grant| grant.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-d::nfs1"],
        "initial worker snapshot should expose the configured grant"
    );

    client.close().await.expect("close source worker");

    let cached_grants = client
        .host_object_grants_snapshot_with_failure()
        .await
        .expect("cached host-object grants snapshot after worker reset");
    assert_eq!(
        cached_grants
            .iter()
            .map(|grant| grant.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-d::nfs1"],
        "host_object_grants_snapshot should fall back to cached grants during restart handoff"
    );
}

#[test]
fn cached_host_object_grants_snapshot_is_used_for_stale_drained_pid_errors() {
    let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
    assert!(
        can_use_cached_grant_derived_snapshot(&err),
        "host_object_grants_snapshot should fall back to cached grants when a stale drained/fenced worker pid rejects new grant attachments"
    );
}

#[test]
fn cached_logical_roots_snapshot_is_used_for_stale_drained_pid_errors() {
    let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
    assert!(
        can_use_cached_grant_derived_snapshot(&err),
        "logical_roots_snapshot should fall back to cached roots when a stale drained/fenced worker pid rejects new grant attachments"
    );
}

#[test]
fn manual_rescan_logical_roots_snapshot_uses_cache_for_stale_worker_handoff() {
    let err = CnxError::TransportClosed(
        "stale shared source worker client detached during logical_roots_snapshot".to_string(),
    );
    assert!(
        can_use_cached_manual_rescan_logical_roots_snapshot(&err),
        "manual rescan target selection should use the cached current-roots view when a live roots snapshot loses an in-flight stale worker client handoff"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerStatusErrorHookReset;
    install_source_worker_status_error_hook(SourceWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = client.status_snapshot_with_failure().await.expect(
        "status_snapshot should retry a stale drained/fenced pid error and reach the live worker",
    );

    assert_eq!(
        snapshot.logical_roots.len(),
        1,
        "status snapshot should come back from the rebound live worker with the configured root"
    );
    assert!(
        snapshot.degraded_roots.is_empty(),
        "fresh live source worker snapshot should still decode after stale-pid retry"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_stale_drained_fenced_pid_errors() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before stale observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a stale drained/fenced pid error and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after stale-pid retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after stale-pid retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after stale-pid retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_bound_route_unexpected_correlation_protocol_violation()
 {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before correlation-mismatch observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::ProtocolViolation(
            "bound route protocol violation: unexpected correlation_id in reply batch (11)"
                .to_string(),
        ),
    });

    let snapshot = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a transient bound-route correlation mismatch and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after correlation-mismatch retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after correlation-mismatch retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after correlation-mismatch retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before bridge-stop observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    let snapshot = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a peer bridge-stopped error after begin and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after bridge-stopped retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after bridge-stopped retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after bridge-stopped retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_replays_retained_source_control_after_worker_rebind_without_new_control_wave()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client
        .reconnect_shared_worker_client_with_failure()
        .await
        .expect("rebind source worker client and arm retained replay");

    let snapshot = tokio::time::timeout(
        Duration::from_secs(7),
        SourceFacade::Worker(client.clone().into())
            .observability_snapshot_nonblocking_for_status_route(),
    )
    .await
    .expect("observability replay repair should stay bounded")
    .0;

    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "source observability should replay retained source control instead of returning not-started after a worker rebind with no new control wave: {snapshot:?}"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "source observability replay repair should restore source scheduled groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "source observability replay repair should restore scan scheduled groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_with_failure_replays_retained_source_control_after_worker_rebind_without_new_control_wave()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client
        .reconnect_shared_worker_client_with_failure()
        .await
        .expect("rebind source worker client and arm retained replay");

    let facade = SourceFacade::Worker(client.clone().into());
    let snapshot = tokio::time::timeout(
        Duration::from_secs(7),
        facade.observability_snapshot_with_failure(),
    )
    .await
    .expect("blocking observability replay repair should stay bounded")
    .expect("blocking observability should repair retained replay");

    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "blocking source observability should replay retained source control instead of reporting a not-started worker after a rebind: {snapshot:?}"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "blocking source observability replay repair should restore source scheduled groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "blocking source observability replay repair should restore scan scheduled groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        !facade.retained_replay_required().await,
        "blocking source observability should clear the retained replay flag after successful repair"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_waits_for_inflight_update_logical_roots_control_op_before_shutting_down_worker() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    // update_logical_roots holds this same client-side control-op guard
    // while the worker RPC is in flight; close must not tear down the worker
    // bridge until that guard has drained.
    let inflight = client.begin_control_op();
    let close_task = tokio::spawn({
        let client = client.clone();
        async move { client.close().await }
    });
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        !close_task.is_finished(),
        "source worker close must wait for in-flight update_logical_roots before tearing down the worker bridge"
    );

    drop(inflight);
    close_task
        .await
        .expect("join close task")
        .expect("close source worker after update");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_replays_start_after_runtime_reverts_to_initialized_without_worker_receipt()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let primed_worker = client.client().await.expect("connect source worker");
    primed_worker
        .control_frames(
            &[bootstrap_envelope(&TestWorkerBootstrapEnvelope::Init {
                node_id: client.node_id.0.clone(),
                payload: SourceWorkerRpc::init_payload(&client.node_id, &client.config)
                    .expect("source worker init payload"),
            })],
            Duration::from_secs(5),
        )
        .await
        .expect("reinitialize worker without replaying Start");

    let not_ready = SourceWorkerClientHandle::call_worker(
        &primed_worker,
        SourceWorkerRequest::LogicalRootsSnapshot,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect_err("raw worker should require Start after direct Init replay");
    assert!(
        matches!(&not_ready, CnxError::PeerError(message) if message == "worker not initialized"),
        "direct bootstrap Init should leave the worker runtime unstarted until Start is replayed: {not_ready:?}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        client.update_logical_roots_with_failure(vec![
            worker_source_root("nfs1", &nfs1),
            worker_source_root("nfs2", &nfs2),
        ]),
    )
    .await
    .expect("update_logical_roots should not hang after raw Init replay")
    .expect("update_logical_roots should replay Start and reach the live worker");

    let roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after bootstrap replay");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-bootstrap-replay update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerUpdateRootsHookReset;
    install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let update_task = tokio::spawn({
        let client = client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            client
                .update_logical_roots_with_failure(vec![
                    worker_source_root("nfs1", &nfs1),
                    worker_source_root("nfs2", &nfs2),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale worker bridge");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(4), update_task)
        .await
        .expect("update_logical_roots should reacquire a live worker client after handoff")
        .expect("join update_logical_roots task")
        .expect("update_logical_roots after worker restart");

    let roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after update");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-handoff update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_initial_source_control_apply_keeps_standard_rpc_budget_for_existing_client() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let signals = source_control_signals_from_envelopes(&vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 1_777_634_405_000,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode source activate"),
    ])
    .expect("decode source activate");

    SourceFacade::Worker(client.clone())
        .record_retained_control_signals(&signals)
        .await;

    let apply = tokio::spawn({
        let client = client.clone();
        async move {
            SourceFacade::Worker(client.into())
                .apply_orchestration_signals_with_total_timeout_with_failure(
                    &signals,
                    Duration::from_secs(7),
                )
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("control-frame rpc did not enter pause hook");
    tokio::time::sleep(Duration::from_millis(5500)).await;
    release.notify_waiters();

    apply
        .await
        .expect("join control apply")
        .expect("fresh initial source control apply must not use the short stale-client budget");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_initial_source_control_apply_does_not_require_live_schedule_refresh_after_ack() {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-bootstrap::nfs1",
            "node-bootstrap",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-bootstrap".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let signals = source_control_signals_from_envelopes(&vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-bootstrap::nfs1",
                "node-bootstrap",
                "10.0.0.41",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{ROUTE_KEY_SOURCE_ROOTS_CONTROL}.stream"),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 1_777_634_405_000,
            expires_at_ms: 1,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["node-bootstrap::nfs1".to_string()],
            }],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{ROUTE_KEY_SOURCE_RESCAN_INTERNAL}.req"),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 1_777_634_405_000,
            expires_at_ms: 1,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["node-bootstrap::nfs1".to_string()],
            }],
        }))
        .expect("encode scan activate"),
    ])
    .expect("decode fresh bootstrap source activation");

    SourceFacade::Worker(client.clone())
        .record_retained_control_signals(&signals)
        .await;

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(50_000)
                .collect(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    SourceFacade::Worker(client.clone())
        .apply_orchestration_signals_with_total_timeout_with_failure(&signals, Duration::from_secs(2))
        .await
        .expect(
            "fresh source bootstrap must complete after control ACK using control-derived schedule evidence",
        );

    let source_groups = client
        .scheduled_source_group_ids()
        .await
        .expect("source schedule cache after fresh bootstrap")
        .expect("source schedule groups should be control-derived");
    let scan_groups = client
        .scheduled_scan_group_ids()
        .await
        .expect("scan schedule cache after fresh bootstrap")
        .expect("scan schedule groups should be control-derived");

    assert_eq!(
        source_groups,
        std::collections::BTreeSet::from(["nfs1".to_string()])
    );
    assert_eq!(
        scan_groups,
        std::collections::BTreeSet::from(["nfs1".to_string()])
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_enforces_total_timeout_when_worker_call_stalls_after_first_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave source activate"),
                    ],
                    Duration::from_millis(150),
                    Duration::from_millis(150),
                )
                .await
        }
    });

    entered.notified().await;
    let err = tokio::time::timeout(Duration::from_secs(1), control_task)
            .await
            .expect("stalled source on_control_frame should resolve within the local total timeout budget")
            .expect("join stalled source on_control_frame task")
            .expect_err("stalled source on_control_frame should fail once its local timeout budget is exhausted");
    assert!(matches!(err, CnxError::Timeout), "err={err:?}");

    release.notify_waiters();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source activate"),
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source-scan activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), control_task)
        .await
        .expect("on_control_frame should reacquire a live source worker client after handoff")
        .expect("join on_control_frame task")
        .expect("source on_control_frame after worker restart");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled_source = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scheduled_scan = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if scheduled_source == expected_groups && scheduled_scan == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after source handoff retry: source={scheduled_source:?} scan={scheduled_scan:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_and_transport_closes_mid_handoff()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_wave = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave source activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge after first wave");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker after first-wave success");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
        .await
        .expect("second source control wave should reacquire a live worker client after reset")
        .expect("join second source control wave")
        .expect("second source control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled source groups should remain converged after second-wave retry: source={source_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn third_on_control_frame_reacquires_worker_client_after_first_and_second_waves_succeeded() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let full_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source-scan activate"),
    ];
    let source_only_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source-only activate"),
    ];

    client
        .on_control_frame(full_wave.clone())
        .await
        .expect("first source control wave should succeed");
    client
        .on_control_frame(source_only_wave.clone())
        .await
        .expect("second source-only control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let third_wave = tokio::spawn({
        let client = client.clone();
        async move { client.on_control_frame(source_only_wave).await }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge after two successful waves");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker after two successful waves");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), third_wave)
        .await
        .expect("third source control wave should reacquire a live worker client after reset")
        .expect("join third source control wave")
        .expect("third source control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled source groups should remain converged after third-wave retry: source={source_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_retries_retryable_bridge_errors_after_failed_followup_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let _reset = SourceWorkerStartErrorQueueHookReset;
    install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::PeerError(
            "transport closed: sidecar control bridge stopped".to_string(),
        )]),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker retry start timed out")
            .expect(
                "source worker start should recover from one retryable bridge reset after a follow-up failure",
            );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode retried source activate"),
        ])
        .await
        .expect("source control should still converge after retryable start recovery");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_retries_after_hung_ensure_started_attempt_from_failed_followup_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let _reset = SourceWorkerStartDelayHookReset;
    install_source_worker_start_delay_hook(SourceWorkerStartDelayHook {
        delays: std::collections::VecDeque::from([Duration::from_secs(10)]),
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect(
                "source worker start should recover after one hung ensure_started attempt from a follow-up failure",
            )
            .expect("source worker start should recover after one hung ensure_started attempt");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode retried source activate"),
        ])
        .await
        .expect("source control should still converge after hung start recovery");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_recreates_shared_worker_client_when_retryable_start_resets_never_converge() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let failing_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerStartErrorQueueHookReset;
    install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
        errs: std::collections::VecDeque::new(),
        sticky_worker_instance_id: Some(failing_instance_id),
        sticky_peer_err: Some("transport closed: sidecar control bridge stopped".to_string()),
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start should swap off a permanently resetting shared worker client")
        .expect("source worker start should recover by recreating the shared worker client");

    assert_ne!(
        client.worker_instance_id_for_tests().await,
        failing_instance_id,
        "post-failure worker-ready recovery must recreate the shared worker client instead of retrying forever on the same permanently resetting instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_worker_bindings_share_started_source_worker_client_on_same_node() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let module_path = fs_meta_worker_module_path();
    let module_link = tmp.path().join("worker-module-link");
    std::os::unix::fs::symlink(&module_path, &module_link).expect("create worker module symlink");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let predecessor_socket_dir = worker_socket_tempdir();
    let successor_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            external_source_worker_binding(predecessor_socket_dir.path()),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding_with_module_path(
                successor_socket_dir.path(),
                &module_link,
            ),
            factory,
        )
        .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "predecessor source worker start timed out: predecessor_stderr={} successor_stderr={}",
                    worker_stderr_excerpt(predecessor_socket_dir.path()),
                    worker_stderr_excerpt(successor_socket_dir.path())
                )
            })
            .expect("start predecessor source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("successor existing client after predecessor start")
            .is_some(),
        "same-node successor handle should reuse the already-started source worker client even when socket_dir/module_path differ across runtime instances"
    );

    let roots = successor
        .logical_roots_snapshot_with_failure()
        .await
        .expect("successor logical_roots_snapshot through shared started worker");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "successor handle should see the shared started worker state after predecessor start"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_runtime_factories_do_not_share_started_source_worker_client_on_same_node() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let state_boundary_a = in_memory_state_boundary();
    let state_boundary_b = in_memory_state_boundary();
    let boundary_a = Arc::new(LoopbackWorkerBoundary::default());
    let boundary_b = Arc::new(LoopbackWorkerBoundary::default());
    let worker_socket_dir = worker_socket_tempdir();
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            RuntimeWorkerClientFactory::new(
                boundary_a.clone(),
                boundary_a.clone(),
                state_boundary_a,
            ),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            binding,
            RuntimeWorkerClientFactory::new(
                boundary_b.clone(),
                boundary_b.clone(),
                state_boundary_b,
            ),
        )
        .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
        .await
        .expect("predecessor source worker start timed out")
        .expect("start predecessor source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("successor existing client after predecessor start")
            .is_none(),
        "a successor created through a distinct runtime worker factory must not inherit the predecessor's started source worker client"
    );
    assert!(
        !Arc::ptr_eq(&predecessor._shared, &successor._shared),
        "distinct runtime worker factories must not share one source worker handle state"
    );

    tokio::time::timeout(Duration::from_secs(8), successor.start())
        .await
        .expect("successor source worker start timed out")
        .expect("start successor source worker");

    assert_ne!(
        predecessor.worker_instance_id_for_tests().await,
        successor.worker_instance_id_for_tests().await,
        "distinct runtime worker factories must drive distinct source worker client instances"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recovery_starts_on_same_shared_source_handle_serialize_worker_ready_recovery() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
        .await
        .expect("source worker start timed out")
        .expect("start shared source worker");

    predecessor
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown shared source worker before recovery");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerStartPauseHookReset;
    install_source_worker_start_pause_hook(SourceWorkerStartPauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let predecessor_start = tokio::spawn({
        let predecessor = predecessor.clone();
        async move { predecessor.start().await }
    });

    entered.notified().await;

    let mut successor_start = tokio::spawn({
        let successor = successor.clone();
        async move { successor.start().await }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(600), &mut successor_start)
            .await
            .is_err(),
        "same-node concurrent recovery start must wait while the shared source worker is still mid worker-ready recovery"
    );

    release.notify_waiters();

    predecessor_start
        .await
        .expect("join predecessor recovery start")
        .expect("predecessor recovery start");
    successor_start
        .await
        .expect("join successor recovery start")
        .expect("successor recovery start");

    let roots = successor
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after serialized recovery start");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "shared source worker should still converge after serialized same-node recovery starts"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_waits_for_shared_control_frame_handoff_before_dispatching_to_worker()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let control_client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct control source worker client"),
    );
    let update_client = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct update source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), control_client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let control_entered = Arc::new(Notify::new());
    let control_release = Arc::new(Notify::new());
    let _control_reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: control_entered.clone(),
        release: control_release.clone(),
    });

    let update_entered = Arc::new(Notify::new());
    let update_release = Arc::new(Notify::new());
    let _update_reset = SourceWorkerUpdateRootsHookReset;
    install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
        entered: update_entered.clone(),
        release: update_release.clone(),
    });

    let control_task = tokio::spawn({
        let control_client = control_client.clone();
        async move {
            control_client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source activate"),
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source-scan activate"),
                ])
                .await
        }
    });

    control_entered.notified().await;

    let update_task = tokio::spawn({
        let update_client = update_client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            update_client
                .update_logical_roots_with_failure(vec![
                    worker_source_root("nfs1", &nfs1),
                    worker_source_root("nfs2", &nfs2),
                ])
                .await
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(600), update_entered.notified())
            .await
            .is_err(),
        "shared source update_logical_roots must not start dispatch while another handle is still mid-control-frame handoff on the same worker"
    );

    control_release.notify_waiters();
    control_task
        .await
        .expect("join control task")
        .expect("apply shared source control frame");

    update_entered.notified().await;
    update_release.notify_waiters();
    update_task
        .await
        .expect("join update task")
        .expect("update logical roots after shared control handoff");

    let roots = update_client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after shared handoff");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-handoff update"
    );

    update_client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_keeps_shared_source_worker_client_alive_when_another_handle_still_exists() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), successor.start())
        .await
        .expect("source worker start timed out")
        .expect("start shared source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("existing client before predecessor close")
            .is_some(),
        "shared source worker must have a live client before predecessor close"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker handle");

    assert!(
        successor
            .existing_client()
            .await
            .expect("existing client after predecessor close")
            .is_some(),
        "closing one source handle must not tear down the shared worker client while a successor handle still exists"
    );

    let roots = successor
        .logical_roots_snapshot_with_failure()
        .await
        .expect("successor source logical_roots_snapshot after predecessor close");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "shared source worker should stay usable for the successor after predecessor close"
    );

    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerUpdateRootsErrorHookReset;
    install_source_worker_update_roots_error_hook(SourceWorkerUpdateRootsErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .update_logical_roots_with_failure(vec![
                worker_source_root("nfs1", &nfs1),
                worker_source_root("nfs2", &nfs2),
            ])
            .await
            .expect(
                "update_logical_roots should retry a stale drained/fenced pid error and reach the live worker",
            );

    let roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after stale-pid retry");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-retry update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_contraction_drops_removed_roots_from_snapshot_immediately() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_source_root("nfs1", &nfs1),
            worker_source_root("nfs2", &nfs2),
            worker_source_root("nfs3", &nfs3),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            worker_source_export("node-d::nfs3", "node-d", "10.0.0.43", nfs3.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("initial logical roots snapshot");
    assert_eq!(
        initial_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2", "nfs3"],
        "fixture must start with all three logical roots present"
    );

    client
        .update_logical_roots_with_failure(vec![worker_source_root("nfs1", &nfs1)])
        .await
        .expect("shrink logical roots to single root");

    let roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after contraction");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "source worker logical_roots_snapshot must drop removed roots immediately after contraction"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect(
                "on_control_frame should retry a stale drained/fenced pid error and reach the live worker",
            );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            && scan_groups
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should reflect the post-retry activation: source={:?} scan={:?}",
            source_groups,
            scan_groups
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_on_current_instance()
 {
    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two stale drained/fenced recv recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after stale drained/fenced recv recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_source_roots_deactivate_fails_fast_after_repeated_bridge_reset_errors()
 {
    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-cleanup-failfast".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial source control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(
            SourceWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let cleanup_envelopes = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source roots deactivate"),
    ];
    assert!(
        is_drained_retire_cleanup_deactivate_batch(&cleanup_envelopes),
        "test precondition: source cleanup batch must be recognized as restart_deferred_retire_pending"
    );

    let started = std::time::Instant::now();
    let err = client
            .on_control_frame_with_timeouts_for_tests(
                cleanup_envelopes,
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "single restart_deferred_retire_pending source roots deactivate should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset cleanup lane should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "bridge-reset cleanup lane should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );
    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "bridge-reset cleanup lane must retire the broken shared source worker client before returning so the next generation-two control wave cannot reuse the same current instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_restart_deferred_retire_pending_source_cleanup_wave_through_source_facade_fails_fast_after_repeated_bridge_reset_errors()
 {
    use std::collections::VecDeque;

    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-cleanup-wave-failfast".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial source control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(
        SourceWorkerControlFrameErrorQueueHook {
            errs: VecDeque::from(vec![
                CnxError::PeerError(
                    "transport closed: sidecar control bridge stopped".to_string(),
                ),
                CnxError::PeerError(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
                CnxError::PeerError(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                        .to_string(),
                ),
            ]),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        },
    );

    let cleanup_signals = source_control_signals_from_envelopes(&vec![
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source roots deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source rescan-control deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source rescan deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source scan deactivate"),
    ])
    .expect("decode cleanup signals");

    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(2),
        SourceFacade::Worker(client.clone().into())
            .apply_orchestration_signals_with_total_timeout_with_failure(
                &cleanup_signals,
                SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
            ),
    )
    .await
    .expect("cleanup wave should settle within the short source-owned timeout budget")
    .expect_err(
        "restart_deferred_retire_pending cleanup wave should fail fast once repeated bridge resets prove this lane is not making progress",
    );

    assert!(
        !matches!(err.as_error(), CnxError::Timeout),
        "restart_deferred_retire_pending cleanup wave should return the bridge reset error instead of collapsing to timeout through the source-owned budget: err={err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "restart_deferred_retire_pending cleanup wave should fail fast instead of spending the full source-owned timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_invalid_or_revoked_grant_attachment_token_error_on_current_instance()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
        err: CnxError::AccessDenied("invalid or revoked grant attachment token".to_string()),
    });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on an invalid or revoked grant attachment token",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client on invalid token",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two invalid-token recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after invalid-token recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_channel_closed_on_current_instance()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
        err: CnxError::ChannelClosed,
    });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on channel-closed current-instance errors",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after channel-closed current-instance errors",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two channel-closed recovery must reacquire a fresh shared source worker client instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_channel_closed_recovery_rearms_scoped_manual_rescan_endpoint() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let node_id = NodeId("node-c-29775497172756365788053506".to_string());
    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            node_id.clone(),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let local_rescan_route = source_rescan_request_route_for(&node_id.0).0;
    let source_wave = |generation| {
        vec![
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: generation,
                grants: vec![worker_route_export(
                    "node-c::nfs1",
                    "node-c",
                    "10.0.0.23",
                    &nfs1,
                )],
            })
            .expect("encode runtime host grants changed"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-c::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-c::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: local_rescan_route.clone(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-c::nfs1"])],
            }))
            .expect("encode scoped source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: local_rescan_route.clone(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-c::nfs1"])],
            }))
            .expect("encode scoped scan rescan activate"),
        ]
    };

    client
        .on_control_frame(source_wave(1))
        .await
        .expect("initial scoped source-rescan control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
        err: CnxError::ChannelClosed,
    });

    tokio::time::timeout(
        Duration::from_secs(4),
        client.on_control_frame(source_wave(2)),
    )
    .await
    .expect("generation-two scoped rescan replay should reacquire a fresh worker client")
    .expect("generation-two scoped rescan replay should recover after channel-closed reset");

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two channel-closed recovery must reacquire a fresh shared source worker client instance"
    );

    let adapter = crate::runtime::seam::exchange_host_adapter(
        boundary.clone(),
        NodeId("api-node".to_string()),
        source_rescan_route_bindings_for(&node_id.0),
    );
    assert!(
        !boundary
            .closed_channels_snapshot()
            .iter()
            .any(|channel| channel == &local_rescan_route
                || channel == &format!("{local_rescan_route}:reply")),
        "generation-two recovery must not leave the scoped manual-rescan request/reply lanes closed before the first API rescan: closed={:?} history={:?}",
        boundary.closed_channels_snapshot(),
        boundary.close_history_snapshot()
    );
    let events = capanix_host_adapter_fs::HostAdapter::call_collect(
        &adapter,
        ROUTE_TOKEN_FS_META_INTERNAL,
        METHOD_SOURCE_RESCAN,
        bytes::Bytes::from_static(b"manual-rescan"),
        Duration::from_secs(2),
        Duration::from_millis(50),
    )
    .await
    .expect("scoped manual-rescan endpoint must be receive-armed after channel-closed replay");

    assert!(
        events.iter().any(|event| {
            event.metadata().origin_id == node_id && event.payload_bytes() == b"accepted"
        }),
        "generation-two replay must rearm the source-owned scoped manual-rescan endpoint before the first management rescan: {events:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client when post-ack schedule refresh stalls on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover when post-ack schedule refresh forces a fresh shared worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after post-ack stale drained/fenced refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_only_followup_replays_retained_fs_source_selected_scopes_after_generation_two_reconnect()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053507".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    client
            .on_control_frame(source_wave(2))
            .await
            .expect("generation-two fs_source-selected control wave should succeed before the tick-only followup");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
        .on_control_frame(vec![
            encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                generation: 3,
                at_ms: 1,
            })
            .expect("encode source-roots tick"),
        ])
        .await
        .expect(
            "tick-only followup should recover after reconnecting the shared source worker client",
        );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "tick-only followup should reconnect to a fresh shared source worker client instance"
    );

    let raw = client
        .client()
        .await
        .expect("connect raw source worker client");
    let snapshot = match SourceWorkerClientHandle::call_worker(
        &raw,
        SourceWorkerRequest::ObservabilitySnapshot,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect("raw observability snapshot after tick-only reconnect")
    {
        SourceWorkerResponse::ObservabilitySnapshot(snapshot) => snapshot,
        other => {
            panic!("unexpected source worker response for raw observability snapshot: {other:?}")
        }
    };

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "tick-only followup after reconnect must replay retained source activate scopes into the fresh worker instead of leaving runtime-managed source groups empty"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "tick-only followup after reconnect must replay retained scan activate scopes into the fresh worker instead of leaving runtime-managed scan groups empty"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_only_followup_skips_external_worker_ipc_when_retained_active_routes_already_match_current_generation()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053507".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    client
        .on_control_frame(source_wave(2))
        .await
        .expect("generation-two fs_source-selected control wave should succeed before the tick-only followup");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_unit_tick(&RuntimeUnitTick {
                            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            generation: 2,
                            at_ms: 1,
                        })
                        .expect("encode source-roots tick"),
                    ],
                    Duration::from_millis(150),
                    Duration::from_millis(150),
                )
                .await
        }
    });

    let entered_result = tokio::time::timeout(Duration::from_millis(250), entered.notified()).await;
    if entered_result.is_ok() {
        release.notify_waiters();
        panic!(
            "tick-only followup should not enter external worker on_control_frame RPC when retained active routes already match the current generation"
        );
    }

    tokio::time::timeout(Duration::from_secs(1), control_task)
        .await
        .expect("tick-only followup task should finish without waiting on external worker IPC")
        .expect("join tick-only followup task")
        .expect(
            "tick-only followup should complete from retained state without reopening worker IPC",
        );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_only_followup_replays_retained_activates_before_forwarding_same_generation_tick_to_fresh_worker()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053509".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    client
        .on_control_frame(source_wave(2))
        .await
        .expect("generation-two fs_source-selected control wave should succeed before the tick-only followup");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    client
        .reconnect_shared_worker_client_with_failure()
        .await
        .expect("force shared worker replacement before same-generation tick");
    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "forced shared worker replacement should rotate the source worker instance before the tick-only followup"
    );

    client
        .on_control_frame(vec![
            encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                generation: 2,
                at_ms: 1,
            })
            .expect("encode source-roots tick"),
        ])
        .await
        .expect(
            "same-generation tick-only followup should recover retained runtime-managed scheduling after the shared worker rotates",
        );

    let raw = client
        .client()
        .await
        .expect("connect raw source worker client");
    let snapshot = match SourceWorkerClientHandle::call_worker(
        &raw,
        SourceWorkerRequest::ObservabilitySnapshot,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect("raw observability snapshot after same-generation tick-only reconnect")
    {
        SourceWorkerResponse::ObservabilitySnapshot(snapshot) => snapshot,
        other => {
            panic!("unexpected source worker response for raw observability snapshot: {other:?}")
        }
    };

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "same-generation tick-only followup after forced worker rotation must replay retained source activate scopes before forwarding the tick"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "same-generation tick-only followup after forced worker rotation must replay retained scan activate scopes before forwarding the tick"
    );
    assert_eq!(
        snapshot.last_control_frame_signals_by_node,
        std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec![
                "host_grant_change".to_string(),
                "activate unit=runtime.exec.scan route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>nfs1\"]".to_string(),
                "activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=2 scopes=[\"nfs1=>nfs1\"]".to_string(),
                "activate unit=runtime.exec.source route=source-manual-rescan-control:v1.stream generation=2 scopes=[\"nfs1=>nfs1\"]".to_string(),
                "activate unit=runtime.exec.source route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>nfs1\"]".to_string(),
            ],
        )]),
        "same-generation tick-only followup after forced worker rotation must preserve the replayed retained control summary instead of collapsing observability to the forwarded tick: {:?}",
        snapshot.last_control_frame_signals_by_node
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-e-29778840745788278147383297".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-e::nfs1", "node-e", "10.0.0.51", &nfs1),
                worker_route_export("node-e::nfs2", "node-e", "10.0.0.52", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after real source route replay: source={source_groups:?} scan={scan_groups:?} grants_version={} grants={:?} logical_roots={:?} stderr={}",
                client
                    .host_object_grants_version_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                client
                    .logical_roots_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two real source route wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29780730718931112664498177".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh stalls on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after real-source post-ack stale drained/fenced refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_timeout_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::collections::VecDeque::from([CnxError::Timeout]),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh times out",
            )
            .expect(
                "generation-two real source route replay should recover after forcing a fresh shared worker client on post-ack refresh timeout",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack timeout recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_four_envelope_wave_early_eof_after_begin_does_not_collapse_to_timeout()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29798506721722971770060801".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave_envelopes = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let source_wave = |generation| {
        source_control_signals_from_envelopes(&source_wave_envelopes(generation))
            .expect("decode source wave signals")
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave_envelopes(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
        errs: std::iter::repeat_with(|| {
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            )
        })
        .take(64)
        .collect(),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let err = tokio::time::timeout(
        Duration::from_secs(2),
        SourceFacade::Worker(client.clone().into())
            .apply_orchestration_signals_with_total_timeout_with_failure(
                &source_wave(2),
                SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
            ),
    )
    .await
    .expect("generation-two real source route replay should settle within the short source-owned timeout budget")
    .expect_err(
        "generation-two real source route replay should surface a retryable bridge reset instead of succeeding through repeated early-eof bridge failures",
    );
    assert!(
        matches!(
            err.as_error(),
            CnxError::PeerError(message)
                if message.contains("transport closed") && message.contains("early eof")
        ),
        "generation-two real source route replay should fail fast with the retryable bridge reset instead of collapsing to timeout: err={err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_four_envelope_wave_timeout_like_reset_fails_fast_before_local_budget_exhaustion()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29798803859008206969765889".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
        errs: std::iter::repeat_with(|| CnxError::Timeout)
            .take(50000)
            .collect(),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let queued_len = {
        let guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.as_ref().map(|hook| hook.errs.len())
    };
    assert_eq!(
        queued_len,
        Some(50000),
        "timeout-like reset hook must be installed before generation-two replay"
    );

    let started = std::time::Instant::now();
    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(1200),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route replay should fail once timeout-like bridge resets keep repeating",
        );

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "generation-two real source route replay should fail fast instead of exhausting the local caller budget after timeout-like resets: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_does_not_fail_closed_from_primed_cache_after_repeated_timeouts_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-repeated-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should settle after repeated post-ack refresh timeouts",
            )
            .expect(
                "generation-two real source route replay should recover instead of failing closed from a primed schedule cache after repeated post-ack refresh timeouts",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "repeated post-ack refresh timeouts must force a fresh shared source worker client instead of succeeding from a primed schedule cache",
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after repeated-timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_exhaustion_does_not_succeed_from_primed_cache()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-timeout-refresh-exhaustion".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(50_000)
                .collect(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route should not succeed from a primed schedule cache when post-ack scheduled-groups refresh exhausts",
        );

    let CnxError::Internal(message) = err else {
        panic!(
            "post-ack schedule refresh exhaustion should fail closed with a sharp internal error instead of a generic retryable timeout: {err:?}"
        );
    };
    assert!(
        message.contains("post-ack scheduled-groups refresh exhausted"),
        "post-ack schedule refresh exhaustion should surface a sharp internal cause, got: {message}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_schedule_rpc_timeout_fail_closes_with_convergence_timeout()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-schedule-rpc-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::PeerError("operation timed out".to_string()))
                .take(50_000)
                .collect(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route should fail closed when post-ack scheduled-groups refresh schedule RPCs keep returning operation timed out",
        );

    let CnxError::Internal(message) = err else {
        panic!(
            "schedule RPC timeouts during post-ack refresh should fail closed with a convergence timeout instead of surfacing a raw transport-shaped error: {err:?}"
        );
    };
    assert!(
        message.contains(
            "post-ack scheduled-groups refresh exhausted before scheduled groups converged (timeout)"
        ),
        "schedule RPC timeouts should classify as the timeout-shaped convergence failure, got: {message}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_replay_required_post_ack_schedule_refresh_reacquires_live_worker_after_repeated_timeouts()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct SourceWorkerScheduledGroupsRefreshDelayHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    impl Drop for SourceWorkerScheduledGroupsRefreshDelayHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_delay_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-replay-required-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    client
        .reconnect_shared_worker_client_with_failure()
        .await
        .expect("force replay-required source worker rebind");
    let replay_required_instance_id = client.worker_instance_id_for_tests().await;

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    let _delay_reset = SourceWorkerScheduledGroupsRefreshDelayHookReset;
    install_source_worker_scheduled_groups_refresh_delay_hook(
        SourceWorkerScheduledGroupsRefreshDelayHook {
            delays: std::collections::VecDeque::from([Duration::from_millis(2100)]),
            sticky_worker_instance_id: Some(replay_required_instance_id),
        },
    );
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(replay_required_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should settle even when replay-required refresh keeps timing out",
            )
            .expect(
                "replay-required post-ack scheduled-groups refresh must reacquire a fresh live worker instead of exhausting after repeated timeouts",
            );
    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, replay_required_instance_id,
        "replay-required repeated timeout refresh must rotate to a fresh live worker instead of remaining stuck on the stalled replay-required instance",
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should converge after replay-required repeated-timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_missing_route_state_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d-29780951151719537312792577".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-d::nfs1", "node-d", "10.0.0.41", &nfs1),
                worker_route_export("node-d::nfs2", "node-d", "10.0.0.42", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "source worker unavailable: missing route state for channel_buffer ChannelSlotId(4750)"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh hits missing route state for channel_buffer",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after missing route state",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance after missing route state"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after missing-route-state refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_worker_not_initialized_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29792434514518209017708545".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some("worker not initialized".to_string()),
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh still sees worker not initialized",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after worker-not-initialized",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "worker-not-initialized post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after worker-not-initialized refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_detaches_stale_worker_client() {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29780730718931112664498177".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let stale_worker = client
        .client()
        .await
        .expect("connect stale source worker client");
    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    client.on_control_frame(source_wave(2)).await.expect(
        "generation-two real source route replay should recover after shared-client rebind",
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let stale_err = SourceWorkerClientHandle::call_worker(
            &stale_worker,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        .expect_err(
            "stale pre-rebind source worker client must not keep serving requests after post-ack refresh forced a shared-client rebind",
        );
    assert!(
        matches!(
            stale_err,
            CnxError::TransportClosed(_) | CnxError::AccessDenied(_) | CnxError::PeerError(_)
        ),
        "stale pre-rebind source worker client should fail closed after shared-client rebind, got {stale_err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_aborts_inflight_stale_prerebind_logical_roots_snapshot()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29792561029320261914573329".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _logical_roots_reset = SourceWorkerLogicalRootsSnapshotHookReset;
    install_source_worker_logical_roots_snapshot_hook(SourceWorkerLogicalRootsSnapshotHook {
        roots: None,
        entered: Some(entered.clone()),
        release: Some(release.clone()),
    });

    let stale_snapshot = tokio::spawn({
        let client = client.clone();
        async move { client.logical_roots_snapshot_with_failure().await }
    });

    entered.notified().await;

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    client.on_control_frame(source_wave(2)).await.expect(
        "generation-two real source route replay should recover after shared-client rebind",
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "inflight stale logical_roots_snapshot seam must first prove post-ack scheduled-groups refresh reacquired a fresh shared source worker client instance"
    );

    release.notify_waiters();

    let stale_result = tokio::time::timeout(Duration::from_secs(4), stale_snapshot)
        .await
        .expect("stale logical_roots_snapshot task should settle after rebind release")
        .expect("join stale logical_roots_snapshot task");
    let stale_err = stale_result.expect_err(
            "inflight stale pre-rebind logical_roots_snapshot must fail closed after generation-two shared-client rebind",
        );
    assert!(
        matches!(
            stale_err.as_error(),
            CnxError::TransportClosed(_) | CnxError::AccessDenied(_) | CnxError::PeerError(_)
        ),
        "inflight stale pre-rebind logical_roots_snapshot should fail closed after shared-client rebind, got {stale_err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_root_real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for single-root real source route replay after stale-pid retry: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial single-root control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two single-root real source route wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_source_selected_real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "nfs://server.example/export/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for fs_source-selected single-root replay after stale-pid retry: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two fs_source-selected single-root wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_source_selected_real_source_route_reacquires_worker_client_after_sticky_peer_early_eof_on_generation_two_wave()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "nfs://server.example/export/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-window-join".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on a sticky early-eof bridge error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client from a sticky early-eof bridge error",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two sticky early-eof recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "generation-two sticky early-eof recovery should keep single-root groups converged: source={source_groups:?} scan={scan_groups:?} stderr={}",
            worker_stderr_excerpt(worker_socket_dir.path()),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_group_snapshots_retry_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("initial scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("initial scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups before stale-pid retry test: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _reset = SourceWorkerScheduledGroupsErrorHookReset;
    install_source_worker_scheduled_groups_error_hook(SourceWorkerScheduledGroupsErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let source_groups = client
        .scheduled_source_group_ids()
        .await
        .expect("scheduled_source_group_ids should retry stale drained/fenced pid errors");
    let scan_groups = client
        .scheduled_scan_group_ids()
        .await
        .expect("scheduled_scan_group_ids should still reach the live worker after retry");

    assert_eq!(source_groups, Some(expected_groups.clone()));
    assert_eq!(scan_groups, Some(expected_groups));

    client.close().await.expect("close source worker");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_find_via_node_shares_runner_state_with_existing_target_worker_handle() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-share-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let target_client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct target source worker client"),
    );
    let routing_client = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct routing source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), target_client.start())
        .await
        .expect("target source worker start timed out")
        .expect("start target source worker");

    let routed = SourceFacade::worker(routing_client);
    let params = selected_group_force_find_request("nfs1");
    let first = routed
        .force_find_via_node_with_failure(&NodeId("node-a".to_string()), &params)
        .await
        .expect("routed force-find via node");
    assert!(
        !first.is_empty(),
        "routed force-find via target node should return at least one response event"
    );

    let shared_runner = target_client
        .last_force_find_runner_by_group_snapshot_with_failure()
        .await
        .expect("target handle last-runner snapshot");
    assert_eq!(
        shared_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1"),
        "force_find_via_node should update runner state visible through the already-started target worker handle"
    );

    target_client
        .close()
        .await
        .expect("close target source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rearm_source_rescan_endpoint_via_node_reaches_existing_target_worker_handle() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-rescan-rearm-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![worker_source_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            root_dir.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let target_node = NodeId("node-a".to_string());
    let target_client = Arc::new(
        SourceWorkerClientHandle::new(
            target_node.clone(),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct target source worker client"),
    );
    let routing_client = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct routing source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), target_client.start())
        .await
        .expect("target source worker start timed out")
        .expect("start target source worker");

    let routed = SourceFacade::worker(routing_client);
    routed
        .rearm_source_rescan_request_endpoints_via_node_with_failure(&target_node, boundary.clone())
        .await
        .expect("routed rearm via target node");

    let adapter = crate::runtime::seam::exchange_host_adapter(
        boundary.clone(),
        NodeId("api-node".to_string()),
        source_rescan_route_bindings_for(&target_node.0),
    );
    let events = capanix_host_adapter_fs::HostAdapter::call_collect(
        &adapter,
        ROUTE_TOKEN_FS_META_INTERNAL,
        METHOD_SOURCE_RESCAN,
        bytes::Bytes::from_static(b"manual-rescan"),
        Duration::from_secs(2),
        Duration::from_millis(50),
    )
    .await
    .expect("scoped manual-rescan endpoint must be receive-armed by routed target-node rearm");

    assert!(
        events.iter().any(|event| {
            event.metadata().origin_id == target_node && event.payload_bytes() == b"accepted"
        }),
        "routed target-node rearm must make the selected source-worker scoped manual-rescan route reachable: {events:?}"
    );

    target_client
        .close()
        .await
        .expect("close target source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_stream_batches_reach_sink_worker_for_each_scheduled_primary_root() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let source_factory = RuntimeWorkerClientFactory::new(
        boundary.clone(),
        boundary.clone(),
        in_memory_state_boundary(),
    );
    let sink_factory = RuntimeWorkerClientFactory::new(
        boundary.clone(),
        boundary.clone(),
        in_memory_state_boundary(),
    );
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_socket_root.path().join("source");
    let sink_socket_dir = worker_socket_root.path().join("sink");
    std::fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    std::fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
    let source = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg.clone(),
        external_source_worker_binding(&source_socket_dir),
        source_factory,
    )
    .expect("construct source worker client");
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(&sink_socket_dir),
        sink_factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), source.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    source
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("activate source worker");
    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink worker");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = sink
            .scheduled_group_ids()
            .await
            .expect("sink status")
            .unwrap_or_default()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for source/sink schedule convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    let selected_file = b"/force-find-stress/seed.txt";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                let mut control = 0usize;
                for event in &batch {
                    let origin = event.metadata().origin_id.0.clone();
                    if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                        *control_counts.entry(origin).or_insert(0) += 1;
                        control += 1;
                    } else {
                        *data_counts.entry(origin).or_insert(0) += 1;
                    }
                }
                if control < batch.len() {
                    sink.send(batch)
                        .await
                        .expect("forward source batch to sink");
                }
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("stream batch recv failed: {err}"),
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied().unwrap_or(0) > 0
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_file,
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2"),
            selected_file,
        )
        .expect("decode nfs2")
        .is_some();
        if complete && nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        control_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "nfs1 should publish at least one control event on the external source stream: {control_counts:?}"
    );
    assert!(
        control_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "nfs2 should publish at least one control event on the external source stream: {control_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "nfs1 should produce at least one data batch on the external source stream: {data_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "nfs2 should produce at least one data batch on the external source stream: {data_counts:?}"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1 final"),
            selected_file,
        )
        .expect("decode nfs1 final")
        .is_some(),
        "nfs1 should materialize after forwarding external source batches into sink.send(...)",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2 final"),
            selected_file,
        )
        .expect("decode nfs2 final")
        .is_some(),
        "nfs2 should materialize after forwarding external source batches into sink.send(...)",
    );

    source.close().await.expect("close source worker");
    sink.close().await.expect("close sink worker");
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
async fn external_source_worker_real_nfs_manual_rescan_publishes_newly_seeded_subtree_alongside_baseline_path()
 {
    let preflight = real_nfs_lab::RealNfsPreflight::detect();
    if !preflight.enabled {
        eprintln!(
            "skip real-nfs external source worker publish-path test: {}",
            preflight
                .reason
                .unwrap_or_else(|| "real-nfs preflight failed".to_string())
        );
        return;
    }

    let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
    let nfs1 = lab
        .mount_export("node-a", "nfs1")
        .expect("mount node-a nfs1 export");
    let nfs2 = lab
        .mount_export("node-a", "nfs2")
        .expect("mount node-a nfs2 export");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let baseline_target = b"/data";
    let force_find_target = b"/force-find-stress";
    let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
    let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < baseline_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_path_data_counts(&mut baseline_counts, &batch, baseline_target);
                record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        let baseline_ready = ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0);
        if baseline_ready {
            break;
        }
    }

    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
        "baseline /data should publish for both local primary roots over the external source stream: {baseline_counts:?}"
    );
    assert!(
        force_find_counts.is_empty(),
        "new subtree target should stay absent before it is seeded and manually rescanned: {force_find_counts:?}"
    );

    lab.mkdir("nfs1", "force-find-stress")
        .expect("create nfs1 force-find dir");
    lab.mkdir("nfs2", "force-find-stress")
        .expect("create nfs2 force-find dir");
    lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
        .expect("seed nfs1 force-find subtree");
    lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
        .expect("seed nfs2 force-find subtree");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal_with_failure(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < force_find_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan publish recv failed: {err}"),
        }
        let subtree_ready = ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0);
        if subtree_ready {
            break;
        }
    }

    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
        "newly seeded /force-find-stress subtree should publish for both local primary roots after manual rescan over the external source stream: {force_find_counts:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_nonblocking_observability_can_serve_stale_published_path_counts_during_control_inflight()
 {
    let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
    std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
    std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let baseline_target = b"/data";
    let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < baseline_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }
    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
        "baseline /data should publish for both local primary roots: {baseline_counts:?}"
    );

    let primed_worker = client.client().await.expect("connect source worker");
    let primed = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime cached observability snapshot");
    assert_eq!(
        primed.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "primed snapshot should carry the configured path target"
    );
    assert!(
        primed
            .published_path_origin_counts_by_node
            .get("node-a")
            .is_none_or(Vec::is_empty),
        "before seeding subtree, cached path counts should be empty: {:?}",
        primed.published_path_origin_counts_by_node
    );

    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
        .expect("seed nfs1 subtree");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
        .expect("seed nfs2 subtree");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal_with_failure(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let force_find_target = b"/force-find-stress";
    let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < force_find_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => record_path_data_counts(&mut force_find_counts, &batch, force_find_target),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan publish recv failed: {err}"),
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }
    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
        "manual rescan should publish /force-find-stress for both roots before stale-observability check: {force_find_counts:?}"
    );

    let inflight = client.begin_control_op();
    let stale = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);
    assert!(
        stale
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "control-inflight nonblocking snapshot should use runtime-scope control-cache evidence: {:?}",
        stale.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&stale),
        "control-inflight runtime-scope control cache should not be classified as worker-unavailable evidence"
    );
    assert!(
        stale
            .published_path_origin_counts_by_node
            .get("node-a")
            .is_none_or(Vec::is_empty),
        "cached fallback should still reflect stale pre-rescan path counts: {:?}",
        stale.published_path_origin_counts_by_node
    );

    let live_worker = client.client().await.expect("reconnect source worker");
    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("fetch live post-rescan observability snapshot");
    let live_counts = live
        .published_path_origin_counts_by_node
        .get("node-a")
        .cloned()
        .unwrap_or_default();
    assert!(
        live_counts
            .iter()
            .any(|entry| entry.starts_with("node-a::nfs1=")),
        "live worker snapshot should include nfs1 /force-find-stress path counts: {live_counts:?}"
    );
    assert!(
        live_counts
            .iter()
            .any(|entry| entry.starts_with("node-a::nfs2=")),
        "live worker snapshot should include nfs2 /force-find-stress path counts after rescan: {live_counts:?}"
    );

    client.close().await.expect("close source worker");
    match previous {
        Some(value) => unsafe { std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value) },
        None => unsafe { std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_nonblocking_observability_preserves_scheduled_groups_after_successful_control_before_next_inflight()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let primed_worker = client.client().await.expect("connect source worker");
    let primed = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime cached observability snapshot");
    let expected = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        primed.scheduled_source_groups_by_node, expected,
        "baseline cached observability should expose configured source groups"
    );
    assert_eq!(
        primed.scheduled_scan_groups_by_node, expected,
        "baseline cached observability should expose configured scan groups"
    );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let local_source_groups = client
        .scheduled_source_group_ids()
        .await
        .expect("scheduled source groups")
        .unwrap_or_default();
    let local_scan_groups = client
        .scheduled_scan_group_ids()
        .await
        .expect("scheduled scan groups")
        .unwrap_or_default();
    assert_eq!(
        local_source_groups,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
    );
    assert_eq!(
        local_scan_groups,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
    );

    let inflight = client.begin_control_op();
    let stale = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);
    assert!(
        stale
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "control-inflight nonblocking snapshot should use runtime-scope control-cache evidence: {:?}",
        stale.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&stale),
        "control-inflight runtime-scope control cache should not be classified as worker-unavailable evidence"
    );
    assert_eq!(
        stale.scheduled_source_groups_by_node, expected,
        "cached fallback should preserve latest scheduled source groups after successful control"
    );
    assert_eq!(
        stale.scheduled_scan_groups_by_node, expected,
        "cached fallback should preserve latest scheduled scan groups after successful control"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_observability_reissues_live_rpc_when_recent_cache_is_active_but_debug_empty() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    assert_eq!(
        live.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
    );
    assert!(
        live.last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "live snapshot should carry accepted control summary before cache corruption: {:?}",
        live.last_control_frame_signals_by_node
    );

    let mut incomplete = live.clone();
    incomplete.scheduled_source_groups_by_node.clear();
    incomplete.scheduled_scan_groups_by_node.clear();
    incomplete.last_control_frame_signals_by_node.clear();
    incomplete.published_batches_by_node.clear();
    incomplete.published_events_by_node.clear();
    incomplete.published_control_events_by_node.clear();
    incomplete.published_data_events_by_node.clear();
    incomplete.last_published_at_us_by_node.clear();
    incomplete.last_published_origins_by_node.clear();
    incomplete.published_origin_counts_by_node.clear();
    incomplete.enqueued_path_origin_counts_by_node.clear();
    incomplete.pending_path_origin_counts_by_node.clear();
    incomplete.yielded_path_origin_counts_by_node.clear();
    incomplete.summarized_path_origin_counts_by_node.clear();
    incomplete.published_path_origin_counts_by_node.clear();
    client.update_cached_observability_snapshot(&incomplete);
    clear_cached_worker_observability_debug_maps_for_test(&client);

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    client.with_cache_mut(|cache| {
        cache.last_live_observability_snapshot_at = Some(Instant::now());
    });

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;

    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "active-but-debug-empty recent cache must trigger one live observability RPC before accepting snapshot"
    );
    assert_eq!(
        nonblocking.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "nonblocking observability must reject active-but-debug-empty recent cache for scheduled source groups"
    );
    assert_eq!(
        nonblocking.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "nonblocking observability must reject active-but-debug-empty recent cache for scheduled scan groups"
    );
    assert!(
        nonblocking
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "nonblocking observability must reject active-but-debug-empty recent cache for control summary: {:?}",
        nonblocking.last_control_frame_signals_by_node
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_nonblocking_observability_reads_share_one_live_worker_rpc() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let _delay_hook_reset = SourceWorkerObservabilityDelayHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    let mut incomplete = live.clone();
    incomplete.scheduled_source_groups_by_node.clear();
    incomplete.scheduled_scan_groups_by_node.clear();
    incomplete.last_control_frame_signals_by_node.clear();
    incomplete.published_batches_by_node.clear();
    incomplete.published_events_by_node.clear();
    incomplete.published_control_events_by_node.clear();
    incomplete.published_data_events_by_node.clear();
    incomplete.last_published_at_us_by_node.clear();
    incomplete.last_published_origins_by_node.clear();
    incomplete.published_origin_counts_by_node.clear();
    incomplete.enqueued_path_origin_counts_by_node.clear();
    incomplete.pending_path_origin_counts_by_node.clear();
    incomplete.yielded_path_origin_counts_by_node.clear();
    incomplete.summarized_path_origin_counts_by_node.clear();
    incomplete.published_path_origin_counts_by_node.clear();
    client.update_cached_observability_snapshot(&incomplete);
    clear_cached_worker_observability_debug_maps_for_test(&client);

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    install_source_worker_observability_delay_hook(SourceWorkerObservabilityDelayHook {
        delay: Duration::from_millis(200),
    });
    client.with_cache_mut(|cache| {
        cache.last_live_observability_snapshot_at = Some(Instant::now());
    });

    let first = client.observability_snapshot_nonblocking_for_status_route();
    let second = client.observability_snapshot_nonblocking_for_status_route();
    let ((first_snapshot, _), (second_snapshot, _)) = tokio::join!(first, second);

    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "overlapping status-route source observability reads for the same node should share one live worker RPC"
    );
    assert_eq!(
        first_snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
    );
    assert_eq!(
        second_snapshot
            .scheduled_source_groups_by_node
            .get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_status_observability_refreshes_live_when_runtime_scope_lacks_publication() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    client.update_cached_observability_snapshot(&live);
    client.with_cache_mut(|cache| {
        cache.published_batches_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            0,
        )]));
        cache.published_events_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            0,
        )]));
        cache.published_control_events_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            0,
        )]));
        cache.published_data_events_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            0,
        )]));
        cache.last_published_at_us_by_node = Some(std::collections::BTreeMap::new());
        cache.last_published_origins_by_node = Some(std::collections::BTreeMap::new());
        cache.published_origin_counts_by_node = Some(std::collections::BTreeMap::new());
        cache.last_live_observability_snapshot_at = Some(
            Instant::now()
                - SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL
                - Duration::from_millis(50),
        );
    });

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });

    let (nonblocking, used_cached_fallback) = client
        .observability_snapshot_nonblocking_for_status_route()
        .await;

    assert!(
        !used_cached_fallback,
        "source-status must take a live worker snapshot when runtime-scope owners still lack publication evidence"
    );
    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "runtime-scope owners without publication evidence must not be hidden by stable root-health cache"
    );
    assert_eq!(
        nonblocking.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "live source-status must preserve source runtime-scope evidence"
    );
    assert!(
        !nonblocking
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_CACHE_STATUS_REASON
            }),
        "a successful live observability RPC must not be labelled as worker-status cache evidence: {:?}",
        nonblocking.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[test]
fn stable_status_root_health_cache_can_skip_live_refresh_after_publication() {
    let worker_socket_dir = worker_socket_tempdir();
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        SourceConfig::default(),
        external_source_worker_binding(worker_socket_dir.path()),
        RuntimeWorkerClientFactory::new(
            Arc::new(LoopbackWorkerBoundary::default()),
            Arc::new(LoopbackWorkerBoundary::default()),
            in_memory_state_boundary(),
        ),
    )
    .expect("construct source worker client");

    client.with_cache_mut(|cache| {
        cache.lifecycle_state = Some("ready".to_string());
        cache.logical_roots = Some(vec![
            worker_watch_scan_root("nfs1", Path::new("/mnt/nfs1")),
            worker_watch_scan_root("nfs2", Path::new("/mnt/nfs2")),
        ]);
        cache.status = Some(SourceStatusSnapshot {
            logical_roots: vec![
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs1".to_string(),
                    status: "healthy".to_string(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs2".to_string(),
                    status: "healthy".to_string(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
            ],
            ..SourceStatusSnapshot::default()
        });
        cache.scheduled_source_groups_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]));
        cache.scheduled_scan_groups_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]));
        cache.published_batches_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            1,
        )]));
        cache.published_events_by_node = Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            1,
        )]));
        cache.last_live_observability_snapshot_at = Some(
            Instant::now()
                - SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL
                - Duration::from_millis(50),
        );
    });

    let snapshot = client
        .stable_status_root_health_observability_cache_snapshot()
        .expect("published root-health cache should be eligible for stable status");

    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
    );
    assert!(
        snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_CACHE_STATUS_REASON
            }),
        "stable cached status must keep cache provenance: {:?}",
        snapshot.status.degraded_roots
    );
}

#[test]
fn runtime_scope_control_cache_snapshot_preserves_scheduled_groups_without_live_status() {
    let mut scheduled_source_groups = std::collections::BTreeMap::new();
    scheduled_source_groups.insert("node-b".to_string(), vec!["nfs3".to_string()]);
    let mut scheduled_scan_groups = std::collections::BTreeMap::new();
    scheduled_scan_groups.insert("node-b".to_string(), vec!["nfs3".to_string()]);
    let mut control_signals = std::collections::BTreeMap::new();
    control_signals.insert(
        "node-b".to_string(),
        vec![
            "source:activate:nfs3".to_string(),
            "scan:activate:nfs3".to_string(),
        ],
    );
    let cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(scheduled_source_groups),
        scheduled_scan_groups_by_node: Some(scheduled_scan_groups),
        last_control_frame_signals_by_node: Some(control_signals),
        ..SourceWorkerSnapshotCache::default()
    };

    let snapshot = build_runtime_scope_control_cache_observability_snapshot(&cache)
        .expect("current control cache should publish source runtime-scope evidence");

    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-b"),
        Some(&vec!["nfs3".to_string()]),
        "source-status must expose app-visible source runtime-scope evidence after control ACK even before the first live observability read"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-b"),
        Some(&vec!["nfs3".to_string()]),
        "source-status must expose app-visible scan runtime-scope evidence after control ACK even before the first live observability read"
    );
    assert!(
        snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "control-cache runtime scope must be marked as cache evidence, not live root health: {:?}",
        snapshot.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&snapshot),
        "runtime-scope control cache should not be classified as worker-unavailable evidence"
    );
}

#[test]
fn source_observability_override_preserves_live_scoped_rescan_route_evidence() {
    let node_id = "node-d-29821640722556522502029313";
    let route = format!("{}.req", source_rescan_route_key_for(node_id));
    let override_summary = std::collections::BTreeMap::from([(
        node_id.to_string(),
        vec![format!(
            "activate unit=runtime.exec.source route={route} generation=1777508374245 scopes=[\"nfs2=>nfs2\"]"
        )],
    )]);
    let live_summary = std::collections::BTreeMap::from([(
        node_id.to_string(),
        vec![
            format!(
                "activate unit=runtime.exec.source route={route} generation=1777508542667 scopes=[\"nfs2=>nfs2\"]"
            ),
            format!("ready unit=runtime.exec.source route={route} generation=1777508542667"),
        ],
    )]);

    let merged =
        merge_source_owned_scoped_manual_rescan_route_evidence(override_summary, &live_summary);
    let signals = merged.get(node_id).expect("node control signals");

    assert!(
        signals.iter().any(|signal| signal
            == &format!(
                "activate unit=runtime.exec.source route={route} generation=1777508542667 scopes=[\"nfs2=>nfs2\"]"
            )),
        "worker observability override must preserve the live scoped manual-rescan route activation: {signals:?}"
    );
    assert!(
        signals.iter().any(|signal| signal
            == &format!("ready unit=runtime.exec.source route={route} generation=1777508542667")),
        "worker observability override must preserve the source-owned scoped manual-rescan ready proof: {signals:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_status_observability_reads_live_worker_when_replay_is_current() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client.with_cache_mut(|cache| {
        cache.status = Some(SourceStatusSnapshot::default());
        cache.lifecycle_state = Some("runtime-scope-ready".to_string());
        cache.last_live_observability_snapshot_at = None;
    });
    assert!(
        !matches!(
            client.control_state.lock().await.replay_state(),
            SourceControlReplayState::Required
        ),
        "precondition: retained source replay must already be current"
    );

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });

    let (snapshot, used_cached_fallback) = client
        .observability_snapshot_nonblocking_for_status_route()
        .await;

    assert!(
        !used_cached_fallback,
        "current replay with an available worker must use live source observability instead of the runtime-scope control cache"
    );
    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "source-status should issue one live worker RPC after replay is current"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()])
    );
    assert!(
        !snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "normal source-status live reads must not be labelled as runtime-scope cache fallback: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_status_observability_keeps_runtime_scope_when_live_probe_fails() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _error_hook_reset = SourceWorkerObservabilityErrorHookReset;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client.with_cache_mut(|cache| {
        cache.status = Some(SourceStatusSnapshot::default());
        cache.lifecycle_state = Some("runtime-scope-ready".to_string());
        cache.last_live_observability_snapshot_at = None;
    });

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::Internal("status live probe stalled".to_string()),
    });

    let (snapshot, used_cached_fallback) = client
        .observability_snapshot_nonblocking_for_status_route()
        .await;

    assert!(
        used_cached_fallback,
        "source-status should fall back to current runtime-scope ownership when the bounded live probe fails"
    );
    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "source-status should try exactly one live probe before using runtime-scope ownership evidence"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()])
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()])
    );
    assert!(
        snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "runtime-scope fallback must be labelled as ownership evidence, not worker-unavailable cache: {:?}",
        snapshot.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&snapshot),
        "runtime-scope ownership evidence must not be classified as worker-unavailable"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_status_observability_times_out_live_probe_inside_route_budget() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _delay_hook_reset = SourceWorkerObservabilityDelayHookReset;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client.with_cache_mut(|cache| {
        cache.status = Some(SourceStatusSnapshot::default());
        cache.lifecycle_state = Some("runtime-scope-ready".to_string());
        cache.last_live_observability_snapshot_at = None;
    });
    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    install_source_worker_observability_delay_hook(SourceWorkerObservabilityDelayHook {
        delay: Duration::from_millis(250),
    });

    let (snapshot, used_cached_fallback) = tokio::time::timeout(
        Duration::from_millis(150),
        client.observability_snapshot_nonblocking_for_status_route_with_timeout(Some(
            Duration::from_millis(50),
        )),
    )
    .await
    .expect("source-status live probe must not consume the route reply budget");

    assert!(
        used_cached_fallback,
        "source-status should fall back to runtime-scope ownership when the live probe exceeds its bounded route budget"
    );
    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "source-status should attempt one live probe before using runtime-scope ownership evidence"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()])
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()])
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_rescan_delivery_status_uses_rearm_ack_cache_without_live_probe() {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _delay_hook_reset = SourceWorkerObservabilityDelayHookReset;
    let _count_hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let _rearm_count_hook_reset = SourceWorkerRearmCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let node_id = NodeId("node-b".to_string());
    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        node_id.clone(),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_rescan_route = source_rescan_request_route_for(&node_id.0).0;
    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: source_rescan_route.clone(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source manual-rescan route activate"),
        ])
        .await
        .expect("apply source + scan + manual-rescan control");

    client
        .rearm_source_rescan_request_endpoints_with_failure()
        .await
        .expect("manual-rescan source endpoint rearm");

    let rearm_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_rearm_call_count_hook(SourceWorkerRearmCallCountHook {
        count: rearm_rpc_count.clone(),
    });
    client
        .rearm_source_rescan_request_endpoints_with_failure()
        .await
        .expect("already armed manual-rescan source endpoint rearm");
    assert_eq!(
        rearm_rpc_count.load(Ordering::Relaxed),
        0,
        "manual-rescan rearm must preserve healthy already-armed endpoints from current delivery evidence instead of queueing another worker control RPC"
    );

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    install_source_worker_observability_delay_hook(SourceWorkerObservabilityDelayHook {
        delay: Duration::from_millis(250),
    });

    let (snapshot, used_cached_fallback) = tokio::time::timeout(
        Duration::from_millis(150),
        client.manual_rescan_delivery_observability_snapshot_for_status_route_with_timeout(Some(
            Duration::from_millis(50),
        )),
    )
    .await
    .expect("manual-rescan delivery status must not wait for live source observability");

    assert!(
        used_cached_fallback,
        "manual-rescan delivery status should use the rearm-ack cache as delivery proof"
    );
    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        0,
        "manual-rescan delivery proof must not call live observability or wait on audit"
    );
    assert!(
        snapshot.status.logical_roots.iter().any(|root| {
            root.root_id == "nfs1" && root.status == "manual_rescan_delivery_ready"
        }),
        "rearm-ack snapshot should carry current logical-root delivery readiness: {:?}",
        snapshot.status.logical_roots
    );
    assert!(
        snapshot.status.concrete_roots.iter().any(|root| {
            root.logical_root_id == "nfs1"
                && root.object_ref == "node-b::nfs1"
                && root.is_group_primary
                && root.status == "manual_rescan_delivery_ready"
        }),
        "rearm-ack snapshot should carry local source-primary concrete-root delivery readiness: {:?}",
        snapshot.status.concrete_roots
    );
    assert!(
        snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_MANUAL_RESCAN_DELIVERY_CACHE_REASON
            }),
        "manual-rescan delivery cache provenance must be explicit: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_rescan_scoped_accept_uses_rearm_ack_cache_without_live_probe() {
    let _rearm_count_hook_reset = SourceWorkerRearmCallCountHookReset;
    let _accept_count_hook_reset = SourceWorkerAcceptTargetedDeliveryCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let node_id = NodeId("node-b".to_string());
    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        node_id.clone(),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_rescan_route = source_rescan_request_route_for(&node_id.0).0;
    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: source_rescan_route,
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source manual-rescan route activate"),
        ])
        .await
        .expect("apply source + scan + manual-rescan control");

    client
        .rearm_source_rescan_request_endpoints_with_failure()
        .await
        .expect("manual-rescan source endpoint rearm");

    let accept_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_accept_targeted_delivery_call_count_hook(
        SourceWorkerAcceptTargetedDeliveryCallCountHook {
            count: accept_rpc_count.clone(),
        },
    );
    client
        .accept_targeted_rescan_delivery_with_failure()
        .await
        .expect("scoped manual-rescan delivery accept from rearm proof");

    assert_eq!(
        accept_rpc_count.load(Ordering::Relaxed),
        0,
        "scoped manual-rescan acceptance must use current rearm-ack delivery proof instead of queueing another worker control RPC"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_status_observability_returns_runtime_scope_control_cache_before_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    client.with_cache_mut(|cache| {
        cache.status = Some(SourceStatusSnapshot::default());
        cache.lifecycle_state = Some("runtime-scope-ready".to_string());
        cache.last_live_observability_snapshot_at = Some(
            Instant::now()
                - SOURCE_WORKER_NONBLOCKING_OBSERVABILITY_CACHE_TTL
                - Duration::from_millis(50),
        );
    });
    client.arm_control_frame_replay().await;
    assert!(
        matches!(
            client.control_state.lock().await.replay_state(),
            SourceControlReplayState::Required
        ),
        "precondition: retained source replay must be pending"
    );

    let (snapshot, used_cached_fallback) = client
        .observability_snapshot_nonblocking_for_status_route()
        .await;

    assert!(
        used_cached_fallback,
        "generic source-status should return bounded runtime-scope cache evidence"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()]),
        "source-status must expose local source runtime-scope evidence before retained replay"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-b"),
        Some(&vec!["nfs1".to_string()]),
        "source-status must expose local scan runtime-scope evidence before retained replay"
    );
    assert!(
        snapshot
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "runtime-scope cache evidence must be labelled separately from live worker status: {:?}",
        snapshot.status.degraded_roots
    );
    assert!(
        matches!(
            client.control_state.lock().await.replay_state(),
            SourceControlReplayState::Required
        ),
        "nonblocking source-status may report ownership from control cache but must not consume retained replay"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_observability_accepts_recent_active_cache_when_only_force_find_observability_remains()
 {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let _hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    assert!(
        source_observability_snapshot_has_active_state(&live),
        "live snapshot must be active before recent-cache force-find preservation is checked"
    );

    let mut later_live = live.clone();
    later_live.scheduled_source_groups_by_node.clear();
    later_live.scheduled_scan_groups_by_node.clear();
    later_live.last_control_frame_signals_by_node.clear();
    later_live.published_batches_by_node.clear();
    later_live.published_events_by_node.clear();
    later_live.published_control_events_by_node.clear();
    later_live.published_data_events_by_node.clear();
    later_live.last_published_at_us_by_node.clear();
    later_live.last_published_origins_by_node.clear();
    later_live.published_origin_counts_by_node.clear();
    later_live.published_path_capture_target = None;
    later_live.enqueued_path_origin_counts_by_node.clear();
    later_live.pending_path_origin_counts_by_node.clear();
    later_live.yielded_path_origin_counts_by_node.clear();
    later_live.summarized_path_origin_counts_by_node.clear();
    later_live.published_path_origin_counts_by_node.clear();
    later_live.last_force_find_runner_by_group =
        std::collections::BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]);
    later_live.force_find_inflight_groups = vec!["nfs1".to_string()];

    client.update_cached_observability_snapshot(&live);
    client.update_cached_observability_snapshot(&later_live);

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });
    client.with_cache_mut(|cache| {
        cache.last_live_observability_snapshot_at = Some(Instant::now());
    });

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;

    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        0,
        "recent active cache with only force-find observability should not be treated like a debug-empty cache that forces a live RPC"
    );
    assert_eq!(
        nonblocking.last_force_find_runner_by_group.get("nfs1"),
        Some(&"node-a::nfs1".to_string()),
        "recent active cache must preserve last force-find runner when it is the only remaining observability truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.force_find_inflight_groups,
        vec!["nfs1".to_string()],
        "recent active cache must preserve force-find inflight groups when they are the only remaining observability truth: {:?}",
        nonblocking
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_preserves_last_control_summary_when_later_active_snapshot_omits_it() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    assert!(
        first_live
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "initial live snapshot must carry control summary before omission preservation is checked: {:?}",
        first_live.last_control_frame_signals_by_node
    );

    let mut later_live = first_live.clone();
    later_live.last_control_frame_signals_by_node.clear();
    later_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);
    later_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);
    later_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert!(
        nonblocking
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "recent live cache must preserve last control summary when a later active snapshot omits it: {:?}",
        nonblocking.last_control_frame_signals_by_node
    );
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&6),
        "preserving last control summary must not roll back newer publication counters"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_preserves_scheduled_groups_when_later_active_snapshot_omits_them() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    assert_eq!(
        first_live.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "initial live snapshot must carry scheduled source groups before omission preservation is checked",
    );
    assert_eq!(
        first_live.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "initial live snapshot must carry scheduled scan groups before omission preservation is checked",
    );

    let mut later_live = first_live.clone();
    later_live.scheduled_source_groups_by_node.clear();
    later_live.scheduled_scan_groups_by_node.clear();
    later_live.last_control_frame_signals_by_node.clear();
    later_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);
    later_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);
    later_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 6)]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        nonblocking.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "recent live cache must preserve last scheduled source groups when a later active snapshot omits them: {:?}",
        nonblocking.scheduled_source_groups_by_node
    );
    assert_eq!(
        nonblocking.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "recent live cache must preserve last scheduled scan groups when a later active snapshot omits them: {:?}",
        nonblocking.scheduled_scan_groups_by_node
    );
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&6),
        "preserving last scheduled groups must not roll back newer publication counters"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_preserves_published_maps_when_later_active_snapshot_omits_them()
 {
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        SourceConfig::default(),
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    let first_live = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(1),
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        last_control_frame_signals_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["activate unit=runtime.exec.source".to_string()],
        )]),
        published_batches_by_node: std::collections::BTreeMap::from([("node-a".to_string(), 4)]),
        published_events_by_node: std::collections::BTreeMap::from([("node-a".to_string(), 9)]),
        published_control_events_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            4,
        )]),
        published_data_events_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            5,
        )]),
        last_published_at_us_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            123_456,
        )]),
        last_published_origins_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["node-a::nfs1".to_string(), "node-a::nfs2".to_string()],
        )]),
        published_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["node-a::nfs1=4".to_string(), "node-a::nfs2=5".to_string()],
        )]),
        published_path_capture_target: Some("/force-find-stress".to_string()),
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["/force-find-stress node-a::nfs1=1".to_string()],
        )]),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["/force-find-stress node-a::nfs1=1".to_string()],
        )]),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["/force-find-stress node-a::nfs1=1".to_string()],
        )]),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["/force-find-stress node-a::nfs1=1".to_string()],
        )]),
        published_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["/force-find-stress node-a::nfs1=1".to_string()],
        )]),
    };
    let mut later_live = first_live.clone();
    later_live.published_batches_by_node.clear();
    later_live.published_events_by_node.clear();
    later_live.published_control_events_by_node.clear();
    later_live.published_data_events_by_node.clear();
    later_live.last_published_at_us_by_node.clear();
    later_live.last_published_origins_by_node.clear();
    later_live.published_origin_counts_by_node.clear();
    later_live.published_path_capture_target = None;
    later_live.enqueued_path_origin_counts_by_node.clear();
    later_live.pending_path_origin_counts_by_node.clear();
    later_live.yielded_path_origin_counts_by_node.clear();
    later_live.summarized_path_origin_counts_by_node.clear();
    later_live.published_path_origin_counts_by_node.clear();
    later_live.last_force_find_runner_by_group =
        std::collections::BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    assert!(
        degraded
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "control-inflight nonblocking snapshot should use runtime-scope control-cache evidence: {:?}",
        degraded.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&degraded),
        "control-inflight runtime-scope control cache should not be classified as worker-unavailable evidence"
    );
    assert_eq!(
        degraded.published_batches_by_node.get("node-a"),
        Some(&4),
        "cached fallback must preserve published batch counts when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.published_events_by_node.get("node-a"),
        Some(&9),
        "cached fallback must preserve published event counts when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.published_control_events_by_node.get("node-a"),
        Some(&4),
        "cached fallback must preserve published control-event counts when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.published_data_events_by_node.get("node-a"),
        Some(&5),
        "cached fallback must preserve published data-event counts when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.last_published_at_us_by_node.get("node-a"),
        Some(&123_456),
        "cached fallback must preserve last_published_at_us when a later active snapshot omits it: {:?}",
        degraded
    );
    assert_eq!(
        degraded.last_published_origins_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string()
        ]),
        "cached fallback must preserve last_published_origins when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.published_origin_counts_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1=4".to_string(),
            "node-a::nfs2=5".to_string()
        ]),
        "cached fallback must preserve published_origin_counts when a later active snapshot omits them: {:?}",
        degraded
    );
    assert_eq!(
        degraded.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "cached fallback must preserve published_path_capture_target when a later active snapshot omits it: {:?}",
        degraded
    );
    assert_eq!(
        degraded.last_force_find_runner_by_group.get("nfs1"),
        Some(&"node-a::nfs1".to_string()),
        "preserving cached published observability must not roll back stronger later live force-find facts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_preserves_published_observability_when_later_active_snapshot_omits_it() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let mut first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    first_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 9)]);
    first_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_data_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 5)]);
    first_live.last_published_at_us_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 123_456)]);
    first_live.last_published_origins_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1".to_string(), "node-a::nfs2".to_string()],
    )]);
    first_live.published_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1=4".to_string(), "node-a::nfs2=5".to_string()],
    )]);
    first_live.published_path_capture_target = Some("/force-find-stress".to_string());
    first_live.enqueued_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.pending_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.yielded_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.summarized_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.published_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);

    let mut later_live = first_live.clone();
    later_live.published_batches_by_node.clear();
    later_live.published_events_by_node.clear();
    later_live.published_control_events_by_node.clear();
    later_live.published_data_events_by_node.clear();
    later_live.last_published_at_us_by_node.clear();
    later_live.last_published_origins_by_node.clear();
    later_live.published_origin_counts_by_node.clear();
    later_live.published_path_capture_target = None;
    later_live.enqueued_path_origin_counts_by_node.clear();
    later_live.pending_path_origin_counts_by_node.clear();
    later_live.yielded_path_origin_counts_by_node.clear();
    later_live.summarized_path_origin_counts_by_node.clear();
    later_live.published_path_origin_counts_by_node.clear();
    later_live.last_force_find_runner_by_group =
        std::collections::BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&4),
        "recent live cache must preserve published batch counts when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_events_by_node.get("node-a"),
        Some(&9),
        "recent live cache must preserve published event counts when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_control_events_by_node.get("node-a"),
        Some(&4),
        "recent live cache must preserve published control-event counts when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_data_events_by_node.get("node-a"),
        Some(&5),
        "recent live cache must preserve published data-event counts when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_published_at_us_by_node.get("node-a"),
        Some(&123_456),
        "recent live cache must preserve last_published_at_us when a later active snapshot omits it: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_published_origins_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string()
        ]),
        "recent live cache must preserve last_published_origins when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_origin_counts_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1=4".to_string(),
            "node-a::nfs2=5".to_string()
        ]),
        "recent live cache must preserve published_origin_counts when a later active snapshot omits them: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "recent live cache must preserve published_path_capture_target when a later active snapshot omits it: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_force_find_runner_by_group.get("nfs1"),
        Some(&"node-a::nfs1".to_string()),
        "preserving published observability must not roll back stronger later live facts"
    );

    client.close().await.expect("close source worker");
}

#[test]
fn published_path_capture_target_only_snapshot_is_not_debug_maps_absent() {
    let snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::new(),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::new(),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: Some("/force-find-stress".to_string()),
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    assert!(
        !source_observability_snapshot_debug_maps_absent(&snapshot),
        "published_path_capture_target should count as debug-map evidence instead of being treated like a debug-empty snapshot: {snapshot:?}"
    );
}

#[test]
fn force_find_observability_only_snapshot_is_not_debug_maps_absent() {
    let snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::from([(
            "nfs1".to_string(),
            "node-a::nfs1".to_string(),
        )]),
        force_find_inflight_groups: vec!["nfs1".to_string()],
        scheduled_source_groups_by_node: std::collections::BTreeMap::new(),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::new(),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    assert!(
        !source_observability_snapshot_debug_maps_absent(&snapshot),
        "force-find runner/inflight observability should count as debug-map evidence instead of being treated like a debug-empty snapshot: {snapshot:?}"
    );
}

#[test]
fn force_find_observability_only_snapshot_counts_as_active_state() {
    let snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::from([(
            "nfs1".to_string(),
            "node-a::nfs1".to_string(),
        )]),
        force_find_inflight_groups: vec!["nfs1".to_string()],
        scheduled_source_groups_by_node: std::collections::BTreeMap::new(),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::new(),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    assert!(
        source_observability_snapshot_has_active_state(&snapshot),
        "force-find runner/inflight observability should count as active-state truth so omission-thin live snapshots can preserve control/published facts: {snapshot:?}"
    );
}

#[test]
fn merge_live_observability_snapshot_with_recent_cache_preserves_recent_control_and_published_observability_when_live_reply_omits_them()
 {
    let live = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 0,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(7),
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::from([(
            "nfs1".to_string(),
            "node-a::nfs1".to_string(),
        )]),
        force_find_inflight_groups: vec!["nfs1".to_string()],
        scheduled_source_groups_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::new(),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };
    let cache = SourceWorkerSnapshotCache {
        lifecycle_state: Some("ready".to_string()),
        status: Some(live.status.clone()),
        last_control_frame_signals_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["activate unit=runtime.exec.source".to_string()],
        )])),
        published_batches_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            4,
        )])),
        published_events_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            9,
        )])),
        published_control_events_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            4,
        )])),
        published_data_events_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            5,
        )])),
        last_published_at_us_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            123_456,
        )])),
        last_published_origins_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["node-a::nfs1".to_string(), "node-a::nfs2".to_string()],
        )])),
        published_origin_counts_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["node-a::nfs1=4".to_string(), "node-a::nfs2=5".to_string()],
        )])),
        published_path_capture_target: Some("/force-find-stress".to_string()),
        ..SourceWorkerSnapshotCache::default()
    };

    let merged = merge_live_observability_snapshot_with_recent_cache(&cache, &live);

    assert_eq!(
        merged.last_control_frame_signals_by_node.get("node-a"),
        Some(&vec!["activate unit=runtime.exec.source".to_string()]),
        "successful live omission-thin snapshots must preserve recent control summary instead of dropping to empty: {merged:?}"
    );
    assert_eq!(
        merged.published_batches_by_node.get("node-a"),
        Some(&4),
        "successful live omission-thin snapshots must preserve recent published batch counters instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.published_events_by_node.get("node-a"),
        Some(&9),
        "successful live omission-thin snapshots must preserve recent published event counters instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.published_control_events_by_node.get("node-a"),
        Some(&4),
        "successful live omission-thin snapshots must preserve recent published control-event counters instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.published_data_events_by_node.get("node-a"),
        Some(&5),
        "successful live omission-thin snapshots must preserve recent published data-event counters instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.last_published_at_us_by_node.get("node-a"),
        Some(&123_456),
        "successful live omission-thin snapshots must preserve last_published_at_us instead of dropping it: {merged:?}"
    );
    assert_eq!(
        merged.last_published_origins_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string()
        ]),
        "successful live omission-thin snapshots must preserve last_published_origins instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.published_origin_counts_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1=4".to_string(),
            "node-a::nfs2=5".to_string()
        ]),
        "successful live omission-thin snapshots must preserve published_origin_counts instead of dropping them: {merged:?}"
    );
    assert_eq!(
        merged.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "successful live omission-thin snapshots must preserve published_path_capture_target instead of dropping it: {merged:?}"
    );
    assert_eq!(
        merged.last_force_find_runner_by_group.get("nfs1"),
        Some(&"node-a::nfs1".to_string()),
        "preserving recent control/published observability must not roll back stronger live force-find facts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_preserves_published_observability_when_later_live_snapshot_only_has_recovered_schedule_and_control_state()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let mut first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    first_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 9)]);
    first_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_data_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 5)]);
    first_live.last_published_at_us_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 123_456)]);
    first_live.last_published_origins_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1".to_string(), "node-a::nfs2".to_string()],
    )]);
    first_live.published_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1=4".to_string(), "node-a::nfs2=5".to_string()],
    )]);
    first_live.published_path_capture_target = Some("/force-find-stress".to_string());
    first_live.enqueued_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.pending_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.yielded_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.summarized_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.published_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);

    let mut later_live = first_live.clone();
    later_live.status.current_stream_generation = None;
    later_live.status.logical_roots.clear();
    later_live.status.concrete_roots.clear();
    later_live.last_control_frame_signals_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["recovered activate unit=runtime.exec.source".to_string()],
    )]);
    later_live.published_batches_by_node.clear();
    later_live.published_events_by_node.clear();
    later_live.published_control_events_by_node.clear();
    later_live.published_data_events_by_node.clear();
    later_live.last_published_at_us_by_node.clear();
    later_live.last_published_origins_by_node.clear();
    later_live.published_origin_counts_by_node.clear();
    later_live.published_path_capture_target = None;
    later_live.enqueued_path_origin_counts_by_node.clear();
    later_live.pending_path_origin_counts_by_node.clear();
    later_live.yielded_path_origin_counts_by_node.clear();
    later_live.summarized_path_origin_counts_by_node.clear();
    later_live.published_path_origin_counts_by_node.clear();
    later_live.last_force_find_runner_by_group =
        std::collections::BTreeMap::from([("nfs1".to_string(), "node-a::nfs1".to_string())]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        nonblocking.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "recent live cache must keep recovered scheduled source groups from the later live snapshot: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "recent live cache must keep recovered scheduled scan groups from the later live snapshot: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_control_frame_signals_by_node.get("node-a"),
        Some(&vec![
            "recovered activate unit=runtime.exec.source".to_string()
        ]),
        "recent live cache must keep recovered control summary from the later live snapshot: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&4),
        "recent live cache must preserve published batch counts when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_events_by_node.get("node-a"),
        Some(&9),
        "recent live cache must preserve published event counts when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_control_events_by_node.get("node-a"),
        Some(&4),
        "recent live cache must preserve published control-event counts when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_data_events_by_node.get("node-a"),
        Some(&5),
        "recent live cache must preserve published data-event counts when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_published_at_us_by_node.get("node-a"),
        Some(&123_456),
        "recent live cache must preserve last_published_at_us when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_published_origins_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string()
        ]),
        "recent live cache must preserve last_published_origins when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_origin_counts_by_node.get("node-a"),
        Some(&vec![
            "node-a::nfs1=4".to_string(),
            "node-a::nfs2=5".to_string()
        ]),
        "recent live cache must preserve published_origin_counts when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "recent live cache must preserve published_path_capture_target when a later live snapshot only keeps recovered schedule/control truth: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.last_force_find_runner_by_group.get("nfs1"),
        Some(&"node-a::nfs1".to_string()),
        "preserving published observability must not roll back stronger later live force-find facts"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_clears_published_path_observability_when_later_active_snapshot_explicitly_reports_zero_publication_counters()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let mut first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    first_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 9)]);
    first_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 4)]);
    first_live.published_data_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 5)]);
    first_live.last_published_at_us_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 123_456)]);
    first_live.last_published_origins_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1".to_string(), "node-a::nfs2".to_string()],
    )]);
    first_live.published_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["node-a::nfs1=4".to_string(), "node-a::nfs2=5".to_string()],
    )]);
    first_live.published_path_capture_target = Some("/force-find-stress".to_string());
    first_live.enqueued_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.pending_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.yielded_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.summarized_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);
    first_live.published_path_origin_counts_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["/force-find-stress node-a::nfs1=1".to_string()],
    )]);

    let mut later_live = first_live.clone();
    later_live.published_batches_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 0)]);
    later_live.published_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 0)]);
    later_live.published_control_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 0)]);
    later_live.published_data_events_by_node =
        std::collections::BTreeMap::from([("node-a".to_string(), 0)]);
    later_live.last_published_at_us_by_node.clear();
    later_live.last_published_origins_by_node.clear();
    later_live.published_origin_counts_by_node.clear();
    later_live.published_path_capture_target = None;
    later_live.enqueued_path_origin_counts_by_node.clear();
    later_live.pending_path_origin_counts_by_node.clear();
    later_live.yielded_path_origin_counts_by_node.clear();
    later_live.summarized_path_origin_counts_by_node.clear();
    later_live.published_path_origin_counts_by_node.clear();

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&0),
        "recent live cache must retain explicit zero published batch counters instead of replaying stale positive counts: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_events_by_node.get("node-a"),
        Some(&0),
        "recent live cache must retain explicit zero published event counters instead of replaying stale positive counts: {:?}",
        nonblocking
    );
    assert!(
        !nonblocking
            .last_published_at_us_by_node
            .contains_key("node-a"),
        "explicit zero published counters must clear stale last_published_at_us instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        !nonblocking
            .last_published_origins_by_node
            .contains_key("node-a"),
        "explicit zero published counters must clear stale last_published_origins instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        !nonblocking
            .published_origin_counts_by_node
            .contains_key("node-a"),
        "explicit zero published counters must clear stale published_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert_eq!(
        nonblocking.published_path_capture_target, None,
        "explicit zero published counters must clear stale published_path_capture_target instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        nonblocking.enqueued_path_origin_counts_by_node.is_empty(),
        "explicit zero published counters must clear stale enqueued_path_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        nonblocking.pending_path_origin_counts_by_node.is_empty(),
        "explicit zero published counters must clear stale pending_path_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        nonblocking.yielded_path_origin_counts_by_node.is_empty(),
        "explicit zero published counters must clear stale yielded_path_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        nonblocking.summarized_path_origin_counts_by_node.is_empty(),
        "explicit zero published counters must clear stale summarized_path_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );
    assert!(
        nonblocking.published_path_origin_counts_by_node.is_empty(),
        "explicit zero published counters must clear stale published_path_origin_counts instead of replaying cached metadata: {:?}",
        nonblocking
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_fail_closes_incomplete_active_cache_when_worker_unavailable()
 {
    let _observability_hook_guard = source_worker_observability_hook_test_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    assert!(
        source_observability_snapshot_has_active_state(&live),
        "live snapshot must be active before the fail-closed cache fallback invariant is checked"
    );
    assert!(
        !source_observability_snapshot_debug_maps_absent(&live),
        "live snapshot must carry debug maps before cache corruption: {:?}",
        live
    );

    let mut incomplete = live.clone();
    incomplete.scheduled_source_groups_by_node.clear();
    incomplete.scheduled_scan_groups_by_node.clear();
    incomplete.last_control_frame_signals_by_node.clear();
    incomplete.published_batches_by_node.clear();
    incomplete.published_events_by_node.clear();
    incomplete.published_control_events_by_node.clear();
    incomplete.published_data_events_by_node.clear();
    incomplete.last_published_at_us_by_node.clear();
    incomplete.last_published_origins_by_node.clear();
    incomplete.published_origin_counts_by_node.clear();
    incomplete.enqueued_path_origin_counts_by_node.clear();
    incomplete.pending_path_origin_counts_by_node.clear();
    incomplete.yielded_path_origin_counts_by_node.clear();
    incomplete.summarized_path_origin_counts_by_node.clear();
    incomplete.published_path_origin_counts_by_node.clear();
    client.update_cached_observability_snapshot(&incomplete);

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::Internal("synthetic observability failure".to_string()),
    });

    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert_eq!(
        degraded.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "worker-unavailable fallback should degrade the snapshot instead of returning a live-looking active cache"
    );
    assert!(
        !source_observability_snapshot_has_active_state(&degraded)
            || !source_observability_snapshot_debug_maps_absent(&degraded),
        "worker-unavailable fallback must not return an active-but-debug-empty source snapshot: {:?}",
        degraded
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_refresh_retries_transient_empty_scheduled_groups_before_publishing_cache()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776102761141088687226881".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::collections::VecDeque::from([(
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            )]),
        },
    );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply control wave");

    let inflight = client.begin_control_op();
    let stale = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    assert_eq!(
        stale.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()],)]),
        "on_control_frame must retry transient empty scheduled-source refreshes before publishing the next degraded cache snapshot"
    );
    assert_eq!(
        stale.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()],)]),
        "on_control_frame must retry transient empty scheduled-scan refreshes before publishing the next degraded cache snapshot"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_latest_activate_signals()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-c-29776225407437800789245953".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let source_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];
    let control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave)
    .collect::<Vec<_>>();

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-c".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must publish the latest runtime-managed source groups from accepted activate scopes even before live refresh converges"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must publish the latest runtime-managed scan groups from accepted activate scopes even before live refresh converges"
    );
    assert!(
        degraded
            .status
            .degraded_roots
            .iter()
            .any(|(root_key, reason)| {
                root_key == SOURCE_WORKER_DEGRADED_ROOT_KEY
                    && reason == SOURCE_WORKER_RUNTIME_SCOPE_CACHE_REASON
            }),
        "control-inflight source-status must be marked as runtime-scope control-cache evidence, not worker-unavailable evidence: {:?}",
        degraded.status.degraded_roots
    );
    assert!(
        !source_observability_snapshot_is_degraded_worker_cache(&degraded),
        "control-inflight runtime-scope control cache should not be classified as worker-unavailable evidence"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_preserves_latest_control_frame_signals_after_successful_activate_wave()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-c-29776225407437800789245953".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-c-29776225407437800789245953".to_string(),
        vec![
            "activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.source route=source-manual-rescan-control:v1.stream generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.source route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.scan route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
        ],
    )]);
    assert_eq!(
        degraded.last_control_frame_signals_by_node, expected,
        "control-inflight nonblocking observability must preserve the latest accepted activate-wave control summary even before live refresh converges"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_local_resource_ids_without_grant_change()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776249969860401661214721".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave without grant change");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must derive latest runtime-managed source groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must derive latest runtime-managed scan groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_limits_bare_logical_scope_ids_to_local_granted_roots_under_mixed_cluster_grants()
 {
    let tmp = tempdir().expect("create temp dir");
    let node_a_nfs1 = tmp.path().join("node-a-nfs1");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_b_nfs1 = tmp.path().join("node-b-nfs1");
    let node_c_nfs1 = tmp.path().join("node-c-nfs1");
    let node_c_nfs2 = tmp.path().join("node-c-nfs2");
    let node_d_nfs2 = tmp.path().join("node-d-nfs2");
    let node_b_nfs3 = tmp.path().join("node-b-nfs3");
    let node_d_nfs3 = tmp.path().join("node-d-nfs3");
    let node_e_nfs3 = tmp.path().join("node-e-nfs3");
    for path in [
        &node_a_nfs1,
        &node_a_nfs2,
        &node_b_nfs1,
        &node_c_nfs1,
        &node_c_nfs2,
        &node_d_nfs2,
        &node_b_nfs3,
        &node_d_nfs3,
        &node_e_nfs3,
    ] {
        std::fs::create_dir_all(path).expect("create mount dir");
    }
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
            worker_fs_source_watch_scan_root("nfs3", nfs3_source),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a-29799407896396737569357825".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs1",
                    "node-a-29799407896396737569357825",
                    "10.0.0.11",
                    &node_a_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs2",
                    "node-a-29799407896396737569357825",
                    "10.0.0.12",
                    &node_a_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs1",
                    "node-b-29799407896396737569357825",
                    "10.0.0.21",
                    &node_b_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs1",
                    "node-c-29799407896396737569357825",
                    "10.0.0.31",
                    &node_c_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs2",
                    "node-c-29799407896396737569357825",
                    "10.0.0.32",
                    &node_c_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs2",
                    "node-d-29799407896396737569357825",
                    "10.0.0.41",
                    &node_d_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs3",
                    "node-b-29799407896396737569357825",
                    "10.0.0.23",
                    &node_b_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs3",
                    "node-d-29799407896396737569357825",
                    "10.0.0.43",
                    &node_d_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-e-29799407896396737569357825::nfs3",
                    "node-e-29799407896396737569357825",
                    "10.0.0.53",
                    &node_e_nfs3,
                    nfs3_source,
                ),
            ],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply mixed-grant fs_source-selected control wave");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-a-29799407896396737569357825".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must not prime a non-local nfs3 schedule from bare logical scope ids when mixed cluster grants already identify node-a as runnable only for nfs1/nfs2"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must not prime a non-local nfs3 scan schedule from bare logical scope ids when mixed cluster grants already identify node-a as runnable only for nfs1/nfs2"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_does_not_exhaust_post_ack_schedule_refresh_when_mixed_cluster_grants_leave_no_local_runnable_groups()
 {
    let tmp = tempdir().expect("create temp dir");
    let node_a_nfs1 = tmp.path().join("node-a-nfs1");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_b_nfs1 = tmp.path().join("node-b-nfs1");
    let node_c_nfs1 = tmp.path().join("node-c-nfs1");
    let node_c_nfs2 = tmp.path().join("node-c-nfs2");
    let node_d_nfs2 = tmp.path().join("node-d-nfs2");
    let node_b_nfs3 = tmp.path().join("node-b-nfs3");
    let node_d_nfs3 = tmp.path().join("node-d-nfs3");
    let node_e_nfs3 = tmp.path().join("node-e-nfs3");
    for path in [
        &node_a_nfs1,
        &node_a_nfs2,
        &node_b_nfs1,
        &node_c_nfs1,
        &node_c_nfs2,
        &node_d_nfs2,
        &node_b_nfs3,
        &node_d_nfs3,
        &node_e_nfs3,
    ] {
        std::fs::create_dir_all(path).expect("create mount dir");
    }
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
            worker_fs_source_watch_scan_root("nfs3", nfs3_source),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-e-29799407896396737569357825".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs1",
                    "node-a-29799407896396737569357825",
                    "10.0.0.11",
                    &node_a_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs2",
                    "node-a-29799407896396737569357825",
                    "10.0.0.12",
                    &node_a_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs1",
                    "node-b-29799407896396737569357825",
                    "10.0.0.21",
                    &node_b_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs1",
                    "node-c-29799407896396737569357825",
                    "10.0.0.31",
                    &node_c_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs2",
                    "node-c-29799407896396737569357825",
                    "10.0.0.32",
                    &node_c_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs2",
                    "node-d-29799407896396737569357825",
                    "10.0.0.41",
                    &node_d_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs3",
                    "node-b-29799407896396737569357825",
                    "10.0.0.23",
                    &node_b_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs3",
                    "node-d-29799407896396737569357825",
                    "10.0.0.43",
                    &node_d_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-e-29799407896396737569357825::nfs3",
                    "node-e-29799407896396737569357825",
                    "10.0.0.53",
                    &node_e_nfs3,
                    nfs3_source,
                ),
            ],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame_with_timeouts_for_tests(
            control,
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect(
            "mixed-cluster wave with no local runnable groups should preserve an empty schedule instead of exhausting post-ack schedule refresh",
        );

    let observability = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    assert!(
        observability.scheduled_source_groups_by_node.is_empty(),
        "node-e should preserve an empty source schedule when the mixed-cluster wave carries only nfs1/nfs2 and all refreshed scheduled groups are legitimately empty for the local node"
    );
    assert!(
        observability.scheduled_scan_groups_by_node.is_empty(),
        "node-e should preserve an empty scan schedule when the mixed-cluster wave carries only nfs1/nfs2 and all refreshed scheduled groups are legitimately empty for the local node"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_scope_ids_for_fs_source_selected_roots()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
        ],
        host_object_grants: vec![
            worker_source_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                nfs1.clone(),
                nfs1_source,
            ),
            worker_source_export_with_fs_source(
                "node-b::nfs2",
                "node-b",
                "10.0.0.22",
                nfs2.clone(),
                nfs2_source,
            ),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776249969860401661214721".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed fs_source-selected control wave");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must preserve runtime-managed source groups from logical scope ids when fs_source-selected roots already have local bootstrap grants"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must preserve runtime-managed scan groups from logical scope ids when fs_source-selected roots already have local bootstrap grants"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let node_id = NodeId("node-a-29775285406139598021591041".to_string());
    let client = SourceWorkerClientHandle::new(
        node_id.clone(),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let live_worker = client.client().await.expect("connect source worker");
    let snapshot = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("fetch live snapshot");
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&expected),
        "scheduled source groups should be keyed by stable host_ref rather than instance-suffixed node id"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-a"),
        Some(&expected),
        "scheduled scan groups should be keyed by stable host_ref rather than instance-suffixed node id"
    );
    assert!(
        !snapshot
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into scheduled source groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert!(
        !snapshot
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into scheduled scan groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );

    client.close().await.expect("close source worker");
}

#[test]
fn observability_snapshot_normalization_uses_cached_grants_when_live_reply_grants_are_empty() {
    let node_id = NodeId("node-d-29776112502313141518991361".to_string());
    let cached_grants = vec![GrantedMountRoot {
        object_ref: "node-d::nfs2".to_string(),
        host_ref: "node-d".to_string(),
        host_ip: "10.0.0.41".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point: PathBuf::from("/mnt/nfs2"),
        fs_source: "nfs://server/export2".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }];
    let mut snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 2,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            vec!["nfs2".to_string()],
        )]),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            vec!["nfs2".to_string()],
        )]),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    normalize_observability_snapshot_scheduled_group_keys(
        &mut snapshot,
        &node_id,
        Some(&cached_grants),
        None,
    );

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-d".to_string(), vec!["nfs2".to_string()],)]),
        "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled source groups",
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-d".to_string(), vec!["nfs2".to_string()],)]),
        "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled scan groups",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_source_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs2", &nfs2)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs2",
            "node-d",
            "10.0.0.41",
            nfs2.clone(),
        )],
        ..SourceConfig::default()
    };
    let node_id = NodeId("node-d-29775443922859927994892289".to_string());
    let source = Arc::new(
        FSMetaSource::with_boundaries(
            cfg,
            node_id.clone(),
            Some(Arc::new(LoopbackWorkerBoundary::default())),
        )
        .expect("init source"),
    );

    source
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let snapshot = SourceFacade::Local(source)
        .observability_snapshot_with_failure()
        .await
        .expect("fetch local observability");
    let expected = vec!["nfs2".to_string()];
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected)
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected)
    );
    assert!(
        snapshot
            .last_control_frame_signals_by_node
            .get("node-d")
            .is_some_and(|signals| !signals.is_empty()),
        "local source observability must carry the accepted activate-wave control summary instead of source_control=[]: {:?}",
        snapshot.last_control_frame_signals_by_node
    );
    assert!(
        !snapshot
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into local scheduled source groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert!(
        !snapshot
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into local scheduled scan groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_source_observability_preserves_published_maps_after_local_stream_emits_batches() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::write(nfs1.join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = Arc::new(
        FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source"),
    );
    let mut stream = source.pub_().await.expect("start source pub stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < deadline {
        let batch = match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => batch,
            Ok(None) => panic!("local pub stream should yield initial scan batch"),
            Err(_) => continue,
        };
        for event in batch {
            let origin = event.metadata().origin_id.0.clone();
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                *control_counts.entry(origin).or_insert(0) += 1;
            } else {
                *data_counts.entry(origin).or_insert(0) += 1;
            }
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        if complete {
            break;
        }
    }

    assert!(
        ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        }),
        "local source stream should emit baseline control+data for both roots before observability check: control={control_counts:?} data={data_counts:?}"
    );

    let snapshot = SourceFacade::Local(source.clone())
        .observability_snapshot_with_failure()
        .await
        .expect("fetch local source observability after local stream publish");

    assert!(
        snapshot
            .published_batches_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must not report active scheduling/control truth with published_batches_by_node still empty after the local stream emitted batches: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .published_events_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must preserve published event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .published_control_events_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must preserve published control-event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .published_data_events_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must preserve published data-event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot.last_published_at_us_by_node.contains_key("node-a"),
        "local source observability must preserve a last_published_at_us timestamp after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .last_published_origins_by_node
            .get("node-a")
            .is_some_and(|origins| !origins.is_empty()),
        "local source observability must preserve last_published_origins after local stream publish: {:?}",
        snapshot
    );

    source.close().await.expect("close local source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_source_observability_does_not_fall_back_to_source_control_summary_without_recovered_active_state()
 {
    let source = Arc::new(
        FSMetaSource::new(SourceConfig::default(), NodeId("node-a".to_string()))
            .expect("init source"),
    );
    let activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: crate::runtime::routes::ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["node-a::nfs1".to_string()],
            }],
        }))
        .expect("encode source activate");
    source
        .on_control_frame(&[activate])
        .await
        .expect("apply source activate");

    assert!(
        !source.last_control_frame_signals_snapshot().is_empty(),
        "fixture must keep a non-empty source-owned control summary before active-state gating is checked"
    );

    let snapshot = SourceFacade::Local(source)
        .observability_snapshot_with_failure()
        .await
        .expect("fetch local observability");

    assert!(
        snapshot.last_control_frame_signals_by_node.is_empty(),
        "local observability must not fall back to stale source-owned control summary when no recovered active state exists: status={:?} source={:?} scan={:?} control={:?}",
        snapshot.status,
        snapshot.scheduled_source_groups_by_node,
        snapshot.scheduled_scan_groups_by_node,
        snapshot.last_control_frame_signals_by_node
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_active_local_status_yields_schedule_and_control_summary_when_runtime_state_is_empty()
 {
    let stable_host_ref = "node-d";
    let status = SourceStatusSnapshot {
        current_stream_generation: Some(7),
        logical_roots: Vec::new(),
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2@node-d::nfs2@/tmp/nfs2".to_string(),
            logical_root_id: "nfs2".to_string(),
            object_ref: "node-d::nfs2".to_string(),
            status: "running".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 65_536,
            audit_interval_ms: 300_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 7,
            emitted_event_count: 4051,
            emitted_control_event_count: 2,
            emitted_data_event_count: 4049,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(123456),
            last_emitted_origins: vec!["node-d::nfs2=1".to_string()],
            forwarded_batch_count: 7,
            forwarded_event_count: 4051,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(123456),
            last_forwarded_origins: vec!["node-d::nfs2=1".to_string()],
            current_revision: Some(1),
            current_stream_generation: Some(7),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };

    let source_groups =
        recovered_scheduled_groups_by_node_from_active_status(stable_host_ref, &status, |entry| {
            entry.watch_enabled
        });
    let scan_groups =
        recovered_scheduled_groups_by_node_from_active_status(stable_host_ref, &status, |entry| {
            entry.scan_enabled
        });
    let control_signals = recovered_control_signals_by_node_from_active_status(
        stable_host_ref,
        &status,
        &source_groups,
        &scan_groups,
    );

    let expected = vec!["nfs2".to_string()];
    assert_eq!(source_groups.get(stable_host_ref), Some(&expected));
    assert_eq!(scan_groups.get(stable_host_ref), Some(&expected));
    assert!(
        control_signals
            .get(stable_host_ref)
            .is_some_and(|signals| !signals.is_empty()),
        "active local status recovery must synthesize a non-empty control summary instead of source_control=[]: {:?}",
        control_signals
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_instance_suffixed_source_worker_recovers_schedule_from_real_source_route_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29775285406139598021591041".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let real_source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after real source route wave: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} stderr={}",
                client
                    .logical_roots_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(real_source_wave(2))
        .await
        .expect("initial real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(real_source_wave(3))
        .await
        .expect("restarted real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_runtime_host_grants_for_real_source_route_wave()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29775384077525007841886209".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after restart-preserved host grants: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} grants_version={} grants={:?} stderr={}",
                client
                    .logical_roots_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                client
                    .host_object_grants_version_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_multi_root_observability_after_runtime_managed_upgrade_recovery()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29776120697300046443446273".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_snapshot = std::collections::BTreeMap::from([(
        "node-c".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);

    async fn assert_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let worker = client.client().await.expect("connect source worker");
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for multi-root observability after runtime-managed restart recovery: snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_observability(
        &client,
        &expected_snapshot,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_observability(
        &client,
        &expected_snapshot,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_observability_preserves_last_control_frame_signals_after_runtime_managed_upgrade_recovery()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29776120697300046443446273".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot");
        if matches!(
            snapshot.last_control_frame_signals_by_node.get("node-c"),
            Some(signals) if !signals.is_empty()
        ) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restarted runtime-managed upgrade recovery must preserve last_control_frame_signals_by_node in live observability snapshot: {:?}",
            snapshot.last_control_frame_signals_by_node
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_single_root_observability_after_runtime_managed_upgrade_recovery()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let worker = client.client().await.expect("connect source worker");
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for schedule+observability convergence after runtime-managed restart recovery: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot_with_failure()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_cache_fallback_preserves_stable_host_ref_after_runtime_managed_upgrade_recovery()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let node_id = NodeId("node-b-29775497172756365788053505".to_string());
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            node_id.clone(),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");

    let inflight = client.begin_control_op();
    let degraded = client
        .observability_snapshot_nonblocking_for_status_route()
        .await
        .0;
    drop(inflight);

    let expected = vec!["nfs1".to_string()];
    assert_eq!(
        degraded.scheduled_source_groups_by_node.get("node-b"),
        Some(&expected),
        "cache fallback should preserve stable host_ref-keyed source schedule after runtime-managed upgrade recovery: {:?}",
        degraded.scheduled_source_groups_by_node
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node.get("node-b"),
        Some(&expected),
        "cache fallback should preserve stable host_ref-keyed scan schedule after runtime-managed upgrade recovery: {:?}",
        degraded.scheduled_scan_groups_by_node
    );
    assert!(
        !degraded
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into cached degraded source schedule: {:?}",
        degraded.scheduled_source_groups_by_node
    );
    assert!(
        !degraded
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into cached degraded scan schedule: {:?}",
        degraded.scheduled_scan_groups_by_node
    );

    client.close().await.expect("close source worker");
}

fn worker_stderr_excerpt(socket_dir: &Path) -> String {
    let mut excerpts = std::fs::read_dir(socket_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|row| row.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".stderr.log"))
        })
        .filter_map(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .map(|text| format!("{}:\n{}", path.display(), text))
        })
        .collect::<Vec<_>>();
    excerpts.sort();
    excerpts.join("\n---\n")
}

#[path = "tests/generation_one_local_apply_recovery.rs"]
mod generation_one_local_apply_recovery;
#[path = "tests/recovery.rs"]
mod recovery;
#[path = "tests/trigger_rescan_republish.rs"]
mod trigger_rescan_republish;

trigger_rescan_republish::define_trigger_rescan_republish_tests!();
generation_one_local_apply_recovery::define_generation_one_local_apply_recovery_tests!();
recovery::define_recovery_tests!();
