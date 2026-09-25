use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use data_view::{
    ActionExecutionState, BoundedActionRegistry, BoundedSourceAction, ContinuationPolicy,
    CoverageState, CreateStreamingSubscription, DATA_VIEW_WIRE_VERSION, DataViewContractError,
    DataViewRef, DiscoveryOperation, FailureMode, FreshnessState, IdGenerator, NativeAnchor,
    ObservationPath, PayloadEncoding, ProjectionApplyEvidence, ProjectionCoverage, ProjectionMode,
    SchemaVersionRef, SideEffectLevel, SourceAuthorityRef, SourceProjectionRef,
    StreamBatchDisposition, StreamingRefreshEntry, StreamingRefreshOperation,
    StreamingRefreshSubmission, StreamingSubscriptionRegistry, StreamingSubscriptionStatus,
    TrackedResultRef,
};

#[derive(Debug)]
struct SequenceIdGenerator {
    ids: Mutex<VecDeque<String>>,
}

impl SequenceIdGenerator {
    fn new(ids: &[&str]) -> Self {
        Self {
            ids: Mutex::new(ids.iter().map(|value| (*value).to_string()).collect()),
        }
    }
}

impl IdGenerator for SequenceIdGenerator {
    fn next_id(&self, prefix: &str) -> Result<String, DataViewContractError> {
        self.ids
            .lock()
            .expect("id queue")
            .pop_front()
            .map(|value| format!("{prefix}:{value}"))
            .ok_or(DataViewContractError::IdGenerationFailed)
    }
}

fn authority() -> SourceAuthorityRef {
    SourceAuthorityRef::new("source:pg:inventory").expect("authority")
}

fn schema() -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: "fustor.test.rows".into(),
        schema_version: "1".into(),
    }
}

fn direct_action() -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: "action:inventory-schema".into(),
        source_authority_ref: authority(),
        observation_path: ObservationPath::Direct,
        operation: DiscoveryOperation::SchemaProbe,
        permission_scope: "tenant-a:inventory".into(),
        freshness_precondition: FreshnessState::Fresh,
        max_rows: 10,
        max_bytes: 4096,
        max_calls: 1,
        max_fanout: 1,
        max_latency_ms: 1000,
        max_cost_units: 4,
        side_effect_level: SideEffectLevel::ReadOnly,
        continuation_policy: ContinuationPolicy::None,
        failure_modes: vec![FailureMode::NotDisclosed],
        expansion_rules: vec![],
        expected_information_gain: 80,
        explanation: "inspect the visible inventory schema".into(),
    }
}

fn projection_ref() -> SourceProjectionRef {
    SourceProjectionRef {
        projection_id: "projection:inventory-index".into(),
        source_authority_ref: authority(),
        projection_kind: "materialized-index".into(),
    }
}

fn projection_evidence() -> ProjectionApplyEvidence {
    ProjectionApplyEvidence {
        projection: projection_ref(),
        source_revision: "source-rev-8".into(),
        projection_revision: "projection-rev-7".into(),
        applied_change_id: "change-8".into(),
        coverage: ProjectionCoverage {
            state: CoverageState::CompleteForScope,
            scope_ref: "tenant-a:inventory".into(),
            limiting_reasons: vec![],
        },
        freshness: FreshnessState::Fresh,
        observed_at_ms: 120,
        evidence_refs: vec!["evidence:apply:8".into()],
    }
}

fn tracked(value: &str) -> TrackedResultRef {
    TrackedResultRef(value.into())
}

fn existing_results() -> BTreeSet<TrackedResultRef> {
    [tracked("result:a"), tracked("result:b")]
        .into_iter()
        .collect()
}

fn subscription_request(path: ObservationPath) -> CreateStreamingSubscription {
    CreateStreamingSubscription {
        wire_version: DATA_VIEW_WIRE_VERSION,
        tracked_result_refs: [tracked("result:a")].into_iter().collect(),
        source_authority_ref: authority(),
        observation_path: path,
        allowed_schema_versions: [schema()].into_iter().collect(),
        source_watermark: "wm-0".into(),
        max_events_per_batch: 10,
        max_bytes_per_batch: 4096,
        max_batches_per_window: 4,
        batch_window_ms: 100,
        max_in_flight: 1,
        lease_issued_at_ms: 100,
        lease_expires_at_ms: 1000,
        policy_context_digest: "policy-a".into(),
        grant_epoch: 4,
    }
}

fn refresh_submission(
    subscription_id: &str,
    projection_evidence: Option<ProjectionApplyEvidence>,
) -> StreamingRefreshSubmission {
    StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:stream:1".into(),
        subscription_id: subscription_id.into(),
        batch_id: "batch:1".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked("result:a"),
            operation: StreamingRefreshOperation::Refresh,
            payload: Some(br#"{"value":1}"#.to_vec()),
            native_anchors: vec![NativeAnchor {
                anchor_kind: "row".into(),
                native_ref: "inventory:1".into(),
                native_pointer: None,
                native_version: Some("8".into()),
            }],
            source_revision: "source-rev-8".into(),
            evidence_refs: vec!["evidence:row:1".into()],
            observed_at_ms: 120,
        }],
        projection_evidence,
        actual_bytes: 11,
        actual_events: 1,
    }
}

#[test]
fn bounded_action_registry_issues_only_current_actionable_contexts() {
    let ids = Arc::new(SequenceIdGenerator::new(&["exec-1", "idem-1"]));
    let mut registry = BoundedActionRegistry::new(ids);
    registry
        .register_action(direct_action())
        .expect("register action");

    let base_revision = DataViewRef {
        data_view_id: "global".into(),
        revision_id: "opaque-genesis".into(),
    };
    let historical = registry.issue_context(
        "action:inventory-schema",
        base_revision.clone(),
        ProjectionMode::HistoricalReplay,
        "policy-a",
        4,
        100,
        200,
    );
    assert_eq!(
        historical,
        Err(DataViewContractError::HistoricalReplayIsReadOnly)
    );

    let context = registry
        .issue_context(
            "action:inventory-schema",
            base_revision,
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("issue context");
    assert_eq!(context.execution_id, "execution:exec-1");
    assert_eq!(context.idempotency_key, "idempotency:idem-1");
    assert_eq!(context.action_contract_digest.len(), 64);
    assert!(
        registry
            .resolve_execution(&context.execution_id, 199)
            .is_ok()
    );
    assert_eq!(
        registry.resolve_execution(&context.execution_id, 200),
        Err(DataViewContractError::Expired)
    );
}

#[test]
fn action_commit_mark_is_idempotent_only_for_the_same_submission() {
    let ids = Arc::new(SequenceIdGenerator::new(&["exec-1", "idem-1"]));
    let mut registry = BoundedActionRegistry::new(ids);
    registry
        .register_action(direct_action())
        .expect("register action");
    let context = registry
        .issue_context(
            "action:inventory-schema",
            DataViewRef {
                data_view_id: "global".into(),
                revision_id: "opaque-genesis".into(),
            },
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("issue context");

    let first = registry
        .mark_committed(&context.execution_id, "submission:1", "outcome:1")
        .expect("first commit mark");
    assert_eq!(
        first,
        ActionExecutionState::Committed {
            submission_id: "submission:1".into(),
            outcome_ref: "outcome:1".into(),
        }
    );
    assert_eq!(
        registry
            .mark_committed(&context.execution_id, "submission:1", "outcome:1")
            .expect("same submission is idempotent"),
        first
    );
    assert_eq!(
        registry.mark_committed(&context.execution_id, "submission:2", "outcome:2"),
        Err(DataViewContractError::AlreadyExists("execution commit"))
    );
}

#[test]
fn subscription_creation_rejects_untracked_results_without_identifying_them() {
    let ids = Arc::new(SequenceIdGenerator::new(&["sub-1"]));
    let mut registry = StreamingSubscriptionRegistry::new(ids);
    let mut request = subscription_request(ObservationPath::Direct);
    request.tracked_result_refs.insert(tracked("result:new"));

    assert_eq!(
        registry.create(request, &existing_results()),
        Err(DataViewContractError::TrackedResultUnavailable)
    );
}

#[test]
fn direct_stream_rejects_projection_evidence_and_unknown_targets() {
    let ids = Arc::new(SequenceIdGenerator::new(&["sub-1"]));
    let mut registry = StreamingSubscriptionRegistry::new(ids);
    let subscription = registry
        .create(
            subscription_request(ObservationPath::Direct),
            &existing_results(),
        )
        .expect("create direct subscription");

    let with_projection =
        refresh_submission(&subscription.subscription_id, Some(projection_evidence()));
    assert_eq!(
        registry.validate_batch(&with_projection, 150, "policy-a", 4),
        Err(DataViewContractError::ProjectionEvidenceUnexpected)
    );

    let mut unknown = refresh_submission(&subscription.subscription_id, None);
    unknown.refreshes[0].tracked_result_ref = tracked("result:new");
    assert_eq!(
        registry.validate_batch(&unknown, 150, "policy-a", 4),
        Err(DataViewContractError::TrackedResultUnavailable)
    );
}

#[test]
fn projection_stream_requires_matching_apply_evidence_and_commits_watermark() {
    let ids = Arc::new(SequenceIdGenerator::new(&["sub-1"]));
    let mut registry = StreamingSubscriptionRegistry::new(ids);
    let subscription = registry
        .create(
            subscription_request(ObservationPath::ProjectionBacked {
                projection: projection_ref(),
            }),
            &existing_results(),
        )
        .expect("create projection subscription");

    let missing = refresh_submission(&subscription.subscription_id, None);
    assert_eq!(
        registry.validate_batch(&missing, 150, "policy-a", 4),
        Err(DataViewContractError::ProjectionEvidenceRequired)
    );

    let submission = refresh_submission(&subscription.subscription_id, Some(projection_evidence()));
    assert_eq!(
        registry
            .validate_batch(&submission, 150, "policy-a", 4)
            .expect("validate projection batch"),
        StreamBatchDisposition::New
    );
    registry
        .commit_batch(&submission, "outcome:stream:1")
        .expect("commit batch metadata");
    let current = registry
        .subscription(&subscription.subscription_id)
        .expect("subscription");
    assert_eq!(current.next_sequence, 2);
    assert_eq!(current.source_watermark, "wm-1");
    assert_eq!(
        registry
            .validate_batch(&submission, 151, "policy-a", 4)
            .expect("duplicate batch"),
        StreamBatchDisposition::Duplicate {
            outcome_ref: "outcome:stream:1".into(),
        }
    );
}

#[test]
fn grant_change_pauses_stream_for_revalidation() {
    let ids = Arc::new(SequenceIdGenerator::new(&["sub-1"]));
    let mut registry = StreamingSubscriptionRegistry::new(ids);
    let subscription = registry
        .create(
            subscription_request(ObservationPath::Direct),
            &existing_results(),
        )
        .expect("create subscription");
    let submission = refresh_submission(&subscription.subscription_id, None);

    assert_eq!(
        registry.validate_batch(&submission, 150, "policy-a", 5),
        Err(DataViewContractError::GrantEpochMismatch)
    );
    assert_eq!(
        registry
            .subscription(&subscription.subscription_id)
            .expect("subscription")
            .status,
        StreamingSubscriptionStatus::RequiresRevalidation
    );
}

#[test]
fn stream_registry_enforces_in_flight_and_batch_window_limits() {
    let ids = Arc::new(SequenceIdGenerator::new(&["sub-1"]));
    let mut registry = StreamingSubscriptionRegistry::new(ids);
    let mut request = subscription_request(ObservationPath::Direct);
    request.max_batches_per_window = 1;
    request.batch_window_ms = 100;
    request.max_in_flight = 1;
    let subscription = registry
        .create(request, &existing_results())
        .expect("subscription");
    let first = refresh_submission(&subscription.subscription_id, None);
    assert_eq!(
        registry
            .validate_batch(&first, 150, "policy-a", 4)
            .expect("reserve first"),
        StreamBatchDisposition::New
    );

    let mut concurrent = first.clone();
    concurrent.submission_id = "submission:stream:concurrent".into();
    concurrent.batch_id = "batch:concurrent".into();
    assert_eq!(
        registry.validate_batch(&concurrent, 151, "policy-a", 4),
        Err(DataViewContractError::LimitExceeded("in-flight"))
    );
    registry.abort_batch(&subscription.subscription_id);
    registry
        .validate_batch(&first, 152, "policy-a", 4)
        .expect("reserve after abort");
    registry
        .commit_batch(&first, "outcome:stream:1")
        .expect("commit first");

    let mut second = first;
    second.submission_id = "submission:stream:2".into();
    second.batch_id = "batch:2".into();
    second.sequence = 2;
    second.previous_watermark = "wm-1".into();
    second.resulting_watermark = "wm-2".into();
    assert_eq!(
        registry.validate_batch(&second, 160, "policy-a", 4),
        Err(DataViewContractError::LimitExceeded("batches-per-window"))
    );
    assert_eq!(
        registry
            .validate_batch(&second, 201, "policy-a", 4)
            .expect("new window"),
        StreamBatchDisposition::New
    );
    registry.abort_batch(&subscription.subscription_id);
}

#[test]
fn public_contracts_roundtrip_without_source_specific_variants() {
    let action = direct_action();
    let encoded = serde_json::to_vec(&action).expect("serialize action");
    let decoded: BoundedSourceAction = serde_json::from_slice(&encoded).expect("decode action");
    assert_eq!(decoded, action);
    assert!(
        !String::from_utf8(encoded)
            .expect("json")
            .contains("ingest-pg")
    );
}
