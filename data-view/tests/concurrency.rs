use std::collections::BTreeSet;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use data_view::{
    ActionResultSubmission, BoundedSourceAction, ContinuationPolicy, CreateStreamingSubscription,
    DATA_VIEW_WIRE_VERSION, DataViewCommitOutcome, DataViewCommitStatus, DataViewEngine,
    DerivationEvidence, DiscoveryOperation, FailureMode, FreshnessState, NativeAnchor,
    ObservationPath, PayloadEncoding, ProducerKind, ProjectionMode, SchemaVersionRef,
    SideEffectLevel, SingleResultNormalizer, SourceAuthorityRef, StreamingRefreshEntry,
    StreamingRefreshOperation, StreamingRefreshSubmission, StreamingSubscriptionStatus,
    TrackedResultRef,
};

fn authority() -> SourceAuthorityRef {
    SourceAuthorityRef::new("source:s3:finance").expect("authority")
}

fn schema() -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: "fustor.test.concurrent-object".into(),
        schema_version: "1".into(),
    }
}

fn action(action_id: &str) -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: action_id.into(),
        source_authority_ref: authority(),
        observation_path: ObservationPath::Direct,
        operation: DiscoveryOperation::ObjectHead,
        permission_scope: "tenant-a:finance".into(),
        freshness_precondition: FreshnessState::Fresh,
        max_rows: 10,
        max_bytes: 4096,
        max_calls: 1,
        max_fanout: 1,
        max_latency_ms: 1000,
        max_cost_units: 10,
        side_effect_level: SideEffectLevel::ReadOnly,
        continuation_policy: ContinuationPolicy::None,
        failure_modes: vec![FailureMode::NotDisclosed],
        expansion_rules: vec![],
        expected_information_gain: 60,
        explanation: "read one visible object".into(),
    }
}

fn engine() -> DataViewEngine {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    engine
}

fn submission(
    context: data_view::ActionExecutionContext,
    submission_id: &str,
    native_ref: &str,
    source_revision: &str,
    payload: &[u8],
) -> ActionResultSubmission {
    ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: submission_id.into(),
        execution: context,
        source_authority_ref: authority(),
        authority_object_ref: authority().0,
        producer_id: "s3-source:finance".into(),
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: schema(),
        encoding: PayloadEncoding::Json,
        payload: payload.to_vec(),
        native_anchors: vec![NativeAnchor {
            anchor_kind: "s3-object".into(),
            native_ref: native_ref.into(),
            native_pointer: Some(format!("s3://finance/{native_ref}")),
            native_version: Some(source_revision.into()),
        }],
        source_revision: source_revision.into(),
        evidence_refs: vec![format!("evidence:{native_ref}:{source_revision}")],
        observed_at_ms: 120,
        policy_labels: BTreeSet::from(["tenant-a".into()]),
        grant_epoch: 4,
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "observed".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
        continuation: None,
        actual_rows: 1,
        actual_bytes: payload.len() as u64,
        actual_calls: 1,
        actual_fanout: 1,
        actual_latency_ms: 20,
        actual_cost_units: 1,
    }
}

fn concurrent_submit(
    engine: Arc<Mutex<DataViewEngine>>,
    requests: [ActionResultSubmission; 2],
) -> [DataViewCommitOutcome; 2] {
    let barrier = Arc::new(Barrier::new(3));
    let handles = requests.map(|request| {
        let engine = engine.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            engine
                .lock()
                .expect("engine lock")
                .submit_action_result(request, 120, "policy-a")
        })
    });
    barrier.wait();
    handles.map(|handle| handle.join().expect("submission thread"))
}

// @verify_spec("CONTRACTS.DATA_VIEW.CONCURRENT_REVISION_SAFETY", mode="system")
#[test]
fn concurrent_disjoint_stale_base_submissions_do_not_lose_updates() {
    let mut engine = engine();
    engine
        .register_action(action("action:a"))
        .expect("action a");
    engine
        .register_action(action("action:b"))
        .expect("action b");
    let base = engine.head_ref();
    let context_a = engine
        .issue_action_context(
            "action:a",
            base.clone(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context a");
    let context_b = engine
        .issue_action_context(
            "action:b",
            base,
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context b");
    let shared = Arc::new(Mutex::new(engine));
    let outcomes = concurrent_submit(
        shared.clone(),
        [
            submission(
                context_a,
                "submission:a",
                "reports/a.json",
                "rev-1",
                br#"{"amount":10}"#,
            ),
            submission(
                context_b,
                "submission:b",
                "reports/b.json",
                "rev-1",
                br#"{"amount":20}"#,
            ),
        ],
    );

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.status == DataViewCommitStatus::Accepted)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.status == DataViewCommitStatus::SafeMerged)
            .count(),
        1
    );
    let engine = shared.lock().expect("engine lock");
    assert_eq!(engine.store().metrics().revision_count, 3);
    assert_eq!(
        engine
            .store()
            .snapshot(&engine.head_ref())
            .expect("head snapshot")
            .selected_payload_versions
            .len(),
        2
    );
}

#[test]
fn concurrent_conflicting_observations_retain_versions_and_select_deterministically() {
    let mut engine = engine();
    engine.register_action(action("action:a")).expect("action");
    let base = engine.head_ref();
    let context_a = engine
        .issue_action_context(
            "action:a",
            base.clone(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context a");
    let context_b = engine
        .issue_action_context(
            "action:a",
            base,
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context b");
    let shared = Arc::new(Mutex::new(engine));
    let outcomes = concurrent_submit(
        shared.clone(),
        [
            submission(
                context_a,
                "submission:rev-1",
                "reports/a.json",
                "rev-1",
                br#"{"amount":10}"#,
            ),
            submission(
                context_b,
                "submission:rev-2",
                "reports/a.json",
                "rev-2",
                br#"{"amount":11}"#,
            ),
        ],
    );

    assert!(
        outcomes
            .iter()
            .any(|outcome| outcome.conflict_version_refs.len() == 2)
    );
    let engine = shared.lock().expect("engine lock");
    assert_eq!(engine.store().metrics().payload_version_count, 2);
    let head = engine
        .store()
        .snapshot(&engine.head_ref())
        .expect("head snapshot");
    let selected = head
        .selected_payload_versions
        .values()
        .next()
        .expect("selected version");
    assert_eq!(
        engine
            .store()
            .payload_version(selected)
            .expect("payload version")
            .source_revision,
        "rev-2"
    );
}

// @verify_spec("CONTRACTS.DATA_VIEW.EVIDENCE_GATED_ATOMIC_COMMIT", mode="system")
#[test]
fn concurrent_duplicate_submission_commits_exactly_once() {
    let mut engine = engine();
    engine.register_action(action("action:a")).expect("action");
    let context = engine
        .issue_action_context(
            "action:a",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context");
    let request = submission(
        context,
        "submission:duplicate",
        "reports/a.json",
        "rev-1",
        br#"{"amount":10}"#,
    );
    let shared = Arc::new(Mutex::new(engine));
    let outcomes = concurrent_submit(shared.clone(), [request.clone(), request]);

    assert_eq!(outcomes[0], outcomes[1]);
    let engine = shared.lock().expect("engine lock");
    assert_eq!(engine.store().metrics().revision_count, 2);
    assert_eq!(engine.store().metrics().payload_version_count, 1);
    assert_eq!(engine.store().metrics().attachment_count, 1);
}

fn materialize_one(engine: &mut DataViewEngine) -> TrackedResultRef {
    engine
        .register_action(action("action:seed"))
        .expect("seed action");
    let context = engine
        .issue_action_context(
            "action:seed",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("seed context");
    let outcome = engine.submit_action_result(
        submission(
            context,
            "submission:seed",
            "reports/a.json",
            "rev-1",
            br#"{"amount":10}"#,
        ),
        120,
        "policy-a",
    );
    engine
        .store()
        .snapshot(&outcome.data_view)
        .expect("seed snapshot")
        .selected_payload_versions
        .keys()
        .next()
        .cloned()
        .expect("seed result")
}

fn create_subscription(engine: &mut DataViewEngine, tracked_result: &TrackedResultRef) -> String {
    engine
        .create_streaming_subscription(CreateStreamingSubscription {
            wire_version: DATA_VIEW_WIRE_VERSION,
            tracked_result_refs: BTreeSet::from([tracked_result.clone()]),
            source_authority_ref: authority(),
            observation_path: ObservationPath::Direct,
            allowed_schema_versions: BTreeSet::from([schema()]),
            source_watermark: "wm-0".into(),
            max_events_per_batch: 4,
            max_bytes_per_batch: 4096,
            max_batches_per_window: 4,
            batch_window_ms: 100,
            max_in_flight: 1,
            lease_issued_at_ms: 120,
            lease_expires_at_ms: 1000,
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
        })
        .expect("subscription")
        .subscription_id
}

fn stream_submission(
    subscription_id: &str,
    tracked_result: &TrackedResultRef,
    submission_id: &str,
    batch_id: &str,
    resulting_watermark: &str,
    operation: StreamingRefreshOperation,
) -> StreamingRefreshSubmission {
    let payload =
        (operation == StreamingRefreshOperation::Refresh).then(|| br#"{"amount":12}"#.to_vec());
    StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: submission_id.into(),
        subscription_id: subscription_id.into(),
        batch_id: batch_id.into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: resulting_watermark.into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked_result.clone(),
            operation,
            payload: payload.clone(),
            native_anchors: vec![NativeAnchor {
                anchor_kind: "s3-object".into(),
                native_ref: "reports/a.json".into(),
                native_pointer: Some("s3://finance/reports/a.json".into()),
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec![format!("evidence:{submission_id}")],
            observed_at_ms: 130,
        }],
        projection_evidence: None,
        actual_bytes: payload.as_ref().map_or(0, |value| value.len() as u64),
        actual_events: 1,
    }
}

fn concurrent_stream_submit(
    engine: Arc<Mutex<DataViewEngine>>,
    requests: [StreamingRefreshSubmission; 2],
) -> [DataViewCommitOutcome; 2] {
    let barrier = Arc::new(Barrier::new(3));
    let handles = requests.map(|request| {
        let engine = engine.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            engine
                .lock()
                .expect("engine lock")
                .submit_stream_refresh(request, 130, "policy-a", 4)
        })
    });
    barrier.wait();
    handles.map(|handle| handle.join().expect("stream thread"))
}

// @verify_spec("CONTRACTS.DATA_VIEW.STREAMING_SUBSCRIPTION_BOUNDARY", mode="system")
#[test]
fn concurrent_stream_batches_advance_exactly_one_watermark() {
    let mut engine = engine();
    let tracked = materialize_one(&mut engine);
    let subscription_id = create_subscription(&mut engine, &tracked);
    let shared = Arc::new(Mutex::new(engine));
    let outcomes = concurrent_stream_submit(
        shared.clone(),
        [
            stream_submission(
                &subscription_id,
                &tracked,
                "submission:stream:a",
                "batch:a",
                "wm-a",
                StreamingRefreshOperation::Refresh,
            ),
            stream_submission(
                &subscription_id,
                &tracked,
                "submission:stream:b",
                "batch:b",
                "wm-b",
                StreamingRefreshOperation::Refresh,
            ),
        ],
    );

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.status == DataViewCommitStatus::Accepted)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.status == DataViewCommitStatus::RequiresRevalidation)
            .count(),
        1
    );
    let accepted = outcomes
        .iter()
        .find(|outcome| outcome.status == DataViewCommitStatus::Accepted)
        .expect("accepted batch");
    let expected_watermark = if accepted.submission_id.ends_with(":a") {
        "wm-a"
    } else {
        "wm-b"
    };
    let engine = shared.lock().expect("engine lock");
    let subscription = engine
        .streaming_registry()
        .subscription(&subscription_id)
        .expect("subscription");
    assert_eq!(subscription.next_sequence, 2);
    assert_eq!(subscription.source_watermark, expected_watermark);
    assert_eq!(
        subscription.status,
        StreamingSubscriptionStatus::RequiresRevalidation
    );
    assert_eq!(engine.store().metrics().revision_count, 3);
    assert_eq!(engine.store().metrics().payload_version_count, 2);
}

// @verify_spec("CONTRACTS.DATA_VIEW.REALTIME_EXISTING_RESULT_FRESHNESS", mode="system")
#[test]
fn concurrent_refresh_and_invalidation_leave_only_the_committed_winner() {
    let mut engine = engine();
    let tracked = materialize_one(&mut engine);
    let subscription_id = create_subscription(&mut engine, &tracked);
    let shared = Arc::new(Mutex::new(engine));
    let outcomes = concurrent_stream_submit(
        shared.clone(),
        [
            stream_submission(
                &subscription_id,
                &tracked,
                "submission:refresh",
                "batch:refresh",
                "wm-refresh",
                StreamingRefreshOperation::Refresh,
            ),
            stream_submission(
                &subscription_id,
                &tracked,
                "submission:invalidate",
                "batch:invalidate",
                "wm-invalidate",
                StreamingRefreshOperation::Invalidate,
            ),
        ],
    );

    let accepted = outcomes
        .iter()
        .find(|outcome| outcome.status == DataViewCommitStatus::Accepted)
        .expect("accepted batch");
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.status == DataViewCommitStatus::RequiresRevalidation)
            .count(),
        1
    );
    let engine = shared.lock().expect("engine lock");
    let selected = &engine
        .store()
        .snapshot(&engine.head_ref())
        .expect("head snapshot")
        .selected_payload_versions;
    if accepted.submission_id == "submission:refresh" {
        assert!(selected.contains_key(&tracked));
        assert_eq!(engine.store().metrics().invalidation_count, 0);
    } else {
        assert!(!selected.contains_key(&tracked));
        assert_eq!(engine.store().metrics().invalidation_count, 1);
    }
    assert_eq!(engine.store().metrics().revision_count, 3);
}
