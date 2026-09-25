use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use data_view::{
    ActionResultSubmission, BoundedSourceAction, ContinuationPolicy, CoverageState,
    CreateStreamingSubscription, DATA_VIEW_WIRE_VERSION, DataViewCommitStatus, DataViewEngine,
    DerivationEvidence, DiscoveryOperation, FailureMode, FreshnessState, NativeAnchor,
    ObservationPath, PayloadEncoding, ProducerKind, ProjectionApplyEvidence, ProjectionCoverage,
    ProjectionMode, RetentionPolicy, SchemaVersionRef, SideEffectLevel, SingleResultNormalizer,
    SourceAuthorityRef, SourceProjectionRef, StreamingRefreshEntry, StreamingRefreshOperation,
    StreamingRefreshSubmission, TrackedResultRef,
};

fn authority() -> SourceAuthorityRef {
    SourceAuthorityRef::new("source:s3:finance").expect("authority")
}

fn schema() -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: "fustor.test.object".into(),
        schema_version: "1".into(),
    }
}

fn action(action_id: &str, path: ObservationPath) -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: action_id.into(),
        source_authority_ref: authority(),
        observation_path: path,
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

fn projection_ref() -> SourceProjectionRef {
    SourceProjectionRef {
        projection_id: "projection:finance-index".into(),
        source_authority_ref: authority(),
        projection_kind: "materialized-index".into(),
    }
}

fn projection_evidence(source_revision: &str) -> ProjectionApplyEvidence {
    ProjectionApplyEvidence {
        projection: projection_ref(),
        source_revision: source_revision.into(),
        projection_revision: "projection-rev-10".into(),
        applied_change_id: "change-10".into(),
        coverage: ProjectionCoverage {
            state: CoverageState::CompleteForScope,
            scope_ref: "tenant-a:finance".into(),
            limiting_reasons: vec![],
        },
        freshness: FreshnessState::Fresh,
        observed_at_ms: 120,
        evidence_refs: vec!["evidence:projection:10".into()],
    }
}

fn submission(
    context: data_view::ActionExecutionContext,
    submission_id: &str,
    native_ref: &str,
    source_revision: &str,
    payload: &[u8],
    projection_evidence: Option<ProjectionApplyEvidence>,
) -> ActionResultSubmission {
    ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: submission_id.into(),
        execution: context,
        source_authority_ref: authority(),
        authority_object_ref: authority().0,
        producer_id: if projection_evidence.is_some() {
            "projection:finance-index".into()
        } else {
            "s3-source:finance".into()
        },
        producer_kind: if projection_evidence.is_some() {
            ProducerKind::SourceProjection
        } else {
            ProducerKind::Source
        },
        projection_evidence,
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

fn engine() -> DataViewEngine {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    engine
}

fn materialize_one(engine: &mut DataViewEngine) -> TrackedResultRef {
    engine
        .register_action(action("action:seed", ObservationPath::Direct))
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
            None,
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

#[test]
fn accepted_action_commit_preserves_complete_payload_and_is_idempotent() {
    let mut engine = engine();
    engine
        .register_action(action("action:a", ObservationPath::Direct))
        .expect("action");
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
        "submission:a",
        "reports/a.json",
        "rev-1",
        br#"{"amount":10}"#,
        None,
    );

    let outcome = engine.submit_action_result(request.clone(), 120, "policy-a");
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    let metrics = engine.store().metrics();
    assert_eq!(metrics.revision_count, 2);
    assert_eq!(metrics.attachment_count, 1);
    assert_eq!(metrics.payload_version_count, 1);

    let snapshot = engine
        .store()
        .snapshot(&outcome.data_view)
        .expect("snapshot");
    let version_ref = snapshot
        .selected_payload_versions
        .values()
        .next()
        .expect("selected payload");
    let version = engine
        .store()
        .payload_version(version_ref)
        .expect("payload version");
    let attachment = engine
        .store()
        .attachment(&version.attachment_ref)
        .expect("attachment");
    assert_eq!(attachment.payload, br#"{"amount":10}"#);
    assert_eq!(version.source_revision, "rev-1");
    assert_eq!(version.evidence_refs, vec!["evidence:reports/a.json:rev-1"]);

    let duplicate = engine.submit_action_result(request, 190, "policy-a");
    assert_eq!(duplicate, outcome);
    assert_eq!(engine.store().metrics(), metrics);
}

#[test]
fn public_revision_ids_are_fixed_length_random_opaque_tokens() {
    let mut engine = engine();
    let genesis = engine.head_ref();
    materialize_one(&mut engine);
    let committed = engine.head_ref();

    for revision in [&genesis, &committed] {
        let token = revision
            .revision_id
            .strip_prefix("revision:")
            .expect("revision prefix");
        assert_eq!(token.len(), 48);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(token, "1");
        assert_ne!(token, "2");
    }
    assert_ne!(genesis.revision_id, committed.revision_id);
}

#[test]
fn rejected_submission_leaves_head_and_append_only_counts_unchanged() {
    let mut engine = engine();
    engine
        .register_action(action("action:a", ObservationPath::Direct))
        .expect("action");
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
    let mut request = submission(
        context,
        "submission:a",
        "reports/a.json",
        "rev-1",
        br#"{"amount":10}"#,
        None,
    );
    request.actual_bytes += 1;
    let head_before = engine.head_ref();
    let metrics_before = engine.store().metrics();

    let outcome = engine.submit_action_result(request, 120, "policy-a");
    assert_eq!(outcome.status, DataViewCommitStatus::Rejected);
    assert_eq!(outcome.data_view, head_before);
    assert_eq!(engine.head_ref(), head_before);
    assert_eq!(engine.store().metrics(), metrics_before);
}

#[test]
fn disjoint_stale_base_submission_is_safe_merged() {
    let mut engine = engine();
    engine
        .register_action(action("action:a", ObservationPath::Direct))
        .expect("action a");
    engine
        .register_action(action("action:b", ObservationPath::Direct))
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

    let first = engine.submit_action_result(
        submission(
            context_a,
            "submission:a",
            "reports/a.json",
            "rev-1",
            br#"{"amount":10}"#,
            None,
        ),
        120,
        "policy-a",
    );
    assert_eq!(first.status, DataViewCommitStatus::Accepted);
    let second = engine.submit_action_result(
        submission(
            context_b,
            "submission:b",
            "reports/b.json",
            "rev-1",
            br#"{"amount":20}"#,
            None,
        ),
        121,
        "policy-a",
    );
    assert_eq!(second.status, DataViewCommitStatus::SafeMerged);
    assert_eq!(
        engine
            .store()
            .snapshot(&second.data_view)
            .expect("snapshot")
            .selected_payload_versions
            .len(),
        2
    );
}

#[test]
fn conflicting_stale_observations_retain_both_versions_without_last_write_wins() {
    let mut engine = engine();
    engine
        .register_action(action("action:a", ObservationPath::Direct))
        .expect("action");
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

    engine.submit_action_result(
        submission(
            context_a,
            "submission:a",
            "reports/a.json",
            "rev-1",
            br#"{"amount":10}"#,
            None,
        ),
        120,
        "policy-a",
    );
    let conflict = engine.submit_action_result(
        submission(
            context_b,
            "submission:b",
            "reports/a.json",
            "rev-2",
            br#"{"amount":11}"#,
            None,
        ),
        121,
        "policy-a",
    );
    assert_eq!(conflict.status, DataViewCommitStatus::Accepted);
    assert_eq!(conflict.conflict_version_refs.len(), 2);
    assert_eq!(engine.store().metrics().payload_version_count, 2);
    assert_eq!(
        engine
            .store()
            .snapshot(&conflict.data_view)
            .expect("snapshot")
            .selected_payload_versions
            .len(),
        1
    );
}

#[test]
fn deterministic_head_selection_is_independent_of_conflict_arrival_order() {
    fn selected_revision(reverse: bool) -> String {
        let mut engine = engine();
        engine
            .register_action(action("action:a", ObservationPath::Direct))
            .expect("action");
        let base = engine.head_ref();
        let first_context = engine
            .issue_action_context(
                "action:a",
                base.clone(),
                ProjectionMode::CurrentActionable,
                "policy-a",
                4,
                100,
                200,
            )
            .expect("first context");
        let second_context = engine
            .issue_action_context(
                "action:a",
                base,
                ProjectionMode::CurrentActionable,
                "policy-a",
                4,
                100,
                200,
            )
            .expect("second context");
        let revision_one = submission(
            first_context,
            "submission:rev-1",
            "reports/a.json",
            "rev-1",
            br#"{"amount":10}"#,
            None,
        );
        let revision_two = submission(
            second_context,
            "submission:rev-2",
            "reports/a.json",
            "rev-2",
            br#"{"amount":11}"#,
            None,
        );
        let submissions = if reverse {
            [revision_two, revision_one]
        } else {
            [revision_one, revision_two]
        };
        for request in submissions {
            engine.submit_action_result(request, 120, "policy-a");
        }
        let head = engine.head_ref();
        let snapshot = engine.store().snapshot(&head).expect("snapshot");
        let selected = snapshot
            .selected_payload_versions
            .values()
            .next()
            .expect("selected version");
        engine
            .store()
            .payload_version(selected)
            .expect("payload version")
            .source_revision
            .clone()
    }

    assert_eq!(selected_revision(false), "rev-2");
    assert_eq!(selected_revision(true), "rev-2");
}

#[test]
fn projection_backed_action_never_falls_back_to_direct() {
    let mut engine = engine();
    engine
        .register_action(action(
            "action:projection",
            ObservationPath::ProjectionBacked {
                projection: projection_ref(),
            },
        ))
        .expect("action");
    let context = engine
        .issue_action_context(
            "action:projection",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context");
    let missing = submission(
        context.clone(),
        "submission:missing",
        "reports/a.json",
        "rev-1",
        br#"{"amount":10}"#,
        None,
    );
    let head_before = engine.head_ref();
    let rejected = engine.submit_action_result(missing, 120, "policy-a");
    assert_eq!(rejected.status, DataViewCommitStatus::Rejected);
    assert_eq!(engine.head_ref(), head_before);

    let accepted = engine.submit_action_result(
        submission(
            context,
            "submission:projection",
            "reports/a.json",
            "rev-1",
            br#"{"amount":10}"#,
            Some(projection_evidence("rev-1")),
        ),
        121,
        "policy-a",
    );
    assert_eq!(accepted.status, DataViewCommitStatus::Accepted);
}

#[test]
fn streaming_refresh_is_atomic_idempotent_and_cannot_discover_new_results() {
    let mut engine = engine();
    let tracked_result = materialize_one(&mut engine);
    let subscription = engine
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
        .expect("subscription");
    let refreshed_payload = br#"{"amount":12}"#.to_vec();
    let refresh = StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:stream:1".into(),
        subscription_id: subscription.subscription_id.clone(),
        batch_id: "batch:1".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked_result.clone(),
            operation: StreamingRefreshOperation::Refresh,
            payload: Some(refreshed_payload.clone()),
            native_anchors: vec![NativeAnchor {
                anchor_kind: "s3-object".into(),
                native_ref: "reports/a.json".into(),
                native_pointer: Some("s3://finance/reports/a.json".into()),
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec!["evidence:reports/a.json:rev-2".into()],
            observed_at_ms: 130,
        }],
        projection_evidence: None,
        actual_bytes: refreshed_payload.len() as u64,
        actual_events: 1,
    };

    let refreshed = engine.submit_stream_refresh(refresh.clone(), 130, "policy-a", 4);
    assert_eq!(refreshed.status, DataViewCommitStatus::Accepted);
    assert_eq!(engine.store().metrics().revision_count, 3);
    assert_eq!(engine.store().metrics().payload_version_count, 2);
    let refreshed_snapshot = engine
        .store()
        .snapshot(&refreshed.data_view)
        .expect("refreshed snapshot");
    let version = engine
        .store()
        .payload_version(
            refreshed_snapshot
                .selected_payload_versions
                .get(&tracked_result)
                .expect("selected refreshed version"),
        )
        .expect("refreshed version");
    assert_eq!(
        engine
            .store()
            .attachment(&version.attachment_ref)
            .expect("refreshed attachment")
            .payload,
        refreshed_payload
    );
    let metrics_after_refresh = engine.store().metrics();
    let duplicate = engine.submit_stream_refresh(refresh.clone(), 131, "policy-a", 4);
    assert_eq!(duplicate, refreshed);
    assert_eq!(engine.store().metrics(), metrics_after_refresh);

    let mut unknown = refresh;
    unknown.submission_id = "submission:stream:2".into();
    unknown.batch_id = "batch:2".into();
    unknown.sequence = 2;
    unknown.previous_watermark = "wm-1".into();
    unknown.resulting_watermark = "wm-2".into();
    unknown.refreshes[0].tracked_result_ref = TrackedResultRef("result:new".into());
    let head_before_unknown = engine.head_ref();
    let rejected = engine.submit_stream_refresh(unknown, 132, "policy-a", 4);
    assert_eq!(rejected.status, DataViewCommitStatus::Rejected);
    assert_eq!(engine.head_ref(), head_before_unknown);
    assert_eq!(engine.store().metrics(), metrics_after_refresh);
}

#[test]
fn streaming_invalidation_removes_actionability_but_retains_history() {
    let mut engine = engine();
    let tracked_result = materialize_one(&mut engine);
    let subscription = engine
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
        .expect("subscription");
    let invalidation = StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:invalidate:1".into(),
        subscription_id: subscription.subscription_id,
        batch_id: "batch:invalidate:1".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked_result,
            operation: StreamingRefreshOperation::Invalidate,
            payload: None,
            native_anchors: vec![NativeAnchor {
                anchor_kind: "s3-object".into(),
                native_ref: "reports/a.json".into(),
                native_pointer: Some("s3://finance/reports/a.json".into()),
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec!["evidence:invalidate:rev-2".into()],
            observed_at_ms: 130,
        }],
        projection_evidence: None,
        actual_bytes: 0,
        actual_events: 1,
    };

    let outcome = engine.submit_stream_refresh(invalidation, 130, "policy-a", 4);
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    assert!(
        engine
            .store()
            .snapshot(&outcome.data_view)
            .expect("snapshot")
            .selected_payload_versions
            .is_empty()
    );
    assert_eq!(engine.store().metrics().payload_version_count, 1);
    assert_eq!(engine.store().metrics().invalidation_count, 1);
}

#[test]
fn streaming_delete_removes_tracked_result_and_records_deleted_delta() {
    let mut engine = engine();
    let tracked_result = materialize_one(&mut engine);
    let subscription = engine
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
        .expect("subscription");
    let deletion = StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:delete:1".into(),
        subscription_id: subscription.subscription_id,
        batch_id: "batch:delete:1".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked_result.clone(),
            operation: StreamingRefreshOperation::Delete,
            payload: None,
            native_anchors: vec![NativeAnchor {
                anchor_kind: "s3-object".into(),
                native_ref: "reports/a.json".into(),
                native_pointer: Some("s3://finance/reports/a.json".into()),
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec!["evidence:delete:rev-2".into()],
            observed_at_ms: 130,
        }],
        projection_evidence: None,
        actual_bytes: 0,
        actual_events: 1,
    };

    let outcome = engine.submit_stream_refresh(deletion, 130, "policy-a", 4);
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    assert!(
        engine
            .store()
            .snapshot(&outcome.data_view)
            .expect("snapshot")
            .selected_payload_versions
            .is_empty()
    );
    let delta = engine
        .store()
        .graph_delta(outcome.delta_ref.as_deref().expect("delta ref"))
        .expect("delta");
    assert_eq!(delta.deleted_result_refs, vec![tracked_result]);
    assert!(delta.invalidated_result_refs.is_empty());
    assert_eq!(engine.store().metrics().invalidation_count, 1);
}

#[test]
fn source_invalidation_propagates_to_transitive_derived_results() {
    let mut engine = engine();
    let source_result = materialize_one(&mut engine);
    engine
        .register_action(action("action:derived", ObservationPath::Direct))
        .expect("derived action");
    let context = engine
        .issue_action_context(
            "action:derived",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            121,
            220,
        )
        .expect("derived context");
    let mut derived_submission = submission(
        context,
        "submission:derived",
        "reports/summary.json",
        "rev-1",
        br#"{"total":10}"#,
        None,
    );
    derived_submission.derivation = DerivationEvidence {
        derivation_kind: "aggregate".into(),
        rule_ref: Some("rule:sum".into()),
        dependency_refs: vec![source_result.0.clone()],
    };
    let derived = engine.submit_action_result(derived_submission, 130, "policy-a");
    assert_eq!(derived.status, DataViewCommitStatus::Accepted);
    assert_eq!(
        engine
            .store()
            .snapshot(&derived.data_view)
            .expect("derived snapshot")
            .selected_payload_versions
            .len(),
        2
    );

    let subscription = engine
        .create_streaming_subscription(CreateStreamingSubscription {
            wire_version: DATA_VIEW_WIRE_VERSION,
            tracked_result_refs: BTreeSet::from([source_result.clone()]),
            source_authority_ref: authority(),
            observation_path: ObservationPath::Direct,
            allowed_schema_versions: BTreeSet::from([schema()]),
            source_watermark: "wm-0".into(),
            max_events_per_batch: 4,
            max_bytes_per_batch: 4096,
            max_batches_per_window: 4,
            batch_window_ms: 100,
            max_in_flight: 1,
            lease_issued_at_ms: 130,
            lease_expires_at_ms: 1000,
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
        })
        .expect("subscription");
    let invalidation = StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:dependency-invalidate".into(),
        subscription_id: subscription.subscription_id,
        batch_id: "batch:dependency-invalidate".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: source_result,
            operation: StreamingRefreshOperation::Invalidate,
            payload: None,
            native_anchors: vec![NativeAnchor {
                anchor_kind: "s3-object".into(),
                native_ref: "reports/a.json".into(),
                native_pointer: Some("s3://finance/reports/a.json".into()),
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec!["evidence:invalidate:rev-2".into()],
            observed_at_ms: 140,
        }],
        projection_evidence: None,
        actual_bytes: 0,
        actual_events: 1,
    };
    let outcome = engine.submit_stream_refresh(invalidation, 140, "policy-a", 4);
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    assert!(
        engine
            .store()
            .snapshot(&outcome.data_view)
            .expect("invalidated snapshot")
            .selected_payload_versions
            .is_empty()
    );
    assert_eq!(engine.store().metrics().payload_version_count, 2);
    assert_eq!(engine.store().metrics().invalidation_count, 2);
}

fn retention_policy(now_ms: u64) -> RetentionPolicy {
    RetentionPolicy {
        now_ms,
        actionable_ttl_ms: 100,
        history_ttl_ms: 10_000,
        max_payload_versions: 100,
        max_payload_bytes: 1024 * 1024,
        source_actionable_ttl_ms: BTreeMap::new(),
        tenant_policy_labels_to_delete: BTreeSet::new(),
        compliance_delete_result_refs: BTreeSet::new(),
        retain_minimum_tombstones: true,
    }
}

#[test]
fn retention_makes_expired_results_non_actionable_before_history_eviction() {
    let mut engine = engine();
    materialize_one(&mut engine);
    let historical_ref = engine.head_ref();

    let first = engine
        .apply_retention(&retention_policy(1_000))
        .expect("retire expired result");
    assert!(
        engine
            .store()
            .snapshot(&first.data_view)
            .expect("current snapshot")
            .selected_payload_versions
            .is_empty()
    );
    assert!(engine.store().snapshot(&historical_ref).is_some());
    assert_eq!(engine.store().metrics().payload_version_count, 1);
    assert_eq!(engine.store().metrics().retention_record_count, 1);

    let mut eviction_policy = retention_policy(20_000);
    eviction_policy.history_ttl_ms = 100;
    let second = engine
        .apply_retention(&eviction_policy)
        .expect("evict history");
    assert_eq!(second.data_view, first.data_view);
    assert_eq!(engine.store().metrics().payload_version_count, 0);
    assert_eq!(engine.store().metrics().attachment_count, 0);
    assert!(engine.store().snapshot(&historical_ref).is_none());
}

#[test]
fn compliance_deletion_redacts_payload_and_keeps_only_minimum_tombstone() {
    let mut engine = engine();
    let result_ref = materialize_one(&mut engine);
    let historical_ref = engine.head_ref();
    let mut policy = retention_policy(130);
    policy.actionable_ttl_ms = 10_000;
    policy.history_ttl_ms = 10_000;
    policy
        .compliance_delete_result_refs
        .insert(result_ref.clone());

    let outcome = engine.apply_retention(&policy).expect("compliance delete");
    assert_eq!(outcome.retired_result_refs, vec![result_ref]);
    assert_eq!(engine.store().metrics().payload_version_count, 0);
    assert_eq!(engine.store().metrics().attachment_count, 0);
    assert_eq!(engine.store().metrics().tombstone_count, 1);
    assert!(engine.store().snapshot(&historical_ref).is_none());
    assert!(
        engine
            .store()
            .snapshot(&outcome.data_view)
            .expect("current snapshot")
            .selected_payload_versions
            .is_empty()
    );
}
