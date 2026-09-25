use std::collections::BTreeSet;

use capanix_host_fs_types::UnixStat;
use data_view::{
    BoundedSourceAction, ContinuationPolicy, CreateStreamingSubscription, DATA_VIEW_WIRE_VERSION,
    DataViewCommitStatus, DataViewEngine, DerivationEvidence, DiscoveryOperation, FailureMode,
    FreshnessState, ObservationPath, ProjectionMode, SchemaVersionRef, SideEffectLevel,
    SourceAuthorityRef,
};
use data_view_normalizers::{
    FsMetaActionSubmissionContext, map_fs_meta_direct_submission,
    map_fs_meta_projection_submission, map_fs_meta_stream_submission, register_builtin_normalizers,
    source_projection_ref_from_fs_meta,
};
use fs_meta::fustor_integration::{
    FsMetaAuthorityCursor, FsMetaAuthorityEpochEvidence, FsMetaExportLimits,
    FsMetaProjectionCoverage, FsMetaProjectionCoverageState, FsMetaProjectionCursor,
    FsMetaProjectionReadiness, FsMetaStreamingChange, FsMetaTrackedResultBinding,
    export_direct_result, export_projection_result, export_tracked_stream_batch,
};
use fs_meta::query::{
    ForceFindQueryPayload, MaterializedQueryPayload, QueryScope, ReadClass, SubtreeStats,
};
use fs_meta::shared_types::FileMetaRecord;

fn authority_ref() -> SourceAuthorityRef {
    SourceAuthorityRef("source:fs-meta:nfs1".into())
}

fn authority_cursor() -> FsMetaAuthorityCursor {
    FsMetaAuthorityCursor {
        source_authority_ref: authority_ref().0,
        change_id: "change:42".into(),
        source_revision: "source-pit:42".into(),
        authority_epoch: FsMetaAuthorityEpochEvidence {
            roots_signature: "roots:1".into(),
            grants_signature: "grants:7".into(),
            source_stream_generation: Some(3),
            journal_sequence: 42,
        },
        observed_at_us: 120_000,
    }
}

fn scope() -> QueryScope {
    QueryScope {
        path: b"/reports".to_vec(),
        recursive: true,
        max_depth: Some(2),
        selected_group: Some("nfs1".into()),
    }
}

fn stats() -> SubtreeStats {
    SubtreeStats {
        total_nodes: 3,
        total_files: 2,
        total_dirs: 1,
        total_size: 42,
        attested_count: 3,
        blind_spot_count: 0,
        latest_file_mtime_us: Some(100),
    }
}

fn action(action_id: &str, observation_path: ObservationPath) -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: action_id.into(),
        source_authority_ref: authority_ref(),
        observation_path,
        operation: DiscoveryOperation::TreeProbe,
        permission_scope: "fs-meta:nfs1:/reports".into(),
        freshness_precondition: FreshnessState::Fresh,
        max_rows: 10,
        max_bytes: 64 * 1024,
        max_calls: 1,
        max_fanout: 1,
        max_latency_ms: 1000,
        max_cost_units: 4,
        side_effect_level: SideEffectLevel::ReadOnly,
        continuation_policy: ContinuationPolicy::None,
        failure_modes: vec![FailureMode::NotDisclosed],
        expansion_rules: vec![],
        expected_information_gain: 80,
        explanation: "read bounded fs-meta scope".into(),
    }
}

fn context(
    execution: data_view::ActionExecutionContext,
    submission_id: &str,
    producer_id: &str,
) -> FsMetaActionSubmissionContext {
    FsMetaActionSubmissionContext {
        submission_id: submission_id.into(),
        execution,
        producer_id: producer_id.into(),
        policy_labels: BTreeSet::from(["tenant-a".into()]),
        grant_epoch: 4,
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "observed".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
        continuation: None,
        actual_calls: 1,
        actual_fanout: 1,
        actual_latency_ms: 10,
        actual_cost_units: 1,
    }
}

#[test]
fn fs_meta_direct_and_projection_exports_commit_through_generic_fustor_contracts() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    register_builtin_normalizers(engine.normalizer_registry_mut()).expect("normalizers");

    engine
        .register_action(action("action:direct", ObservationPath::Direct))
        .expect("direct action");
    let direct_context = engine
        .issue_action_context(
            "action:direct",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("direct context");
    let direct_export = export_direct_result(
        ForceFindQueryPayload::Stats(stats()),
        &scope(),
        authority_cursor(),
        &FsMetaExportLimits {
            max_rows: 10,
            max_payload_bytes: 64 * 1024,
        },
    )
    .expect("direct export");
    let direct_submission = map_fs_meta_direct_submission(
        context(direct_context, "submission:direct", "fs-meta:source"),
        direct_export,
    )
    .expect("direct mapping");
    assert!(
        direct_submission
            .evidence_refs
            .iter()
            .any(|value| value == "fs-meta:journal-sequence:42")
    );
    let direct = engine.submit_action_result(direct_submission, 120, "policy-a");
    assert_eq!(direct.status, DataViewCommitStatus::Accepted);
    let direct_result_ref = engine
        .store()
        .snapshot(&direct.data_view)
        .expect("direct snapshot")
        .selected_payload_versions
        .keys()
        .next()
        .cloned()
        .expect("direct result");

    let projection_export = export_projection_result(
        MaterializedQueryPayload::Stats(stats()),
        &scope(),
        FsMetaProjectionCursor {
            authority: authority_cursor(),
            projection_ref: "projection:fs-meta:nfs1".into(),
            projection_revision: "materialized:9".into(),
            applied_change_id: "change:42".into(),
            coverage: FsMetaProjectionCoverage {
                state: FsMetaProjectionCoverageState::CompleteForScope,
                scope_ref: "nfs1:/reports".into(),
                limiting_reasons: vec![],
            },
            readiness: FsMetaProjectionReadiness::Ready,
            read_class: ReadClass::TrustedMaterialized,
        },
        &FsMetaExportLimits {
            max_rows: 10,
            max_payload_bytes: 64 * 1024,
        },
    )
    .expect("projection export");
    let projection_ref = source_projection_ref_from_fs_meta(&projection_export.projection_evidence)
        .expect("projection ref");
    engine
        .register_action(action(
            "action:projection",
            ObservationPath::ProjectionBacked {
                projection: projection_ref,
            },
        ))
        .expect("projection action");
    let projection_context = engine
        .issue_action_context(
            "action:projection",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            121,
            220,
        )
        .expect("projection context");
    let projection_submission = map_fs_meta_projection_submission(
        context(
            projection_context,
            "submission:projection",
            "fs-meta:projection:nfs1",
        ),
        projection_export,
    )
    .expect("projection mapping");
    let projection = engine.submit_action_result(projection_submission, 130, "policy-a");
    assert_eq!(projection.status, DataViewCommitStatus::Accepted);
    let projection_snapshot = engine
        .store()
        .snapshot(&projection.data_view)
        .expect("projection snapshot");
    assert_eq!(projection_snapshot.selected_payload_versions.len(), 1);
    assert_eq!(
        projection_snapshot.selected_payload_versions.keys().next(),
        Some(&direct_result_ref),
        "direct and projection-backed paths must preserve one source-native identity"
    );
    assert_eq!(engine.store().metrics().payload_version_count, 2);

    let subscription = engine
        .create_streaming_subscription(CreateStreamingSubscription {
            wire_version: DATA_VIEW_WIRE_VERSION,
            tracked_result_refs: BTreeSet::from([direct_result_ref.clone()]),
            source_authority_ref: authority_ref(),
            observation_path: ObservationPath::Direct,
            allowed_schema_versions: BTreeSet::from([SchemaVersionRef {
                schema_ref: data_view_normalizers::FS_META_DIRECT_SCHEMA_REF.into(),
                schema_version: data_view_normalizers::BUILTIN_SCHEMA_VERSION.into(),
            }]),
            source_watermark: "journal:42".into(),
            max_events_per_batch: 4,
            max_bytes_per_batch: 64 * 1024,
            max_batches_per_window: 4,
            batch_window_ms: 100,
            max_in_flight: 1,
            lease_issued_at_ms: 130,
            lease_expires_at_ms: 1000,
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
        })
        .expect("stream subscription");
    let stat = UnixStat {
        is_dir: true,
        ..UnixStat::default()
    };
    let batch = export_tracked_stream_batch(
        subscription.subscription_id.clone(),
        "batch:43",
        1,
        "journal:42",
        "journal:43",
        &[FsMetaTrackedResultBinding {
            tracked_result_ref: direct_result_ref.0.clone(),
            native_path: b"/reports".to_vec(),
        }],
        vec![FsMetaStreamingChange {
            record: FileMetaRecord::realtime_update(
                b"/reports".to_vec(),
                b"reports".to_vec(),
                stat,
                true,
            ),
            change_id: "change:43".into(),
            source_revision: "journal:43".into(),
            authority_epoch: FsMetaAuthorityEpochEvidence {
                journal_sequence: 43,
                ..authority_cursor().authority_epoch
            },
            observed_at_us: 140_000,
        }],
        None,
        4,
        64 * 1024,
    )
    .expect("stream export")
    .expect("tracked stream batch");
    let stream_submission =
        map_fs_meta_stream_submission("submission:stream:43", batch).expect("stream mapping");
    let streamed = engine.submit_stream_refresh(stream_submission, 140, "policy-a", 4);
    assert_eq!(streamed.status, DataViewCommitStatus::Accepted);
    assert_eq!(
        engine
            .streaming_registry()
            .subscription(&subscription.subscription_id)
            .expect("advanced subscription")
            .source_watermark,
        "journal:43"
    );
}
