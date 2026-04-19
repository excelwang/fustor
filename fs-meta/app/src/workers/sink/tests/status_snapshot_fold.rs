#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_missing_group_rows_after_stream_evidence()
{
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "scheduled sink groups that already carry stream evidence must classify as missing-group-rows, not as healthy"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_missing_group_rows_after_stream_ready_path_evidence()
 {
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3:/srv/export=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "missing scheduled group rows must still classify from path-level ready evidence when only stream path counters survived the fold"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_missing_group_rows_after_stream_applied_path_evidence()
 {
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_applied_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3:/srv/export=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "missing scheduled group rows must still classify from path-level applied evidence when only stream path counters survived the fold"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_missing_group_rows_after_stream_received_evidence()
 {
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_received_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "missing scheduled group rows must still classify from stream-received evidence when ready/applied counters have not been populated yet"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_missing_group_rows_after_stream_received_path_evidence()
 {
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_received_path_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3:/srv/export=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "missing scheduled group rows must still classify from path-level stream-received evidence when only received path counters survived the fold"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_waiting_for_materialized_root() {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "unassigned".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledWaitingForMaterializedRoot),
        "scheduled groups that only expose zero/uninitialized rows must classify as waiting for a materialized root"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_mixed_ready_and_unready() {
    let snapshot = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-b::nfs1".to_string(),
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
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "unassigned".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready),
        "scheduled groups that split between ready and unready rows must classify as a mixed readiness regression"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_pending_materialization_without_stream_receipts_regardless_of_replay_required()
 {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
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
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot),
        Some(SinkStatusSnapshotIssue::ScheduledPendingMaterializationWithoutStreamReceipts),
        "scheduled zero-state pending-materialization snapshots must classify as pending-materialization without stream receipts even before replay_required is armed"
    );
}

#[test]
fn sink_group_status_readiness_distinguishes_waiting_for_materialized_root_from_pending_materialization()
 {
    let waiting_for_materialized_root = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs3".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,

        readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    };
    let pending_materialization = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs3".to_string(),
        primary_object_ref: "node-b::nfs3".to_string(),
        total_nodes: 6,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,

        readiness: crate::sink::GroupReadinessState::PendingMaterialization,
        materialized_revision: 7,
        estimated_heap_bytes: 1,
    };

    assert_eq!(
        waiting_for_materialized_root.normalized_readiness(),
        crate::sink::GroupReadinessState::WaitingForMaterializedRoot
    );
    assert_eq!(
        pending_materialization.normalized_readiness(),
        crate::sink::GroupReadinessState::PendingMaterialization
    );
}

#[test]
fn classify_sink_group_status_readiness_prefers_exported_group_readiness_over_legacy_fields() {
    let exported_waiting_for_materialized_root = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs3".to_string(),
        primary_object_ref: "node-b::nfs3".to_string(),
        total_nodes: 6,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,

        readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        materialized_revision: 7,
        estimated_heap_bytes: 1,
    };

    assert_eq!(
        exported_waiting_for_materialized_root.normalized_readiness(),
        crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        "workers/sink must consume the exported group readiness state directly instead of reconstructing a different readiness class from legacy snapshot fields"
    );
}

#[test]
fn classify_sink_group_status_readiness_treats_zero_row_with_placeholder_primary_as_waiting_for_materialized_root_even_when_exported_readiness_is_ready()
 {
    let exported_ready_zero_row = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 0,
        live_nodes: 0,
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
    };

    assert_eq!(
        exported_ready_zero_row.normalized_readiness(),
        crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        "structural zero rows with placeholder primary must override stale exported readiness=Ready and stay waiting-for-root"
    );
}

#[test]
fn classify_sink_group_status_readiness_treats_zero_row_with_bound_primary_as_pending_materialization_even_when_exported_readiness_is_ready()
 {
    let exported_ready_zero_row = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "node-d::nfs1".to_string(),
        total_nodes: 0,
        live_nodes: 0,
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
    };

    assert_eq!(
        exported_ready_zero_row.normalized_readiness(),
        crate::sink::GroupReadinessState::PendingMaterialization,
        "structural zero rows with a bound primary must override stale exported readiness=Ready and stay pending-materialization"
    );
}

#[test]
fn classify_sink_group_status_readiness_preserves_exported_waiting_for_materialized_root_for_zero_row_bound_primary()
 {
    let exported_waiting_zero_row = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "node-d::nfs1".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,

        readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    };

    assert_eq!(
        exported_waiting_zero_row.normalized_readiness(),
        crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        "workers/sink must preserve an exported waiting-for-materialized-root zero-row even when the primary is already bound; only stale exported readiness=Ready should be structurally downgraded"
    );
}

#[test]
fn classify_sink_group_status_readiness_normalizes_live_row_waiting_for_materialized_root_to_ready()
{
    let exported_waiting_live_row = crate::sink::SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 1,
        live_nodes: 1,
        tombstoned_count: 0,
        attested_count: 1,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,

        readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    };

    assert_eq!(
        exported_waiting_live_row.normalized_readiness(),
        crate::sink::GroupReadinessState::Ready,
        "a live materialized row must not stay structurally waiting-for-materialized-root just because exported readiness lagged behind the row-level live truth"
    );
}

#[test]
fn ready_groups_from_snapshot_prefers_exported_readiness_over_legacy_initial_audit_bool() {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-b::nfs1".to_string(),
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
            estimated_heap_bytes: 1,
        }],
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        snapshot.ready_groups(),
        std::collections::BTreeSet::from(["nfs1".to_string()]),
        "ready-group extraction must trust exported readiness over stale initial_audit_completed=false"
    );
}

#[test]
fn snapshot_looks_stale_empty_prefers_exported_readiness_over_legacy_initial_audit_bool() {
    let snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "unassigned".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    assert!(
        snapshot_looks_stale_empty(&snapshot),
        "stale-empty detection must treat exported waiting-for-root rows as unready even when legacy initial_audit_completed stayed true"
    );
}

#[test]
fn republish_scheduled_groups_into_zero_row_summary_prefers_exported_readiness_over_legacy_initial_audit_bool()
 {
    let mut snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "unassigned".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    republish_scheduled_groups_into_zero_row_summary(
        &mut snapshot,
        &NodeId("node-b".to_string()),
        &std::collections::BTreeSet::from(["nfs1".to_string()]),
    );

    assert_eq!(
        snapshot.scheduled_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "zero-row republish must trust exported readiness over stale initial_audit_completed=true"
    );
}

#[test]
fn republish_scheduled_groups_into_zero_row_summary_republishes_when_live_rows_cover_cached_schedule()
 {
    let mut snapshot = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-b::nfs1".to_string(),
                total_nodes: 2,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 4,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 3,
                estimated_heap_bytes: 749,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-b::nfs2".to_string(),
                total_nodes: 2,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 8,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 3,
                estimated_heap_bytes: 749,
            },
        ],
        ..SinkStatusSnapshot::default()
    };

    republish_scheduled_groups_into_zero_row_summary(
        &mut snapshot,
        &NodeId("node-b".to_string()),
        &std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
    );

    assert_eq!(
        snapshot.scheduled_groups_by_node,
        std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        "when live rows already cover the converged cached schedule, the republish helper must carry that schedule back into the status summary instead of leaving it empty"
    );
}

#[test]
fn fold_live_sink_status_snapshot_returns_cached_for_control_inflight_scheduled_zero_when_cached_ready_truth_survives()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
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
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "unassigned".to_string(),
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
        }],
        scheduled_groups_by_node: cached_snapshot.scheduled_groups_by_node.clone(),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        evaluate_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            None,
            SinkStatusAccessPath::ControlInflight,
        ),
        SinkStatusSnapshotOutcome {
            kind: SinkStatusOutcomeKind::ReturnCached,
            concern: Some(SinkStatusConcern::ReplayPending),
            should_mark_replay_required: true,
            should_republish_zero_row_summary: false,
        },
        "control-inflight live scheduled-zero regressions must fall back to cached ready truth instead of reopening the zero snapshot",
    );
}

#[test]
fn fold_live_sink_status_snapshot_fails_closed_for_blocking_missing_group_rows_after_stream_evidence()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        evaluate_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            None,
            SinkStatusAccessPath::Blocking,
        ),
        SinkStatusSnapshotOutcome {
            kind: SinkStatusOutcomeKind::FailClosed,
            concern: Some(SinkStatusConcern::CoverageGap),
            should_mark_replay_required: false,
            should_republish_zero_row_summary: false,
        },
        "blocking status truth must fail close on scheduled missing-group rows once stream evidence proves the schedule already existed instead of publishing a structurally incomplete live snapshot",
    );
}

#[test]
fn fold_live_sink_status_snapshot_fails_closed_for_control_inflight_missing_group_rows_when_cached_ready_groups_do_not_cover_live_schedule()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-b::nfs1".to_string(),
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
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let live_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs2".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs2=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        evaluate_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            None,
            SinkStatusAccessPath::ControlInflight,
        ),
        SinkStatusSnapshotOutcome {
            kind: SinkStatusOutcomeKind::FailClosed,
            concern: Some(SinkStatusConcern::CoverageGap),
            should_mark_replay_required: false,
            should_republish_zero_row_summary: false,
        },
        "control-inflight fallback must not reuse cached ready truth from a different scheduled group when the live missing-group-rows regression targets a new schedule"
    );
}

#[test]
fn evaluate_live_sink_status_snapshot_marks_replay_required_for_pending_materialization_without_stream_receipts()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
            total_nodes: 6,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_live_sink_status_snapshot(
        &live_snapshot,
        &cached_snapshot,
        None,
        SinkStatusAccessPath::Steady,
    );

    assert_eq!(evaluation.kind, SinkStatusOutcomeKind::FailClosed);
    assert!(
        evaluation.should_mark_replay_required,
        "pending-materialization live snapshots without stream receipts must request replay before the caller decides how to surface the failure",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "live pending-materialization snapshots must not request zero-row republish side effects"
    );
}

#[test]
fn evaluate_live_sink_status_snapshot_returns_live_for_steady_after_retry_reset_waiting_for_materialized_root()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-d::nfs1".to_string(),
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
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-d".to_string(),
            vec!["nfs1".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_live_sink_status_snapshot(
        &live_snapshot,
        &cached_snapshot,
        None,
        SinkStatusAccessPath::SteadyAfterRetryReset,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnLive,
        "a fresh scheduled waiting-for-materialized-root snapshot recovered after retry-reset should surface live schedule truth instead of failing closed as an ordinary steady zero-state regression",
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "retry-reset steady waiting-for-materialized-root should not re-arm replay-required once replay already succeeded"
    );
}

#[test]
fn evaluate_live_sink_status_snapshot_returns_cached_for_steady_after_retry_reset_pending_materialization_when_cached_ready_groups_cover_live_schedule()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-d::nfs2".to_string(),
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
                materialized_revision: 3,
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs4".to_string(),
                primary_object_ref: "node-d::nfs4".to_string(),
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
                materialized_revision: 3,
                estimated_heap_bytes: 1,
            },
        ],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-d".to_string(),
            vec!["nfs2".to_string(), "nfs4".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-d::nfs2".to_string(),
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
                group_id: "nfs4".to_string(),
                primary_object_ref: "node-d::nfs4".to_string(),
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
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-d".to_string(),
            vec!["nfs2".to_string(), "nfs4".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_live_sink_status_snapshot(
        &live_snapshot,
        &cached_snapshot,
        None,
        SinkStatusAccessPath::SteadyAfterRetryReset,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "retry-reset steady pending-materialization live snapshots should preserve cached ready truth when the cached ready groups still cover the current live schedule",
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "retry-reset steady pending-materialization fallback should not re-arm replay-required once replay already succeeded"
    );
}

#[test]
fn evaluate_live_sink_status_snapshot_returns_live_for_steady_after_retry_reset_pending_materialization_when_cached_snapshot_is_stale_empty()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-d::nfs1".to_string(),
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
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-d".to_string(),
            vec!["nfs1".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_live_sink_status_snapshot(
        &live_snapshot,
        &cached_snapshot,
        None,
        SinkStatusAccessPath::SteadyAfterRetryReset,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnLive,
        "retry-reset steady pending-materialization snapshots should surface the republished live schedule when cached truth is only a stale-empty zero state",
    );
    assert_eq!(evaluation.concern, Some(SinkStatusConcern::ReplayPending));
    assert!(!evaluation.should_mark_replay_required);
}

#[test]
fn evaluate_live_sink_status_snapshot_returns_live_for_steady_after_retry_reset_pending_materialization_when_cached_snapshot_is_pre_activate_unscheduled()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-d::nfs1".to_string(),
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
        }],
        ..SinkStatusSnapshot::default()
    };
    let live_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-d::nfs1".to_string(),
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
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-d".to_string(),
            vec!["nfs1".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_live_sink_status_snapshot(
        &live_snapshot,
        &cached_snapshot,
        None,
        SinkStatusAccessPath::SteadyAfterRetryReset,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnLive,
        "retry-reset steady pending-materialization snapshots should surface the fresh republished live schedule even when the cached snapshot still reflects the pre-activate unscheduled zero-state",
    );
    assert_eq!(evaluation.concern, Some(SinkStatusConcern::ReplayPending));
    assert!(!evaluation.should_mark_replay_required);
}

#[test]
fn fold_live_sink_status_snapshot_fails_closed_for_steady_stale_empty_without_cached_missing_rows_escape()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot::default();

    assert_eq!(
        evaluate_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            None,
            SinkStatusAccessPath::Steady,
        ),
        SinkStatusSnapshotOutcome {
            kind: SinkStatusOutcomeKind::FailClosed,
            concern: Some(SinkStatusConcern::StaleEmpty),
            should_mark_replay_required: false,
            should_republish_zero_row_summary: false,
        },
        "steady stale-empty live snapshots must fail closed unless the cached issue explicitly proves the missing-group-rows retry-reset escape hatch",
    );
}

#[test]
fn fold_cached_sink_status_snapshot_returns_cached_for_worker_unavailable_missing_group_rows_with_stream_evidence()
 {
    let cached_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        evaluate_cached_sink_status_snapshot(
            &cached_snapshot,
            SinkStatusAccessPath::WorkerUnavailable,
        )
        .kind,
        SinkStatusOutcomeKind::ReturnCached,
        "worker-unavailable paths should still be allowed to return cached missing-group rows when stream evidence proves the schedule was already alive",
    );
}

#[test]
fn fold_cached_sink_status_snapshot_fails_closed_for_not_started_stale_empty_cache() {
    let cached_snapshot = SinkStatusSnapshot::default();

    assert_eq!(
        evaluate_cached_sink_status_snapshot(&cached_snapshot, SinkStatusAccessPath::NotStarted)
            .kind,
        SinkStatusOutcomeKind::FailClosed,
        "not-started paths must fail closed on stale-empty cached snapshots instead of treating them as usable sink truth",
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_worker_unavailable_pending_materialization_without_stream_receipts()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
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
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_cached_sink_status_snapshot(
        &cached_snapshot,
        SinkStatusAccessPath::WorkerUnavailable,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "worker-unavailable cached pending-materialization snapshots should preserve the existing cached snapshot fallback instead of propagating the worker error",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "worker-unavailable cached pending-materialization snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached pending-materialization snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_not_started_pending_materialization_without_stream_receipts()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
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
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation =
        evaluate_cached_sink_status_snapshot(&cached_snapshot, SinkStatusAccessPath::NotStarted);

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "not-started cached pending-materialization snapshots should preserve the cached scheduled-zero truth instead of failing closed as stale cache",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "not-started cached pending-materialization snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached pending-materialization snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_worker_unavailable_waiting_for_materialized_root()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_cached_sink_status_snapshot(
        &cached_snapshot,
        SinkStatusAccessPath::WorkerUnavailable,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "worker-unavailable cached waiting-for-root snapshots should preserve the stable cached unready truth instead of propagating the worker error",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "worker-unavailable cached waiting-for-root snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached waiting-for-root snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_not_started_waiting_for_materialized_root()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation =
        evaluate_cached_sink_status_snapshot(&cached_snapshot, SinkStatusAccessPath::NotStarted);

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "not-started cached waiting-for-root snapshots should preserve the stable cached unready truth instead of failing closed as stale cache",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "not-started cached waiting-for-root snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached waiting-for-root snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_not_started_missing_group_rows_after_stream_evidence()
 {
    let cached_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_applied_batches_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            1,
        )]),
        stream_applied_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            15,
        )]),
        stream_applied_control_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            3,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            12,
        )]),
        stream_applied_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_last_applied_at_us_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            42,
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation =
        evaluate_cached_sink_status_snapshot(&cached_snapshot, SinkStatusAccessPath::NotStarted);

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "not-started cached missing-group-row snapshots with stream evidence should preserve the cached scheduled truth instead of failing closed as stale cache",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "not-started cached missing-group-row snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached missing-group-row snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_returns_cached_for_control_inflight_no_client_pending_materialization_without_stream_receipts()
 {
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
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
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let evaluation = evaluate_cached_sink_status_snapshot(
        &cached_snapshot,
        SinkStatusAccessPath::ControlInflightNoClient,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::ReturnCached,
        "control-inflight cached pending-materialization snapshots should preserve the cached scheduled-zero truth instead of failing closed as stale cache",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "control-inflight cached pending-materialization snapshots must not request zero-row republish"
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached pending-materialization snapshots should not arm replay-required from the cached fold path"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_requests_zero_row_republish_for_stale_empty_cache() {
    let cached_snapshot = SinkStatusSnapshot::default();

    let evaluation =
        evaluate_cached_sink_status_snapshot(&cached_snapshot, SinkStatusAccessPath::NotStarted);

    assert_eq!(evaluation.kind, SinkStatusOutcomeKind::FailClosed);
    assert!(
        evaluation.should_republish_zero_row_summary,
        "stale-empty cached snapshots must request zero-row republish before callers fail closed",
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached stale-empty snapshots should not arm replay-required side effects on their own"
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_propagates_error_for_control_inflight_no_client_stale_empty_cache()
 {
    let cached_snapshot = SinkStatusSnapshot::default();

    let evaluation = evaluate_cached_sink_status_snapshot(
        &cached_snapshot,
        SinkStatusAccessPath::ControlInflightNoClient,
    );

    assert_eq!(
        evaluation.kind,
        SinkStatusOutcomeKind::PropagateError,
        "control-inflight stale-empty cached snapshots must preserve the live probe error instead of flattening it into fail-closed timeout",
    );
    assert!(
        evaluation.should_republish_zero_row_summary,
        "control-inflight stale-empty cached snapshots must still request zero-row republish before surfacing the live probe error",
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached stale-empty snapshots should not arm replay-required side effects on their own",
    );
}
