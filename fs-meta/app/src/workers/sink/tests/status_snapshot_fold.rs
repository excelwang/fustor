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
        classify_sink_status_snapshot_issue(&snapshot, false),
        Some(SinkStatusSnapshotIssue::ScheduledMissingGroupRowsAfterStreamEvidence),
        "scheduled sink groups that already carry stream evidence must classify as missing-group-rows, not as healthy"
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
            overflow_pending_audit: false,
            initial_audit_completed: false,
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
        classify_sink_status_snapshot_issue(&snapshot, false),
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
                overflow_pending_audit: false,
                initial_audit_completed: true,
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
                overflow_pending_audit: false,
                initial_audit_completed: false,
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
        classify_sink_status_snapshot_issue(&snapshot, false),
        Some(SinkStatusSnapshotIssue::ScheduledMixedReadyAndUnready),
        "scheduled groups that split between ready and unready rows must classify as a mixed readiness regression"
    );
}

#[test]
fn classify_sink_status_snapshot_issue_detects_scheduled_pending_audit_without_stream_receipts_only_when_replay_is_required()
 {
    let snapshot = SinkStatusSnapshot {
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
            overflow_pending_audit: false,
            initial_audit_completed: false,
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
        classify_sink_status_snapshot_issue(&snapshot, false),
        None,
        "without replay_required, a replay-only not-ready snapshot should stay classified as healthy so caller policy can decide separately"
    );
    assert_eq!(
        classify_sink_status_snapshot_issue(&snapshot, true),
        Some(SinkStatusSnapshotIssue::ScheduledPendingAuditWithoutStreamReceipts),
        "with replay_required, the same snapshot must classify as pending-audit without stream receipts"
    );
}

#[test]
fn sink_group_status_readiness_distinguishes_waiting_for_materialized_root_from_pending_audit() {
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
        overflow_pending_audit: false,
        initial_audit_completed: false,
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    };
    let pending_audit = crate::sink::SinkGroupStatusSnapshot {
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
        overflow_pending_audit: false,
        initial_audit_completed: false,
        materialized_revision: 7,
        estimated_heap_bytes: 1,
    };

    assert_eq!(
        classify_sink_group_status_readiness(&waiting_for_materialized_root),
        SinkGroupStatusReadiness::WaitingForMaterializedRoot
    );
    assert_eq!(
        classify_sink_group_status_readiness(&pending_audit),
        SinkGroupStatusReadiness::PendingAudit
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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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
            overflow_pending_audit: false,
            initial_audit_completed: false,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        scheduled_groups_by_node: cached_snapshot.scheduled_groups_by_node.clone(),
        ..SinkStatusSnapshot::default()
    };

    assert_eq!(
        fold_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            false,
            None,
            SinkStatusLiveFoldMode::ControlInflight,
        ),
        SinkStatusLiveFoldOutcome::ReturnCached,
        "control-inflight live scheduled-zero regressions must fall back to cached ready truth instead of reopening the zero snapshot",
    );
}

#[test]
fn evaluate_live_sink_status_snapshot_marks_replay_required_for_pending_audit_without_stream_receipts()
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
            overflow_pending_audit: false,
            initial_audit_completed: false,
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
        true,
        None,
        SinkStatusLiveFoldMode::Steady,
    );

    assert_eq!(evaluation.decision, SinkStatusAvailabilityDecision::FailClosed);
    assert!(
        evaluation.should_mark_replay_required,
        "pending-audit live snapshots without stream receipts must request replay before the caller decides how to surface the failure",
    );
    assert!(
        !evaluation.should_republish_zero_row_summary,
        "live pending-audit snapshots must not request zero-row republish side effects"
    );
}

#[test]
fn fold_live_sink_status_snapshot_fails_closed_for_steady_stale_empty_without_cached_missing_rows_escape()
 {
    let cached_snapshot = SinkStatusSnapshot::default();
    let live_snapshot = SinkStatusSnapshot::default();

    assert_eq!(
        fold_live_sink_status_snapshot(
            &live_snapshot,
            &cached_snapshot,
            false,
            None,
            SinkStatusLiveFoldMode::Steady,
        ),
        SinkStatusLiveFoldOutcome::FailClosed,
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
        fold_cached_sink_status_snapshot(
            &cached_snapshot,
            false,
            SinkStatusCachedFoldMode::WorkerUnavailable,
        ),
        SinkStatusCachedFoldOutcome::ReturnCached,
        "worker-unavailable paths should still be allowed to return cached missing-group rows when stream evidence proves the schedule was already alive",
    );
}

#[test]
fn fold_cached_sink_status_snapshot_fails_closed_for_not_started_stale_empty_cache() {
    let cached_snapshot = SinkStatusSnapshot::default();

    assert_eq!(
        fold_cached_sink_status_snapshot(
            &cached_snapshot,
            false,
            SinkStatusCachedFoldMode::NotStarted,
        ),
        SinkStatusCachedFoldOutcome::FailClosed,
        "not-started paths must fail closed on stale-empty cached snapshots instead of treating them as usable sink truth",
    );
}

#[test]
fn evaluate_cached_sink_status_snapshot_requests_zero_row_republish_for_stale_empty_cache() {
    let cached_snapshot = SinkStatusSnapshot::default();

    let evaluation = evaluate_cached_sink_status_snapshot(
        &cached_snapshot,
        false,
        SinkStatusCachedFoldMode::NotStarted,
    );

    assert_eq!(evaluation.decision, SinkStatusAvailabilityDecision::FailClosed);
    assert!(
        evaluation.should_republish_zero_row_summary,
        "stale-empty cached snapshots must request zero-row republish before callers fail closed",
    );
    assert!(
        !evaluation.should_mark_replay_required,
        "cached stale-empty snapshots should not arm replay-required side effects on their own"
    );
}
