fn sink_group_status(
    group_id: &str,
    initial_audit_completed: bool,
) -> crate::sink::SinkGroupStatusSnapshot {
    crate::sink::SinkGroupStatusSnapshot {
        group_id: group_id.to_string(),
        primary_object_ref: format!("{group_id}-owner"),
        total_nodes: 1,
        live_nodes: 1,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 1,
        shadow_lag_us: 0,
        overflow_pending_audit: false,
        readiness: if initial_audit_completed {
            crate::sink::GroupReadinessState::Ready
        } else {
            crate::sink::GroupReadinessState::PendingAudit
        },
        initial_audit_completed,
        materialized_revision: 1,
        estimated_heap_bytes: 0,
    }
}

#[test]
fn cached_sink_status_preserves_complete_snapshot_for_same_roots() {
    let groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()]);
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
                sink_group_status("nfs3", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let fresh = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec!["nfs1", "nfs2", "nfs3"]);
    assert!(
        merged
            .groups
            .iter()
            .all(|group| group.initial_audit_completed)
    );
}

#[test]
fn materialized_scheduled_group_ids_preserves_groups_present_in_snapshot_when_scheduled_map_is_partial()
 {
    let snapshot = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", false),
            sink_group_status("nfs2", false),
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let groups = materialized_scheduled_group_ids(&snapshot)
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(groups, vec!["nfs1", "nfs2", "nfs3"]);
}

#[test]
fn cached_sink_status_drops_stale_groups_after_root_set_change() {
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let fresh_groups = BTreeSet::from(["nfs1".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &fresh_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec!["nfs1"]);
}

#[test]
fn local_sink_snapshot_explicit_empty_clears_preserved_cached_ready_groups_after_routed_transition()
{
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs3".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let cached_snapshot = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: cached_snapshot.clone(),
    };
    let allowed_groups =
        BTreeSet::from(["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()]);
    let routed_sink_status = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
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
            readiness: crate::sink::GroupReadinessState::PendingAudit,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-a::nfs2".to_string(),
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
            readiness: crate::sink::GroupReadinessState::PendingAudit,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let merged =
        merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, routed_sink_status);
    let preserved = preserve_cached_ready_groups_across_explicit_empty_root_transition(
        merged,
        Some(&cached_snapshot),
        &source_status,
        &BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
    );
    let local_sink_status = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
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
            readiness: crate::sink::GroupReadinessState::PendingAudit,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-a::nfs2".to_string(),
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
            readiness: crate::sink::GroupReadinessState::PendingAudit,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            sink_group_status("nfs3", true),
        ],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    let route_explicit_empty_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let explicit_empty_groups_to_clear = local_sink_snapshot_route_preserved_explicit_empty_groups(
        &preserved,
        &local_sink_status,
        &route_explicit_empty_groups,
    );

    assert!(
        !sink_status_snapshot_has_better_groups(&preserved, &local_sink_status),
        "generic local snapshot comparison must not treat same-group explicit-empty as globally better when the only correction needed is to clear route-preserved stale ready groups"
    );
    assert_eq!(
        explicit_empty_groups_to_clear, route_explicit_empty_groups,
        "only the route-explicit-empty groups should be cleared from the preserved snapshot before merging the local sink snapshot"
    );

    let merged = merge_with_local_sink_status_snapshot(
        preserved,
        local_sink_status,
        &explicit_empty_groups_to_clear,
    );
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.as_str(),
                group.initial_audit_completed,
                group.total_nodes,
                group.live_nodes,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        merged_groups,
        vec![
            ("nfs1", false, 0, 0),
            ("nfs2", false, 0, 0),
            ("nfs3", true, 1, 1),
        ],
        "local same-group explicit-empty sink status must clear route-preserved cached ready groups instead of leaving stale trusted-materialized nfs1/nfs2 in the merged snapshot: {merged_groups:?}"
    );
}

#[test]
fn materialized_query_readiness_preserves_retained_group_audit_across_root_set_contraction() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "nfs2".into(),
            status: "ok".into(),
            matched_grants: 3,
            active_members: 3,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2-key".into(),
            logical_root_id: "nfs2".into(),
            object_ref: "node-a::nfs2".into(),
            status: "ok".into(),
            coverage_mode: "realtime_hotset_plus_audit".into(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 128,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: Some("manual".into()),
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(1),
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: Some(1),
            current_stream_generation: Some(1),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
                sink_group_status("nfs3", true),
                sink_group_status("nfs4", false),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs2".to_string(), "nfs4".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![
            sink_group_status("nfs2", false),
            sink_group_status("nfs4", false),
        ],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec![("nfs2", true), ("nfs4", false)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_none(),
        "retained nfs2 should stay trusted-materialized ready across retire root contraction"
    );
}

#[test]
fn cached_sink_status_preserves_missing_nfs2_readiness_when_later_fresh_snapshot_only_reports_other_groups()
 {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let cached = CachedSinkStatusSnapshot {
        snapshot: SinkStatusSnapshot {
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(merged_groups, vec![("nfs1", true), ("nfs2", true)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_none(),
        "later fresh snapshots that only mention other groups must not make cached nfs2 readiness disappear again"
    );
}
