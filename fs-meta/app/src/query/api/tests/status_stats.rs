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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_preserves_cached_ready_groups_when_routed_sink_status_reports_explicit_empty_root_transition_snapshot()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a-nfs2");
    let node_c_root = tmp.path().join("node-c-nfs4");
    fs::create_dir_all(&node_a_root).expect("create node-a nfs2 dir");
    fs::create_dir_all(&node_c_root).expect("create node-c nfs4 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs4".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root.clone(),
            fs_source: "nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs2", &node_a_root),
            RootSpec::new("nfs4", &node_c_root),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        groups: vec![
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
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs4".to_string(),
                primary_object_ref: "node-c::nfs4".to_string(),
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
        ..SinkStatusSnapshot::default()
    })
    .expect("encode retire-shaped explicit-empty sink status snapshot");
    let boundary = Arc::new(SourceStatusTimeoutSinkStatusOkBoundary::new(
        sink_status_payload,
    ));
    let cached_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs4".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs2", true),
            sink_group_status("nfs4", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary,
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_sink_status_cache: Arc::new(Mutex::new(Some(CachedSinkStatusSnapshot {
            snapshot: cached_snapshot,
        }))),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("materialized status snapshots");

    let ready_groups = sink_status
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.as_str(),
                group.initial_audit_completed,
                group.total_nodes,
                group.live_nodes,
                group.shadow_time_us,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ready_groups,
        vec![("nfs2", true, 1, 1, 1), ("nfs4", true, 1, 1, 1)],
        "routed explicit-empty sink-status during retire-shaped root transition must not clear previously trusted cached ready groups before a better authoritative sink snapshot arrives"
    );
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "retire-shaped routed explicit-empty sink-status must not reintroduce trusted-materialized NOT_READY for cached-ready groups nfs2/nfs4"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_falls_back_to_local_source_when_route_source_status_times_out()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_a_root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SourceStatusTimeoutSinkStatusOkBoundary::new(
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let result = load_materialized_status_snapshots(&state).await;

    assert!(
        result.is_ok(),
        "tree/status loading should fall back to local source status when routed source-status fan-in times out but sink-status still replies; source_status_calls={} sink_status_calls={} source_status_reply_polls={} sink_status_reply_polls={} err={:?}",
        boundary.send_batch_count(&source_status_route.0),
        boundary.send_batch_count(&sink_status_route.0),
        boundary.recv_batch_count(&format!("{}:reply", source_status_route.0)),
        boundary.recv_batch_count(&format!("{}:reply", sink_status_route.0)),
        result.as_ref().err(),
    );
    let (source_status, sink_status) = result.expect("materialized status snapshots");
    assert_eq!(
        source_status
            .logical_roots
            .iter()
            .map(|root| root.root_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "local source status fallback should preserve the configured logical roots"
    );
    assert_eq!(
        sink_status
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "sink status route fan-in should still contribute the scheduled materialized groups"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_retries_routed_source_status_before_local_fallback() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 2,
                active_members: 2,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )]),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SourceStatusRetryThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, _sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("materialized status snapshots after routed source-status retry");

    assert_eq!(
        boundary.send_batch_count(&source_status_route.0),
        2,
        "source-status caller chain must reissue routed status collection after a transient first-attempt timeout instead of collapsing immediately to local fallback",
    );
    assert_eq!(
        source_status.current_stream_generation,
        Some(9),
        "successful routed retry should preserve the peer source-status snapshot instead of falling back to local-only status",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_source_status_snapshot_retries_missing_route_state_without_blind_sleep() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 2,
                active_members: 2,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )]),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SourceStatusMissingRouteStateThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");

    let query_task = tokio::spawn({
        let boundary = boundary.clone();
        async move {
            route_source_status_snapshot(
                boundary,
                NodeId("node-d".to_string()),
                Duration::from_secs(5),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(1), boundary.wait_for_second_send())
            .await
            .expect(
                "source-status continuity-gap retry should reissue the routed source-status request once missing-route-state continuity is restored",
            );

    assert_eq!(
        boundary.send_batch_count(&source_status_route.0),
        2,
        "source-status continuity-gap retry should still issue exactly one retry before succeeding",
    );
    let retry_reissue_delay = boundary
        .retry_reissue_delay()
        .expect("boundary should record the gap-to-reissue delay");
    assert!(
        retry_reissue_delay < Duration::from_millis(80),
        "source-status continuity-gap retry should reissue immediately from missing-route-state truth instead of sleeping a fixed 100ms first; observed delay {:?}",
        retry_reissue_delay,
    );
    query_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_retries_routed_source_status_after_missing_channel_buffer_state()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs1".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 2,
                active_members: 2,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string()],
        )]),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let boundary = Arc::new(SourceStatusMissingRouteStateThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, _sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("materialized status snapshots after routed source-status continuity-gap retry");

    assert_eq!(
        boundary.send_batch_count(&source_status_route.0),
        2,
        "source-status caller chain must reissue routed source-status collection after a transient missing channel_buffer route-state gap instead of failing public `/tree` immediately",
    );
    assert_eq!(
        source_status.current_stream_generation,
        Some(9),
        "successful routed retry should preserve the peer source-status snapshot after a missing channel_buffer continuity gap",
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_falls_back_to_local_sink_when_route_sink_status_times_out()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_a_root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
            logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            }],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::new(),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let boundary = Arc::new(SourceStatusRetryThenReplyBoundary::new(
        source_status_payload,
        Vec::new(),
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let result = load_materialized_status_snapshots(&state).await;

    assert!(
        result.is_ok(),
        "materialized status loading should fall back to local sink status when routed sink-status fan-in times out but source-status still replies; source_status_calls={} sink_status_calls={} err={:?}",
        boundary.send_batch_count(&source_status_route.0),
        boundary.send_batch_count(&sink_status_route.0),
        result.as_ref().err(),
    );
    let (source_status, sink_status) = result.expect("materialized status snapshots");
    assert_eq!(source_status.current_stream_generation, Some(9));
    assert_eq!(
        sink_status
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "local sink status fallback should preserve the configured materialized groups"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_falls_back_to_local_source_when_route_source_status_send_hits_missing_channel_buffer_state()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: node_a_root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let boundary = Arc::new(SourceStatusSendMissingRouteStateSinkStatusOkBoundary::new(
        sink_status_payload,
    ));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let result = load_materialized_status_snapshots(&state).await;

    assert!(
        result.is_ok(),
        "materialized status loading should fall back or retry when routed source-status send path loses channel_buffer state instead of failing public `/tree`; source_status_calls={} err={:?}",
        boundary.send_batch_count(&source_status_route.0),
        result.as_ref().err(),
    );
    let (source_status, sink_status) = result.expect("materialized status snapshots");
    assert_eq!(
        source_status
            .logical_roots
            .iter()
            .map(|root| root.root_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "local source status fallback should preserve the configured logical roots"
    );
    assert_eq!(
        sink_status
            .groups
            .iter()
            .map(|group| group.group_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "sink status route should still contribute the materialized group after source continuity fallback"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_supplements_missing_route_sink_groups_from_local_sink()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let roots = {
        let mut nfs1 = RootSpec::new("nfs1", &node_a_root);
        nfs1.scan = true;
        let mut nfs2 = RootSpec::new("nfs2", &node_b_root);
        nfs2.scan = true;
        vec![nfs1, nfs2]
    };
    let source = source_facade_with_roots(roots.clone(), &grants);
    let local_sink = Arc::new(
        SinkFileMeta::with_boundaries(
            NodeId("sink-node".into()),
            None,
            SourceConfig {
                roots: roots.clone(),
                host_object_grants: grants.clone(),
                ..SourceConfig::default()
            },
        )
        .expect("build sink"),
    );
    local_sink
        .send(&[
            mk_source_record_event("node-b::nfs2", b"/ready.txt", b"ready.txt", 1),
            mk_control_event(
                "node-b::nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-b::nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .await
        .expect("materialize nfs2 locally");
    let sink = Arc::new(SinkFacade::local(local_sink));
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: roots.clone(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
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
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::new(),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode partial sink-status payload");
    let boundary = Arc::new(SourceStatusRetryThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary,
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("status snapshots should supplement missing route sink groups from local sink");

    let groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(
        groups,
        vec![("nfs1", true), ("nfs2", true)],
        "local sink snapshot should fill the missing nfs2 readiness group instead of leaving trusted-materialized reads blocked on a partial routed sink-status reply",
    );
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "supplemented sink-status should clear the spurious initial-audit-incomplete readiness error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_retries_routed_sink_status_when_first_reply_reports_all_active_groups_explicit_empty()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let roots = vec![
        RootSpec::new("nfs1", &node_a_root),
        RootSpec::new("nfs2", &node_b_root),
    ];
    let source = source_facade_with_roots(roots.clone(), &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: roots.clone(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
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
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([
            ("nfs1".to_string(), "node-a::nfs1".to_string()),
            ("nfs2".to_string(), "node-a::nfs2".to_string()),
        ]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs1".to_string()]),
        ]),
        scheduled_scan_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs1".to_string()]),
        ]),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::from([
            ("node-a".to_string(), 6),
            ("node-b".to_string(), 0),
            ("node-c".to_string(), 0),
            ("node-d".to_string(), 0),
        ]),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let explicit_empty_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode explicit-empty sink-status payload");
    let ready_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode ready sink-status payload");
    let decoded_source_status =
        rmp_serde::from_slice::<SourceObservabilitySnapshot>(&source_status_payload)
            .expect("decode source-status payload for helper assertion")
            .status;
    let decoded_explicit_empty_sink_status =
        rmp_serde::from_slice::<SinkStatusSnapshot>(&explicit_empty_sink_status_payload)
            .expect("decode explicit-empty sink-status payload for helper assertion");
    let readiness_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    assert!(
        sink_status_snapshot_reports_explicit_empty_for_all_active_readiness_groups(
            &decoded_source_status,
            &decoded_explicit_empty_sink_status,
            &readiness_groups,
        ),
        "helper should detect the explicit-empty-all-active-groups shape before routed sink-status recollect",
    );
    let boundary = Arc::new(SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary::new(
        source_status_payload,
        vec![
            explicit_empty_sink_status_payload,
            ready_sink_status_payload,
        ],
    ));
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_reply_route = format!("{}:reply", sink_status_route.0);
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
            .await
            .expect("status snapshots should retry routed sink-status when the first reply reports all active groups explicit-empty");

    assert!(
        boundary.recv_batch_count(&sink_status_reply_route) > 1,
        "routed sink-status fan-in should consume more than one sink-status reply when the first successful reply reports all active readiness groups explicit-empty",
    );
    let groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(groups, vec![("nfs1", true), ("nfs2", true)]);
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "recollected sink-status should clear the spurious trusted-materialized NOT_READY after an explicit-empty-all-groups first reply",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_retries_routed_sink_status_when_first_reply_reports_one_active_group_explicit_empty_while_others_ready()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    fs::create_dir_all(&node_c_root).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs3".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let roots = vec![
        RootSpec::new("nfs1", &node_a_root),
        RootSpec::new("nfs2", &node_b_root),
        RootSpec::new("nfs3", &node_c_root),
    ];
    let source = source_facade_with_roots(roots.clone(), &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: roots.clone(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
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
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([
            ("nfs1".to_string(), "node-a::nfs1".to_string()),
            ("nfs2".to_string(), "node-b::nfs2".to_string()),
            ("nfs3".to_string(), "node-c::nfs3".to_string()),
        ]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs1".to_string()]),
            ("node-b".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs3".to_string()]),
        ]),
        scheduled_scan_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs1".to_string()]),
            ("node-b".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs3".to_string()]),
        ]),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let first_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs1".to_string()]),
            ("node-b".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs3".to_string(),
                primary_object_ref: "node-c::nfs3".to_string(),
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
        ..SinkStatusSnapshot::default()
    })
    .expect("encode first partial explicit-empty sink-status payload");
    let ready_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs1".to_string()]),
            ("node-b".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode ready sink-status payload");
    let boundary = Arc::new(SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary::new(
        source_status_payload,
        vec![first_sink_status_payload, ready_sink_status_payload],
    ));
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_reply_route = format!("{}:reply", sink_status_route.0);
    let state = ApiState {
        backend: QueryBackend::Route {
            sink,
            boundary: boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
            .await
            .expect("status snapshots should retry routed sink-status when one active group is explicit-empty while others are ready");

    assert!(
        boundary.recv_batch_count(&sink_status_reply_route) > 1,
        "routed sink-status fan-in should consume more than one sink-status reply when the first successful reply leaves an active readiness group explicit-empty while peers are already ready",
    );
    let groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(groups, vec![("nfs1", true), ("nfs2", true), ("nfs3", true)]);
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "recollected sink-status should clear the spurious trusted-materialized readiness gap for the later-ranked explicit-empty group",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_bounds_second_routed_sink_status_recollect_after_explicit_empty_all_active_groups()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let roots = vec![
        RootSpec::new("nfs1", &node_a_root),
        RootSpec::new("nfs2", &node_b_root),
    ];
    let source = source_facade_with_roots(roots.clone(), &grants);
    let sink = sink_facade_with_group(&grants);
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: roots.clone(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
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
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([
            ("nfs1".to_string(), "node-a::nfs1".to_string()),
            ("nfs2".to_string(), "node-a::nfs2".to_string()),
        ]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        scheduled_scan_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::new(),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let explicit_empty_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode explicit-empty sink-status payload");
    let boundary = Arc::new(SourceStatusOkSinkStatusExplicitEmptyThenReadyBoundary::new(
        source_status_payload,
        vec![explicit_empty_sink_status_payload],
    ));
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary,
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = tokio::time::timeout(
            Duration::from_secs(4),
            load_materialized_status_snapshots(&state),
        )
        .await
        .expect(
            "explicit-empty second routed sink-status recollect must stay bounded and must not consume the whole caller budget when the recollect stalls",
        )
        .expect("status snapshots should still return the first explicit-empty sink snapshot");

    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_some(),
        "bounded recollect fallback should preserve the explicit-empty readiness boundary instead of hanging the caller",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_supplements_empty_route_sink_groups_from_local_sink() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(&node_a_root).expect("create node-a dir");
    fs::create_dir_all(&node_b_root).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs2".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_b_root.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let roots = vec![
        RootSpec::new("nfs1", &node_a_root),
        RootSpec::new("nfs2", &node_b_root),
    ];
    let source = source_facade_with_roots(roots.clone(), &grants);
    let local_sink = Arc::new(
        SinkFileMeta::with_boundaries(
            NodeId("sink-node".into()),
            None,
            SourceConfig {
                roots: roots.clone(),
                host_object_grants: grants.clone(),
                ..SourceConfig::default()
            },
        )
        .expect("build sink"),
    );
    local_sink
        .send(&[
            mk_source_record_event("node-a::nfs1", b"/ready-a.txt", b"ready-a.txt", 1),
            mk_source_record_event("node-b::nfs2", b"/ready-b.txt", b"ready-b.txt", 2),
            mk_control_event(
                "node-a::nfs1",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
            mk_control_event(
                "node-a::nfs1",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
            mk_control_event(
                "node-b::nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                5,
            ),
            mk_control_event(
                "node-b::nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
        ])
        .await
        .expect("materialize nfs1/nfs2 locally");
    let sink = Arc::new(SinkFacade::local(local_sink));
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: roots.clone(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
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
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::new(),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs1".to_string()]),
        ]),
        scheduled_scan_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs1".to_string()]),
        ]),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::from([
            ("node-a".to_string(), 6),
            ("node-b".to_string(), 0),
            ("node-c".to_string(), 0),
            ("node-d".to_string(), 0),
        ]),
        published_events_by_node: BTreeMap::new(),
        published_control_events_by_node: BTreeMap::new(),
        published_data_events_by_node: BTreeMap::new(),
        last_published_at_us_by_node: BTreeMap::new(),
        last_published_origins_by_node: BTreeMap::new(),
        published_origin_counts_by_node: BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: BTreeMap::new(),
        pending_path_origin_counts_by_node: BTreeMap::new(),
        yielded_path_origin_counts_by_node: BTreeMap::new(),
        summarized_path_origin_counts_by_node: BTreeMap::new(),
        published_path_origin_counts_by_node: BTreeMap::new(),
    })
    .expect("encode source-status payload");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot::default())
        .expect("encode empty sink-status payload");
    let boundary = Arc::new(SourceStatusRetryThenReplyBoundary::new(
        source_status_payload,
        sink_status_payload,
    ));
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary,
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("status snapshots should supplement empty route sink groups from local sink");

    let groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group.initial_audit_completed))
        .collect::<Vec<_>>();
    assert_eq!(
        groups,
        vec![("nfs1", true), ("nfs2", true)],
        "local sink snapshot should replace an empty routed sink-status view instead of leaving trusted-materialized reads blocked on an empty peer status fan-in",
    );
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "supplemented sink-status should clear the spurious initial-audit-incomplete readiness error after an empty routed sink-status view",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_sink_status_snapshot_retries_transient_internal_collect_gap() {
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SinkStatusInternalRetryThenReplyBoundary::new(
        sink_status_payload,
    ));
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");

    let snapshot = route_sink_status_snapshot(
        boundary.clone(),
        NodeId("node-d".to_string()),
        Duration::from_secs(5),
    )
    .await
    .expect("sink-status collection should retry transient internal collect gap");

    assert_eq!(
        boundary.send_batch_count(&sink_status_route.0),
        2,
        "sink-status collection must reissue after a transient internal collect gap instead of returning internal immediately",
    );
    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string()])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_sink_status_snapshot_retries_transient_internal_collect_gap_without_blind_sleep() {
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SinkStatusInternalRetryThenReplyBoundary::new(
        sink_status_payload,
    ));
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");

    let snapshot = tokio::time::timeout(
        Duration::from_secs(3),
        route_sink_status_snapshot(
            boundary.clone(),
            NodeId("node-d".to_string()),
            Duration::from_secs(5),
        ),
    )
    .await
    .expect("sink-status continuity-gap retry should stay bounded")
    .expect("sink-status continuity-gap retry should still return the retried snapshot");

    assert_eq!(
        boundary.send_batch_count(&sink_status_route.0),
        2,
        "sink-status continuity-gap retry should still issue exactly one retry before succeeding",
    );
    let retry_reissue_delay = boundary
        .retry_reissue_delay()
        .expect("boundary should record the gap-to-reissue delay");
    assert!(
        retry_reissue_delay < Duration::from_millis(80),
        "sink-status continuity-gap retry should reissue immediately from continuity-gap truth instead of sleeping a fixed 100ms first; observed delay {:?}",
        retry_reissue_delay,
    );
    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string()])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_sink_status_snapshot_retries_peer_transport_close() {
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status snapshot");
    let boundary = Arc::new(SinkStatusPeerTransportRetryThenReplyBoundary::new(
        sink_status_payload,
    ));
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");

    let snapshot = route_sink_status_snapshot(
        boundary.clone(),
        NodeId("node-d".to_string()),
        Duration::from_secs(5),
    )
    .await
    .expect("sink-status collection should retry peer transport close");

    assert_eq!(
        boundary.send_batch_count(&sink_status_route.0),
        2,
        "sink-status collection must reissue after a peer transport close instead of returning transport immediately",
    );
    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string()])
    );
}

#[test]
fn merge_sink_status_snapshots_preserves_received_origin_counts_by_node() {
    let merged = merge_sink_status_snapshots(vec![
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            received_batches_by_node: BTreeMap::from([("node-a".to_string(), 11)]),
            received_events_by_node: BTreeMap::from([("node-a".to_string(), 111)]),
            received_control_events_by_node: BTreeMap::from([("node-a".to_string(), 2)]),
            received_data_events_by_node: BTreeMap::from([("node-a".to_string(), 109)]),
            last_received_at_us_by_node: BTreeMap::from([("node-a".to_string(), 123)]),
            last_received_origins_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=2".to_string()],
            )]),
            received_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=111".to_string()],
            )]),
            stream_applied_batches_by_node: BTreeMap::from([("node-a".to_string(), 5)]),
            stream_applied_events_by_node: BTreeMap::from([("node-a".to_string(), 50)]),
            stream_applied_control_events_by_node: BTreeMap::from([("node-a".to_string(), 4)]),
            stream_applied_data_events_by_node: BTreeMap::from([("node-a".to_string(), 46)]),
            stream_applied_origin_counts_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["node-a::nfs1=50".to_string()],
            )]),
            stream_last_applied_at_us_by_node: BTreeMap::from([("node-a".to_string(), 321)]),
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs2".to_string()],
            )]),
            received_batches_by_node: BTreeMap::from([("node-b".to_string(), 22)]),
            received_events_by_node: BTreeMap::from([("node-b".to_string(), 222)]),
            received_control_events_by_node: BTreeMap::from([("node-b".to_string(), 3)]),
            received_data_events_by_node: BTreeMap::from([("node-b".to_string(), 219)]),
            last_received_at_us_by_node: BTreeMap::from([("node-b".to_string(), 456)]),
            last_received_origins_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=1".to_string()],
            )]),
            received_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=222".to_string()],
            )]),
            stream_applied_batches_by_node: BTreeMap::from([("node-b".to_string(), 6)]),
            stream_applied_events_by_node: BTreeMap::from([("node-b".to_string(), 60)]),
            stream_applied_control_events_by_node: BTreeMap::from([("node-b".to_string(), 5)]),
            stream_applied_data_events_by_node: BTreeMap::from([("node-b".to_string(), 55)]),
            stream_applied_origin_counts_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["node-b::nfs2=60".to_string()],
            )]),
            stream_last_applied_at_us_by_node: BTreeMap::from([("node-b".to_string(), 654)]),
            ..SinkStatusSnapshot::default()
        },
    ]);

    assert_eq!(merged.received_batches_by_node.get("node-a"), Some(&11));
    assert_eq!(merged.received_batches_by_node.get("node-b"), Some(&22));
    assert_eq!(merged.received_events_by_node.get("node-a"), Some(&111));
    assert_eq!(merged.received_events_by_node.get("node-b"), Some(&222));
    assert_eq!(
        merged.stream_applied_batches_by_node.get("node-a"),
        Some(&5)
    );
    assert_eq!(
        merged.stream_applied_batches_by_node.get("node-b"),
        Some(&6)
    );
    assert_eq!(
        merged.stream_applied_events_by_node.get("node-a"),
        Some(&50)
    );
    assert_eq!(
        merged.stream_applied_events_by_node.get("node-b"),
        Some(&60)
    );
    assert_eq!(
        merged.last_received_origins_by_node.get("node-a"),
        Some(&vec!["node-a::nfs1=2".to_string()])
    );
    assert_eq!(
        merged.last_received_origins_by_node.get("node-b"),
        Some(&vec!["node-b::nfs2=1".to_string()])
    );
    assert_eq!(
        merged.received_origin_counts_by_node.get("node-a"),
        Some(&vec!["node-a::nfs1=111".to_string()])
    );
    assert_eq!(
        merged.received_origin_counts_by_node.get("node-b"),
        Some(&vec!["node-b::nfs2=222".to_string()])
    );
}

#[test]
fn merge_sink_status_snapshots_unions_scheduled_groups_for_same_node_across_partial_snapshots() {
    let merged = merge_sink_status_snapshots(vec![
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            groups: vec![
                sink_group_status("nfs1", true),
                sink_group_status("nfs2", true),
            ],
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            groups: vec![sink_group_status("nfs1", true)],
            ..SinkStatusSnapshot::default()
        },
        SinkStatusSnapshot {
            scheduled_groups_by_node: BTreeMap::from([(
                "node-b".to_string(),
                vec!["nfs3".to_string()],
            )]),
            groups: vec![sink_group_status("nfs3", true)],
            ..SinkStatusSnapshot::default()
        },
    ]);

    assert_eq!(
        merged.scheduled_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "same-node partial sink-status snapshots must union scheduled groups instead of letting a later partial view erase nfs2"
    );
    assert_eq!(
        merged.scheduled_groups_by_node.get("node-b"),
        Some(&vec!["nfs3".to_string()]),
    );
}
