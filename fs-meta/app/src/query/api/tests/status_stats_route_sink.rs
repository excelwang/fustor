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

