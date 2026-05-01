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
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
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
async fn load_materialized_status_snapshots_falls_back_to_local_source_after_routed_source_status_timeout_without_route_retry()
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
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, _sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("materialized status snapshots after routed source-status timeout fallback");

    assert_eq!(
        boundary.send_batch_count(&source_status_route.0),
        1,
        "routed source-status timeout is bounded observation evidence; the query must not amplify it with same-request route retries",
    );
    assert_eq!(
        source_status
            .logical_roots
            .iter()
            .map(|root| root.root_id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "routed source-status timeout should fall back to package-local source readiness evidence for the same authority inputs",
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
                StatusRoutePlan::new(
                    Duration::from_secs(5),
                    STATUS_ROUTE_COLLECT_IDLE_GRACE,
                ),
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
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
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
    let boundary = Arc::new(SourceStatusOkSinkStatusTimeoutBoundary::new(
        source_status_payload,
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
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
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
async fn load_materialized_status_snapshots_uses_nonblocking_sink_evidence_when_route_sink_status_times_out()
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
        mount_point: node_a_root.clone(),
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
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
    let route_boundary = Arc::new(SourceStatusOkSinkStatusTimeoutBoundary::new(
        source_status_payload,
    ));
    let worker_boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let worker_socket_dir = tmp.path().join("worker-sockets");
    fs::create_dir_all(&worker_socket_dir).expect("create worker socket dir");
    let worker_binding = RuntimeWorkerBinding {
        role_id: "sink".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(tmp.path().join("missing-worker-module.so")),
        socket_dir: Some(worker_socket_dir),
    };
    let worker_factory = RuntimeWorkerClientFactory::new(
        worker_boundary.clone(),
        worker_boundary,
        in_memory_state_boundary(),
    );
    let worker_config = SourceConfig {
        roots: vec![external_worker_root("nfs1", &node_a_root)],
        host_object_grants: grants.clone(),
        ..SourceConfig::default()
    };
    let worker_client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            worker_config,
            worker_binding,
            worker_factory,
        )
        .expect("construct sink worker client"),
    );
    assert!(
        worker_client
            .shared_worker_existing_client_for_tests()
            .await
            .expect("read initial sink worker client")
            .is_none(),
        "test setup should start with no live sink worker client"
    );
    let sink = Arc::new(SinkFacade::worker(worker_client.clone()));
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: route_boundary.clone(),
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("route sink-status timeout should degrade to nonblocking sink evidence");

    assert_eq!(source_status.current_stream_generation, Some(9));
    assert!(
        sink_status.groups.is_empty(),
        "not-started worker fallback should provide cached degraded sink evidence instead of live materialization"
    );
    assert_eq!(
        route_boundary.send_batch_count(&source_status_route.0),
        1,
        "source status should still be collected independently"
    );
    assert_eq!(
        route_boundary.send_batch_count(&sink_status_route.0),
        1,
        "sink status should make one routed attempt before degrading"
    );
    assert!(
        worker_client
            .shared_worker_existing_client_for_tests()
            .await
            .expect("read final sink worker client")
            .is_none(),
        "trusted-materialized readiness fan-in must not start the local sink worker after routed sink-status timeout"
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
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink_facade_with_group(&grants)),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(None)),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
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
