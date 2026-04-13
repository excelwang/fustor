include!("pit_public/exact_file_missing_dir_follow_up.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_excludes_unscheduled_groups_after_root_transition() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_a_nfs4 = tmp.path().join("node-a-nfs4");
    fs::create_dir_all(node_a_nfs2.join("live-layout")).expect("create node-a nfs2 dir");
    fs::create_dir_all(node_a_nfs4.join("retired-layout")).expect("create node-a nfs4 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs2,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs4,
            fs_source: "nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let mut nfs2 = RootSpec::new("nfs2", "/unused");
    nfs2.selector = crate::source::config::RootSelector {
        fs_type: Some("nfs".to_string()),
        ..crate::source::config::RootSelector::default()
    };
    nfs2.subpath_scope = std::path::PathBuf::from("/");
    let mut nfs4 = RootSpec::new("nfs4", "/unused");
    nfs4.selector = crate::source::config::RootSelector {
        fs_type: Some("nfs".to_string()),
        ..crate::source::config::RootSelector::default()
    };
    nfs4.subpath_scope = std::path::PathBuf::from("/");
    let source = source_facade_with_roots(vec![nfs2, nfs4], &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let routed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let routed_groups_for_handler = routed_groups.clone();
    let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![sink_group_status("nfs2", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink status snapshot");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let status_payload = status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-a",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let routed_groups = routed_groups_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode node-a internal query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-a request");
                    routed_groups
                        .lock()
                        .expect("routed groups lock")
                        .push(group_id.clone());
                    if group_id == "nfs2" {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("node-a request correlation"),
                            real_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                    }
                }
                responses
            }
        },
    );

    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
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
    let params = NormalizedApiParams {
        path: b"/retired-layout".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let session = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &params,
        Duration::from_millis(250),
        ObservationStatus::fresh_only(),
        None,
    )
    .await
    .expect("build tree pit session");

    assert_eq!(
        session
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs2"],
        "tree PIT should exclude groups that current sink scheduling does not bind/serve; sink_status_calls={} routed_groups={:?}",
        boundary.send_batch_count(&sink_status_route.0),
        routed_groups.lock().expect("routed groups lock").clone(),
    );
    assert_eq!(
        routed_groups.lock().expect("routed groups lock").as_slice(),
        &["nfs2".to_string()],
        "materialized tree PIT should only route the currently scheduled group"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_preserves_materialized_target_groups_when_later_sink_status_snapshot_is_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_nfs1 = tmp.path().join("node-a-nfs1");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_a_nfs3 = tmp.path().join("node-a-nfs3");
    fs::create_dir_all(node_a_nfs1.join("live-layout")).expect("create node-a nfs1 dir");
    fs::create_dir_all(node_a_nfs2.join("live-layout")).expect("create node-a nfs2 dir");
    fs::create_dir_all(node_a_nfs3.join("live-layout")).expect("create node-a nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs1,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs2,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs3".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_nfs3,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let sink_status_call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let call_count_for_handler = sink_status_call_count.clone();
    let full_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode full sink status snapshot");
    let empty_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot::default())
        .expect("encode empty sink status snapshot");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let full_status_payload = full_status_payload.clone();
            let empty_status_payload = empty_status_payload.clone();
            let call_count = call_count_for_handler.clone();
            async move {
                let payload = if call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    full_status_payload
                } else {
                    empty_status_payload
                };
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-a",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a internal query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
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
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let first = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &params,
        Duration::from_millis(250),
        ObservationStatus::fresh_only(),
        None,
    )
    .await
    .expect("first tree pit session");
    assert_eq!(
        first
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2", "nfs3"],
        "first materialized tree PIT should include all scheduled groups"
    );

    let second = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &params,
        Duration::from_millis(250),
        ObservationStatus::fresh_only(),
        None,
    )
    .await
    .expect("second tree pit session");
    assert_eq!(
        second
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2", "nfs3"],
        "later empty sink-status snapshots must not clear previously observed materialized target groups"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_prefers_cached_sink_primary_when_later_sink_status_snapshot_is_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    fs::create_dir_all(node_b_root.join("layout-b")).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
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
            object_ref: "node-b::nfs4".to_string(),
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
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let node_b_route = sink_query_request_route_for("node-b");
    let node_a_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_b_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_a_groups_for_handler = node_a_groups.clone();
    let node_b_groups_for_handler = node_b_groups.clone();
    let sink_status_call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink_status_call_count_for_handler = sink_status_call_count.clone();
    let full_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs4".to_string()]),
            ("node-b".to_string(), vec!["nfs4".to_string()]),
        ]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode full sink status snapshot");
    let empty_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot::default())
        .expect("encode empty sink status snapshot");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let full_status_payload = full_status_payload.clone();
            let empty_status_payload = empty_status_payload.clone();
            let sink_status_call_count = sink_status_call_count_for_handler.clone();
            async move {
                let payload = if sink_status_call_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    full_status_payload
                } else {
                    empty_status_payload
                };
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-stale-source-primary-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let node_a_groups = node_a_groups_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode node-a internal query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-a request");
                    node_a_groups
                        .lock()
                        .expect("node-a groups lock")
                        .push(group_id.clone());
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-a request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-sink-primary-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let node_b_groups = node_b_groups_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode node-b internal query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-b request");
                    node_b_groups
                        .lock()
                        .expect("node-b groups lock")
                        .push(group_id.clone());
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );

    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary: boundary.clone(),
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
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let first = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &params,
        Duration::from_millis(250),
        ObservationStatus::fresh_only(),
        None,
    )
    .await
    .expect("first tree pit session");
    assert_eq!(first.groups.len(), 1);
    assert_eq!(first.groups[0].group, "nfs4");
    assert!(
        first.groups[0].root.as_ref().expect("first root").exists,
        "first tree PIT should use the fresh sink primary and materialize nfs4"
    );

    let second = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &params,
        Duration::from_millis(250),
        ObservationStatus::fresh_only(),
        None,
    )
    .await
    .expect("second tree pit session after later empty sink status");
    assert_eq!(second.groups.len(), 1);
    assert_eq!(second.groups[0].group, "nfs4");
    assert!(
        second.groups[0].root.as_ref().expect("second root").exists,
        "later empty sink-status snapshots must not make build_tree_pit_session fall back to a stale source-primary owner; node_a_groups={:?} node_b_groups={:?}",
        node_a_groups.lock().expect("node-a groups lock").clone(),
        node_b_groups.lock().expect("node-b groups lock").clone(),
    );
    assert!(
        node_a_groups.lock().expect("node-a groups lock").is_empty(),
        "stale source-primary owner should not receive the later selected-group materialized read once a cached sink primary exists; node_a_groups={:?} node_b_groups={:?}",
        node_a_groups.lock().expect("node-a groups lock").clone(),
        node_b_groups.lock().expect("node-b groups lock").clone(),
    );
    assert_eq!(
        node_b_groups.lock().expect("node-b groups lock").as_slice(),
        &["nfs4".to_string(), "nfs4".to_string()],
        "sink primary should serve both the initial and later selected-group materialized reads"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_times_out_stalled_selected_group_route_after_ranking() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(&root).expect("create node-a dir");
    fs::write(root.join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let boundary = Arc::new(MaterializedSelectedGroupTreeStallBoundary::new(
        owner_route.0.clone(),
        sink_status_route.0,
    ));
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::FileCount,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(1200),
            ObservationStatus::fresh_only(),
            None,
        ),
    )
    .await
    .expect("selected-group materialized route stall should not hang tree PIT beyond query timeout")
    .expect("build tree pit session");

    assert_eq!(
        session.groups.len(),
        1,
        "single-group ranking fixture should still produce one PIT group snapshot"
    );
    assert_eq!(session.groups[0].group, "nfs1");
    assert_eq!(
        session.groups[0]
            .root
            .as_ref()
            .expect("fallback empty root snapshot")
            .exists,
        false,
        "stalled selected-group owner route should degrade into an empty materialized group payload instead of hanging `/tree`"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_ready_group_fails_closed_when_selected_group_route_stalls(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(&root).expect("create node-a dir");
    fs::write(root.join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let boundary = Arc::new(MaterializedSelectedGroupTreeStallBoundary::new(
        owner_route.0.clone(),
        sink_status_route.0,
    ));
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::FileCount,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1200),
                ObservationStatus::fresh_only(),
                None,
            ),
        )
        .await
        .expect("trusted-materialized stalled selected-group route should fail within the shared tree PIT timeout budget")
        .expect_err("trusted-materialized ready group must fail closed instead of degrading into an empty ok tree snapshot");

    assert!(
        matches!(err, CnxError::Timeout),
        "trusted-materialized ready group should surface the stalled selected-group route timeout instead of settling an empty tree: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_file_count_reranks_first_page_after_late_richer_group_recovery(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("nfs1")).expect("create node-a nfs1 dir");
    fs::create_dir_all(root_a.join("nfs2")).expect("create node-a nfs2 dir");
    fs::create_dir_all(root_b.join("nfs3")).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs1"),
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs2"),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.join("nfs3"),
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-file-count-rerank-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let node_a_route = sink_query_request_route_for("node-a");
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-file-count-rerank-node-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-a request correlation");
                match params.op {
                    QueryOp::Stats => {
                        let stats = if group_id == "nfs1" {
                            SubtreeStats::default()
                        } else {
                            SubtreeStats {
                                total_nodes: if group_id == "nfs2" { 7 } else { 9 },
                                total_files: if group_id == "nfs2" { 7 } else { 0 },
                                total_dirs: 0,
                                total_size: 0,
                                latest_file_mtime_us: Some(1),
                                ..SubtreeStats::default()
                            }
                        };
                        let payload =
                            rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(stats))
                                .expect("encode node-a stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        if group_id == "nfs1" {
                            responses.push(Event::new(
                                EventMetadata {
                                    origin_id: NodeId(group_id.clone()),
                                    timestamp_us: 1,
                                    logical_ts: None,
                                    correlation_id: Some(correlation),
                                    ingress_auth: None,
                                    trace: None,
                                },
                                Bytes::from(empty_materialized_tree_payload_for_test(
                                    &params.scope.path,
                                )),
                            ));
                            responses.push(Event::new(
                                EventMetadata {
                                    origin_id: NodeId(group_id.clone()),
                                    timestamp_us: 2,
                                    logical_ts: None,
                                    correlation_id: Some(correlation),
                                    ingress_auth: None,
                                    trace: None,
                                },
                                Bytes::from(real_materialized_tree_payload_with_entries_for_test(
                                    &params.scope.path,
                                    &[
                                        b"/data",
                                        b"/data/a.txt",
                                        b"/data/b.txt",
                                        b"/latest-age.txt",
                                        b"/nested",
                                        b"/nested/child",
                                        b"/nested/child/deep.txt",
                                        b"/root.txt",
                                        b"/z.txt",
                                    ],
                                )),
                            ));
                        } else {
                            responses.push(mk_event_with_correlation(
                                &group_id,
                                correlation,
                                real_materialized_tree_payload_with_entries_for_test(
                                    &params.scope.path,
                                    &[
                                        b"/data",
                                        b"/data/a.txt",
                                        b"/data/b.txt",
                                        b"/latest-age.txt",
                                        b"/nested",
                                        b"/nested/peer.txt",
                                        b"/root.txt",
                                    ],
                                ),
                            ));
                        }
                    }
                }
            }
            responses
        },
    );
    let node_b_route = sink_query_request_route_for("node-b");
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-file-count-rerank-node-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-b request correlation");
                match params.op {
                    QueryOp::Stats => {
                        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                            SubtreeStats {
                                total_nodes: 5,
                                total_files: 5,
                                total_dirs: 0,
                                total_size: 0,
                                latest_file_mtime_us: Some(1),
                                ..SubtreeStats::default()
                            },
                        ))
                        .expect("encode node-b stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            correlation,
                            real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[
                                    b"/a.txt",
                                    b"/b.txt",
                                    b"/c.txt",
                                    b"/nested",
                                    b"/nested/leaf.txt",
                                ],
                            ),
                        ));
                    }
                }
            }
            responses
        },
    );

    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let session = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/".to_vec(),
            group: None,
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::FileCount,
            group_page_size: Some(1),
            group_after: None,
            entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        Duration::from_millis(3400),
        ObservationStatus::trusted_materialized(),
        Some(&request_sink_status),
    )
    .await
    .expect("build trusted materialized file-count pit session");

    assert_eq!(
        session.groups.first().map(|group| group.group.as_str()),
        Some("nfs1"),
        "late richer first-ranked group recovery must still win the first file-count page after full tree payloads decode: {:?}",
        session
            .groups
            .iter()
            .map(|group| (
                group.group.clone(),
                group.root.as_ref().map(|root| root.exists),
                group.entries.len()
            ))
            .collect::<Vec<_>>()
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_file_age_reranks_first_page_after_late_newer_group_recovery(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(root_a.join("nfs1")).expect("create node-a nfs1 dir");
    fs::create_dir_all(root_a.join("nfs2")).expect("create node-a nfs2 dir");
    fs::create_dir_all(root_b.join("nfs3")).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs1"),
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs2"),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.join("nfs3"),
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-file-age-rerank-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let node_a_route = sink_query_request_route_for("node-a");
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-file-age-rerank-node-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-a request correlation");
                match params.op {
                    QueryOp::Stats => {
                        let stats = if group_id == "nfs1" {
                            SubtreeStats {
                                latest_file_mtime_us: Some(5),
                                ..SubtreeStats::default()
                            }
                        } else {
                            SubtreeStats {
                                latest_file_mtime_us: Some(3),
                                ..SubtreeStats::default()
                            }
                        };
                        let payload =
                            rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(stats))
                                .expect("encode node-a stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        if group_id == "nfs1" {
                            responses.push(Event::new(
                                EventMetadata {
                                    origin_id: NodeId(group_id.clone()),
                                    timestamp_us: 1,
                                    logical_ts: None,
                                    correlation_id: Some(correlation),
                                    ingress_auth: None,
                                    trace: None,
                                },
                                Bytes::from(empty_materialized_tree_payload_for_test(
                                    &params.scope.path,
                                )),
                            ));
                            responses.push(Event::new(
                                EventMetadata {
                                    origin_id: NodeId(group_id.clone()),
                                    timestamp_us: 2,
                                    logical_ts: None,
                                    correlation_id: Some(correlation),
                                    ingress_auth: None,
                                    trace: None,
                                },
                                Bytes::from(
                                    real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                        &params.scope.path,
                                        &[
                                            b"/data",
                                            b"/data/a.txt",
                                            b"/data/b.txt",
                                            b"/nested",
                                            b"/nested/child",
                                            b"/nested/child/deep.txt",
                                            b"/root.txt",
                                        ],
                                        4,
                                        5,
                                    ),
                                ),
                            ));
                        } else {
                            responses.push(mk_event_with_correlation(
                                &group_id,
                                correlation,
                                real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                    &params.scope.path,
                                    &[b"/older.txt"],
                                    2,
                                    3,
                                ),
                            ));
                        }
                    }
                }
            }
            responses
        },
    );
    let node_b_route = sink_query_request_route_for("node-b");
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-file-age-rerank-node-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-b request correlation");
                match params.op {
                    QueryOp::Stats => {
                        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                            SubtreeStats {
                                latest_file_mtime_us: Some(9),
                                ..SubtreeStats::default()
                            },
                        ))
                        .expect("encode node-b stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            correlation,
                            real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                &params.scope.path,
                                &[b"/freshest.txt", b"/nested", b"/nested/leaf.txt"],
                                8,
                                9,
                            ),
                        ));
                    }
                }
            }
            responses
        },
    );

    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let session = build_tree_pit_session(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/".to_vec(),
            group: None,
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::FileAge,
            group_page_size: Some(1),
            group_after: None,
            entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        Duration::from_millis(3400),
        ObservationStatus::trusted_materialized(),
        Some(&request_sink_status),
    )
    .await
    .expect("build trusted materialized file-age pit session");

    assert_eq!(
        session.groups.first().map(|group| group.group.as_str()),
        Some("nfs3"),
        "late newer group recovery must still win the first file-age page after full tree payloads decode: {:?}",
        session
            .groups
            .iter()
            .map(|group| (
                group.group.clone(),
                group.root.as_ref().map(|root| root.modified_time_us),
                group
                    .entries
                    .iter()
                    .map(|entry| entry.modified_time_us)
                    .max()
            ))
            .collect::<Vec<_>>()
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_file_age_first_page_shares_budget_after_ranking_route_failure_and_top_group_decode(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs1"),
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.join("nfs2"),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.join("nfs3"),
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-file-age-shared-budget-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let node_a_route = sink_query_request_route_for("node-a");
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-file-age-shared-budget-node-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-a request correlation");
                match params.op {
                    QueryOp::Stats => {
                        if group_id == "nfs2" {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        let latest_file_mtime_us = match group_id.as_str() {
                            "nfs1" => Some(5),
                            "nfs2" => Some(3),
                            other => panic!("unexpected stats group on node-a: {other}"),
                        };
                        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                            SubtreeStats {
                                latest_file_mtime_us,
                                ..SubtreeStats::default()
                            },
                        ))
                        .expect("encode node-a stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        if group_id == "nfs1" {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        let payload = match group_id.as_str() {
                            "nfs1" => {
                                real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                    &params.scope.path,
                                    &[b"/steady.txt"],
                                    4,
                                    5,
                                )
                            }
                            "nfs2" => {
                                real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                    &params.scope.path,
                                    &[b"/older.txt"],
                                    2,
                                    3,
                                )
                            }
                            other => panic!("unexpected tree group on node-a: {other}"),
                        };
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                }
            }
            responses
        },
    );
    let node_b_route = sink_query_request_route_for("node-b");
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-file-age-shared-budget-node-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("node-b request correlation");
                match params.op {
                    QueryOp::Stats => {
                        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                            SubtreeStats {
                                latest_file_mtime_us: Some(9),
                                ..SubtreeStats::default()
                            },
                        ))
                        .expect("encode node-b stats payload");
                        responses.push(mk_event_with_correlation(&group_id, correlation, payload));
                    }
                    QueryOp::Tree => {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            correlation,
                            real_materialized_tree_payload_with_entries_and_mtime_for_test(
                                &params.scope.path,
                                &[b"/freshest.txt"],
                                8,
                                9,
                            ),
                        ));
                    }
                }
            }
            responses
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-file-age-shared-budget-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );

    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::FileAge,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_millis(4700),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3000),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted-materialized file-age first page must settle inside the original PIT budget even when ranking consumes budget before a later-ranked tree stall")
        .expect_err(
            "trusted-materialized file-age first page must fail closed instead of resetting the PIT budget after ranking and hanging after the top-ranked group decodes",
        );

    assert!(
        matches!(err, CnxError::Timeout | CnxError::NotReady(_)),
        "trusted-materialized file-age first page should settle as timeout/not-ready once the shared PIT budget is exhausted across ranking and tree stages: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_bounds_first_ranked_ready_group_stage_under_large_caller_timeout(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(&root).expect("create node-a dir");
    fs::write(root.join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode ready sink-status payload");
    let boundary = Arc::new(
        MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload(
            owner_route.0.clone(),
            sink_status_route.0,
            sink_status_payload,
        ),
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_secs(4),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_secs(60),
                ObservationStatus::fresh_only(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect(
            "trusted-materialized first-ranked ready group stage must stay bounded even when the caller timeout is large",
        )
        .expect_err(
            "trusted-materialized first-ranked ready group must fail closed instead of consuming the full caller timeout budget",
        );

    assert!(
        matches!(err, CnxError::Timeout),
        "bounded first-ranked trusted-ready stage should still surface a timeout-like failure: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_retries_empty_selected_group_once_before_fail_closed(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().join("node-a");
    fs::create_dir_all(&root).expect("create node-a dir");
    fs::write(root.join("seed.txt"), b"a").expect("seed node-a file");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs1".to_string(),
        host_ref: "node-a".to_string(),
        host_ip: "10.0.0.1".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: std::collections::BTreeMap::new(),
        mount_point: root,
        fs_source: "nfs".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: Vec::new(),
        interfaces: Vec::new(),
        active: true,
    }];
    let source = source_facade_with_group("nfs1", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let owner_call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_call_count_for_handler = owner_call_count.clone();
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-a",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-empty-then-real-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_call_count = owner_call_count_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner request");
                    let payload = if owner_call_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        == 0
                    {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(1),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let session = build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        )
        .await
        .expect(
            "trusted-materialized ready group should retry a transient empty selected-group tree once before failing closed",
        );

    assert_eq!(session.groups.len(), 1);
    assert!(
        session.groups[0]
            .root
            .as_ref()
            .is_some_and(|root| root.exists),
        "trusted-materialized ready group should settle on the later real payload after one transient empty owner reply"
    );
    assert_eq!(
        owner_call_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "trusted-materialized empty selected-group retry should reissue the owner route once"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_uses_outer_retry_budget_when_later_ranked_group_first_reply_is_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-later-ranked-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-later-ranked-empty-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_call_count_for_handler = owner_call_count.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-later-ranked-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-later-ranked-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_call_count = owner_call_count_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    let nth = owner_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if nth == 0 {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-b request correlation"),
                            empty_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                        continue;
                    }
                    tokio::time::sleep(Duration::from_millis(650)).await;
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("later-ranked trusted group should settle within the shared PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session
            .groups
            .iter()
            .all(|group| group.root.as_ref().is_some_and(|root| root.exists)),
        "later-ranked trusted group should use the outer PIT retry budget instead of exhausting an inner owner retry: {:?}",
        session
            .groups
            .iter()
            .map(|group| (
                group.group.clone(),
                group.root.as_ref().map(|root| root.exists)
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_call_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "later-ranked trusted group should retry owner exactly once via the outer PIT retry path"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_recovers_later_ranked_root_after_proxy_continuity_gap_follows_empty_owner_reply(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-later-ranked-proxy-gap-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-later-ranked-proxy-gap-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                if group_id == "nfs1" {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let owner_nfs2_call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_nfs2_call_count_for_handler = owner_nfs2_call_count.clone();
    let owner_route = sink_query_request_route_for("node-a");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-trusted-later-ranked-proxy-gap-owner-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_nfs2_call_count = owner_nfs2_call_count_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner request");
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        let nth =
                            owner_nfs2_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if nth == 0 {
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            tokio::time::sleep(Duration::from_millis(650)).await;
                            real_materialized_tree_payload_for_test(&params.scope.path)
                        }
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("later-ranked trusted root must settle within the outer PIT budget when the proxy branch hits a continuity gap after the first group already decoded")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    assert!(
        session
            .groups
            .iter()
            .all(|group| group.root.as_ref().is_some_and(|root| root.exists)),
        "later-ranked trusted root should recover through the outer PIT retry path instead of timing out after an empty owner reply and proxy continuity gap: {:?}",
        session
            .groups
            .iter()
            .map(|group| (
                group.group.clone(),
                group.root.as_ref().map(|root| root.exists)
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_nfs2_call_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "later-ranked trusted root should reissue the owner once after the proxy continuity gap"
    );
    assert!(
        boundary.send_batch_count(&proxy_route.0) >= 1,
        "later-ranked trusted root should still attempt the proxy branch before reissuing the owner"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

pit_public_missing_dir_follow_up_tests!();

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_later_ranked_non_root_subtree_owner_remains_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-later-ranked-non-root-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-later-ranked-non-root-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-later-ranked-non-root-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let owner_b_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_b_calls_for_handler = owner_b_calls.clone();
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-later-ranked-non-root-owner-b-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_b_calls = owner_b_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-b query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-b request");
                        if group_id == "nfs3" {
                            owner_b_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let payload = if group_id == "nfs3" {
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_for_test(&params.scope.path)
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-b request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted later-ranked non-root proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted later-ranked non-root subtree should fall back to generic proxy when the owner keeps returning an empty subtree despite ready sink status: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_b_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "later-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_middle_ranked_non_root_subtree_owner_remains_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-owner-a-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-a query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-a request");
                        if group_id == "nfs2" {
                            owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let payload = if group_id == "nfs2" {
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_for_test(&params.scope.path)
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted middle-ranked non-root proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted middle-ranked non-root subtree should fall back to generic proxy when the owner keeps returning an empty subtree despite ready sink status: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "middle-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_delayed_middle_ranked_non_root_reply_after_owner_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-delayed-proxy-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let proxy_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proxy_calls_for_handler = proxy_calls.clone();
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| {
            let proxy_calls = proxy_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("proxy selected group");
                    if group_id == "nfs2" {
                        proxy_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(700)).await;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-owner-a-delayed-proxy-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-a query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-a request");
                        if group_id == "nfs2" {
                            owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let payload = if group_id == "nfs2" {
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_for_test(&params.scope.path)
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-owner-b-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted middle-ranked delayed proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted middle-ranked non-root subtree should keep enough proxy budget to replace an empty owner result with the delayed proxy reply: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "middle-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );
    assert_eq!(
        proxy_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "middle-ranked non-root trusted subtree should issue exactly one delayed proxy retry for nfs2"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_middle_ranked_non_root_proxy_times_out_after_owner_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-timeout-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-timeout-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("proxy selected group");
                if group_id == "nfs2" {
                    tokio::time::sleep(Duration::from_millis(3_000)).await;
                }
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_with_entries_for_test(
                        &params.scope.path,
                        &[b"/data/a.txt", b"/data/b.txt"],
                    ),
                ));
            }
            responses
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-timeout-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let payload = if group_id == "nfs2" {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        )
                    };
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-timeout-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        ),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let err = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted middle-ranked non-root proxy-timeout path should settle within the PIT budget")
        .expect_err(
            "trusted middle-ranked non-root subtree must fail closed when the owner stays empty and the proxy retry times out after a prior group already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted middle-ranked non-root subtree proxy timeout should fail closed instead of settling an ok empty root: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_middle_ranked_non_root_proxy_returns_error_after_owner_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-proxy-error-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-proxy-error-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("proxy selected group");
                    let payload = if group_id == "nfs2" {
                        b"operation timed out".to_vec()
                    } else {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        )
                    };
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-proxy-error-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let payload = if group_id == "nfs2" {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        )
                    };
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-proxy-error-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        ),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let err = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted middle-ranked non-root proxy-error path should settle within the PIT budget")
        .expect_err(
            "trusted middle-ranked non-root subtree must fail closed when the owner stays empty and the proxy retry only returns an error event after a prior group already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted middle-ranked non-root subtree proxy error should fail closed instead of settling an ok empty root: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_delayed_later_ranked_non_root_reply_after_owner_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-later-ranked-non-root-delayed-proxy-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let proxy_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proxy_calls_for_handler = proxy_calls.clone();
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-later-ranked-non-root-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| {
            let proxy_calls = proxy_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("proxy selected group");
                    if group_id == "nfs3" {
                        proxy_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(700)).await;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-later-ranked-non-root-owner-a-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_b_calls_for_handler = owner_b_calls.clone();
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-later-ranked-non-root-owner-b-delayed-proxy-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_b_calls = owner_b_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-b query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-b request");
                        if group_id == "nfs3" {
                            owner_b_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let payload = if group_id == "nfs3" {
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_for_test(&params.scope.path)
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-b request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(1600),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted later-ranked delayed proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted later-ranked non-root subtree should keep enough proxy budget to replace an empty owner result with the delayed proxy reply: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_b_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "later-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );
    assert_eq!(
        proxy_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "later-ranked non-root trusted subtree should issue exactly one delayed proxy retry for nfs3"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_non_root_proxy_times_out_after_owner_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-later-ranked-non-root-proxy-timeout-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-later-ranked-non-root-proxy-timeout-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            Vec::<Event>::new()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-later-ranked-non-root-proxy-timeout-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        ),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-later-ranked-non-root-proxy-timeout-owner-b-empty-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let err = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1600),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted later-ranked non-root proxy-timeout path should settle within the PIT budget")
        .expect_err(
            "trusted later-ranked non-root subtree must fail closed when the owner stays empty and the proxy never returns a materialized reply after prior groups already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted later-ranked non-root owner+proxy timeout should fail closed as NotReady instead of settling an ok empty root: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_uses_one_shared_timeout_budget_across_multiple_stalled_groups() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
    .expect("encode multi-group sink-status payload");
    let boundary = Arc::new(
        MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload(
            owner_route.0.clone(),
            sink_status_route.0,
            sink_status_payload,
        ),
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let session = tokio::time::timeout(
            Duration::from_millis(2500),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1200),
                ObservationStatus::fresh_only(),
                None,
            ),
        )
        .await
        .expect("multi-group stalled selected-group routes must share one PIT timeout budget instead of multiplying it per group")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    assert!(session
        .groups
        .iter()
        .all(|group| group.root.as_ref().is_some_and(|root| !root.exists)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_completes_when_one_ranked_group_tree_stalls_after_another_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let boundary = Arc::new(
            MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload_and_successful_tree_group(
                owner_route.0.clone(),
                sink_status_route.0,
                sink_status_payload,
                Some("nfs1".to_string()),
            ),
        );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let session = tokio::time::timeout(
            Duration::from_millis(2500),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1200),
                ObservationStatus::fresh_only(),
                None,
            ),
        )
        .await
        .expect("mixed decoded+stalled selected-group routes must share one PIT timeout budget instead of hanging the whole tree PIT")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    assert_eq!(session.groups[0].group, "nfs1");
    assert!(session.groups[0]
        .root
        .as_ref()
        .is_some_and(|root| root.exists));
    assert_eq!(session.groups[1].group, "nfs2");
    assert!(session.groups[1]
        .root
        .as_ref()
        .is_some_and(|root| !root.exists));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_completes_when_later_ranked_group_reply_is_delayed_but_within_shared_budget(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-a-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-a nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs3".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "running".into(),
        host_object_grants_version: 1,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
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
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-multi-group-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-a",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut source_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        source_status_route.clone(),
        "test-multi-group-source-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let source_status_payload = source_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-a",
                            req.metadata()
                                .correlation_id
                                .expect("source-status request correlation"),
                            source_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-later-ranked-delayed-owner-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                if group_id == "nfs3" {
                    tokio::time::sleep(Duration::from_millis(650)).await;
                }
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::Materialized),
    };

    let session = tokio::time::timeout(
        Duration::from_secs(4),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(1800),
            ObservationStatus::fresh_only(),
            None,
        ),
    )
    .await
    .expect(
        "later-ranked delayed materialized reply should still settle within the shared PIT budget",
    )
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session
            .groups
            .iter()
            .all(|group| { group.root.as_ref().is_some_and(|root| root.exists) }),
        "later-ranked delayed owner reply should still yield materialized roots for all groups instead of timing out the whole PIT: {:?}",
        session
            .groups
            .iter()
            .map(|group| (
                group.group.clone(),
                group.root.as_ref().map(|root| root.exists)
            ))
            .collect::<Vec<_>>()
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    source_status_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_group_tree_stalls_after_first_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let boundary = Arc::new(
            MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload_and_successful_tree_group(
                owner_route.0.clone(),
                sink_status_route.0,
                sink_status_payload,
                Some("nfs1".to_string()),
            ),
        );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_millis(2500),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1200),
                ObservationStatus::fresh_only(),
                None,
            ),
        )
        .await
        .expect("trusted-materialized mixed decoded+stalled ranked groups must finish within the shared PIT timeout budget")
        .expect_err("trusted-materialized PIT must fail closed instead of hanging after the first ranked group already decoded");

    assert!(
        matches!(err, CnxError::Timeout),
        "trusted-materialized later ranked group stall should surface the shared PIT timeout: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_bounds_later_ranked_group_stage_after_first_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let boundary = Arc::new(
            MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload_and_successful_tree_group(
                owner_route.0.clone(),
                sink_status_route.0,
                sink_status_payload,
                Some("nfs1".to_string()),
            ),
        );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_secs(4),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_secs(60),
                ObservationStatus::fresh_only(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect(
            "trusted-materialized later-ranked group stage must stay bounded after an earlier group already decoded, even when the caller timeout is large",
        )
        .expect_err("trusted-materialized later-ranked stalled group must fail closed instead of consuming the full caller timeout budget");

    assert!(
        matches!(err, CnxError::Timeout | CnxError::NotReady(_)),
        "bounded later-ranked trusted group stage should still settle as timeout/not-ready: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_bounds_third_group_stage_after_first_and_second_groups_decode(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-a-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-a nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs3".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let boundary = Arc::new(
            MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload_and_delayed_successful_tree_group(
                owner_route.0.clone(),
                sink_status_route.0,
                sink_status_payload,
                Some("nfs1".to_string()),
                Some("nfs2".to_string()),
                Duration::from_millis(200),
            ),
        );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_secs(4),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_secs(60),
                ObservationStatus::fresh_only(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect(
            "trusted-materialized third-group stage must stay bounded after the first two groups already decoded, even when the caller timeout is large",
        )
        .expect_err("trusted-materialized third stalled group must fail closed instead of consuming the full caller timeout budget");

    assert!(
        matches!(err, CnxError::Timeout | CnxError::NotReady(_)),
        "bounded third trusted group stage should still settle as timeout/not-ready: {err:?}"
    );
}

pit_public_exact_file_missing_dir_follow_up_tests!();

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_later_ranked_root_group_owner_times_out_after_prior_groups_decode(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-root-ready-sink-status-timeout-fallback-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-root-proxy-timeout-fallback-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-root-owner-a-timeout-fallback-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-root-owner-b-timeout-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(900)).await;
            Vec::<Event>::new()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted root timeout fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/")
        }),
        "trusted root query should fall back to generic proxy when a later-ranked owner times out after prior groups already decoded: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_root_proxy_protocol_violation_follows_owner_timeout_after_two_groups_decode(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-root-proxy-protocol-gap-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-root-proxy-protocol-gap-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let Some(group_id) = params.scope.selected_group.clone() else {
                    continue;
                };
                if group_id == "nfs3" {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation")
                            + 1,
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-root-owner-a-protocol-gap-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-root-owner-b-timeout-before-protocol-gap-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(900)).await;
            Vec::<Event>::new()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let err = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted root proxy protocol-gap path should settle within the PIT budget")
        .expect_err(
            "trusted-materialized PIT must fail closed when the later-ranked root proxy only returns protocol-violating batches after nfs1 and nfs2 already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted-materialized later-ranked root proxy protocol-gap path should fail closed as NotReady instead of leaking Timeout: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_group_returns_no_payload_after_first_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |_| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                vec![mk_event_with_correlation(
                    "sink-status-ready",
                    1,
                    sink_status_payload.clone(),
                )]
            }
        },
    );
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-selected-group-empty-after-first-decode-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                if group_id == "nfs1" {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                } else if group_id == "nfs2" {
                    responses.push(mk_event_with_correlation(
                        "other-group",
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(1800),
            ObservationStatus::fresh_only(),
            None,
        )
        .await
        .expect_err(
            "trusted-materialized PIT must fail closed when a later ranked ready group returns no payload after an earlier group already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted-materialized later ranked group with no payload should fail closed instead of rendering an ok empty root: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_group_returns_empty_root_after_first_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route = sink_query_request_route_for("node-a");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "sink-status-ready",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-empty-later-ranked-owner-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner request");
                let payload = if group_id == "nfs1" {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-later-ranked-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                let payload = if group_id == "nfs1" {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(2),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_millis(2500),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1800),
                ObservationStatus::fresh_only(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted-materialized later-ranked empty-root failure should finish within the shared PIT timeout budget")
        .expect_err(
            "trusted-materialized PIT must fail closed when a later ranked ready group keeps returning an empty root after an earlier group already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted-materialized later-ranked ready group empty root should fail closed instead of timing out the whole PIT: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_middle_ranked_group_returns_empty_root_but_later_group_decodes(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let owner_route_a = sink_query_request_route_for("node-a");
    let owner_route_b = sink_query_request_route_for("node-b");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-middle-ranked-root-empty-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "sink-status-ready",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_a.clone(),
        "test-middle-ranked-root-empty-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner-a query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner-a request");
                let payload = if group_id == "nfs2" {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner-a request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_b.clone(),
        "test-middle-ranked-root-empty-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-middle-ranked-root-empty-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy request");
                let payload = if group_id == "nfs2" {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };

    let err = tokio::time::timeout(
            Duration::from_millis(2500),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(1800),
                ObservationStatus::fresh_only(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted-materialized middle-ranked empty-root failure should finish within the shared PIT timeout budget")
        .expect_err(
            "trusted-materialized PIT must fail closed when a middle-ranked ready group returns an empty root even if a later group can still decode",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted-materialized middle-ranked ready group empty root should fail closed instead of rendering an ok body: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[test]
fn materialized_owner_node_for_group_prefers_scheduled_owner_when_primary_object_ref_is_unscheduled(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs4".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.clone(),
            fs_source: "server:/nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs4".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b.clone(),
            fs_source: "server:/nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(vec![RootSpec::new("nfs4", &root_a)], &grants);
    let sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let owner = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            Some(&sink_status),
            "nfs4",
        ))
        .expect("resolve nfs4 owner")
        .expect("nfs4 owner");

    assert_eq!(
        owner.0, "node-b",
        "scheduled sink owner should win when primary_object_ref names a node that is no longer scheduled for the group"
    );
}

#[test]
fn materialized_owner_node_for_group_tracks_group_primary_by_group() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a");
    let root_b = tmp.path().join("node-b");
    fs::create_dir_all(&root_a).expect("create node-a dir");
    fs::create_dir_all(&root_b).expect("create node-b dir");

    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a.clone(),
            fs_source: "server:/nfs1".to_string(),
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
            mount_point: root_b.clone(),
            fs_source: "server:/nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
        ],
        &grants,
    );

    let nfs1 = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            None,
            "nfs1",
        ))
        .expect("resolve nfs1 owner")
        .expect("nfs1 owner");
    let nfs2 = crate::runtime_app::shared_tokio_runtime()
        .block_on(materialized_owner_node_for_group(
            source.as_ref(),
            None,
            "nfs2",
        ))
        .expect("resolve nfs2 owner")
        .expect("nfs2 owner");

    assert_eq!(nfs1.0, "node-a");
    assert_eq!(nfs2.0, "node-b");
}

#[test]
fn resolve_force_find_groups_uses_local_source_snapshot_and_filters_scan_disabled_roots() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("nfs1");
    let root_b = tmp.path().join("nfs2");
    let root_c = tmp.path().join("nfs3");
    fs::create_dir_all(&root_a).expect("create nfs1 dir");
    fs::create_dir_all(&root_b).expect("create nfs2 dir");
    fs::create_dir_all(&root_c).expect("create nfs3 dir");

    let grants = vec![
        granted_mount_root("node-a::nfs1", &root_a),
        granted_mount_root("node-b::nfs2", &root_b),
        granted_mount_root("node-c::nfs3", &root_c),
    ];
    let mut nfs3 = RootSpec::new("nfs3", &root_c);
    nfs3.scan = false;
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
            nfs3,
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::default(),
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: None,
    };

    let groups = crate::runtime_app::shared_tokio_runtime()
        .block_on(resolve_force_find_groups(&state, &params))
        .expect("resolve groups");

    assert_eq!(groups, vec!["nfs1".to_string(), "nfs2".to_string()]);
}

#[test]
fn resolve_force_find_groups_keeps_scan_enabled_roots_when_primary_snapshot_is_partial() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("nfs1");
    let root_b = tmp.path().join("nfs2");
    let root_c = tmp.path().join("nfs3");
    fs::create_dir_all(&root_a).expect("create nfs1 dir");
    fs::create_dir_all(&root_b).expect("create nfs2 dir");
    fs::create_dir_all(&root_c).expect("create nfs3 dir");

    // Simulate node-local grants that only include one group while all scan-enabled roots are present.
    let grants = vec![granted_mount_root("node-a::nfs1", &root_a)];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", &root_a),
            RootSpec::new("nfs2", &root_b),
            RootSpec::new("nfs3", &root_c),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let state = test_api_state_for_source(source, sink);
    let params = NormalizedApiParams {
        path: b"/".to_vec(),
        group: None,
        recursive: true,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::default(),
        group_page_size: Some(GROUP_PAGE_SIZE_DEFAULT),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: None,
    };

    let groups = crate::runtime_app::shared_tokio_runtime()
        .block_on(resolve_force_find_groups(&state, &params))
        .expect("resolve groups");

    assert_eq!(
        groups,
        vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        "fresh all-groups force-find scope must retain all scan-enabled groups even if source primary snapshot is partial"
    );
}

#[test]
fn query_responses_from_source_events_keeps_prefixed_find_rows() {
    let query_path = b"/qf-e2e-job";
    let events = vec![
        mk_source_record_event(
            "node-a::nfs1",
            b"/tmp/capanix/data/nfs1/qf-e2e-job/file-a.txt",
            b"file-a.txt",
            10,
        ),
        mk_source_record_event(
            "node-a::nfs1",
            b"/tmp/capanix/data/nfs1/qf-e2e-job/file-b.txt",
            b"file-b.txt",
            11,
        ),
    ];

    let grouped = query_responses_by_origin_from_source_events(&events, query_path)
        .expect("build grouped query responses");
    let response = grouped.get("node-a::nfs1").expect("origin response exists");
    let mut paths = response
        .entries
        .iter()
        .map(|node| String::from_utf8_lossy(&node.path).to_string())
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/qf-e2e-job/file-a.txt".to_string(),
            "/qf-e2e-job/file-b.txt".to_string()
        ]
    );
}

#[test]
fn max_total_files_from_stats_events_uses_highest_total_files() {
    let mk_stats_event = |origin: &str, total_files: u64| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            total_files,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let events = vec![mk_stats_event("n1", 3), mk_stats_event("n2", 8)];
    assert_eq!(max_total_files_from_stats_events(&events), Some(8));
}

#[test]
fn latest_file_mtime_from_stats_events_uses_newest_file_mtime() {
    let mk_stats_event = |origin: &str, mtime: Option<u64>| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            latest_file_mtime_us: mtime,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let events = vec![
        mk_stats_event("n1", Some(7)),
        mk_stats_event("n2", Some(11)),
        mk_stats_event("n3", None),
        mk_event("n3", b"bad-msgpack".to_vec()),
    ];
    assert_eq!(latest_file_mtime_from_stats_events(&events), Some(11));
}

#[test]
fn error_response_maps_protocol_violation_to_bad_gateway() {
    let response = error_response(CnxError::ProtocolViolation("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn error_response_maps_timeout_to_gateway_timeout() {
    let response = error_response(CnxError::Timeout);
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
}

#[test]
fn error_response_maps_internal_to_internal_server_error() {
    let response = error_response(CnxError::Internal("x".into()));
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn error_response_maps_transport_closed_to_service_unavailable() {
    let response = error_response(CnxError::TransportClosed("x".into()));
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn error_response_maps_peer_error_to_bad_gateway() {
    let response = error_response(CnxError::PeerError("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn error_response_maps_invalid_input_to_bad_request() {
    let response = error_response(CnxError::InvalidInput("x".into()));
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn error_response_maps_force_find_inflight_conflict_to_too_many_requests() {
    let response = error_response(CnxError::NotReady(format!(
        "{FORCE_FIND_INFLIGHT_CONFLICT_PREFIX} force-find already running for group: nfs1"
    )));
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[test]
fn build_materialized_stats_request_keeps_shape() {
    let params = build_materialized_stats_request(b"/a", true, Some("sink-a".into()));
    assert_eq!(params.op, QueryOp::Stats);
    assert_eq!(params.scope.path, b"/a");
    assert!(params.scope.recursive);
    assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
}

#[test]
fn build_materialized_tree_request_sets_selected_group() {
    let params = build_materialized_tree_request(
        b"/a",
        false,
        Some(2),
        ReadClass::TrustedMaterialized,
        Some("sink-a".to_string()),
    );
    assert_eq!(params.op, QueryOp::Tree);
    assert_eq!(params.scope.path, b"/a");
    assert!(!params.scope.recursive);
    assert_eq!(params.scope.max_depth, Some(2));
    let tree_options = params.tree_options.expect("tree options");
    assert_eq!(tree_options.read_class, ReadClass::TrustedMaterialized);
    assert_eq!(params.scope.selected_group.as_deref(), Some("sink-a"));
}

#[test]
fn normalize_api_params_uses_defaults() {
    let params = ApiParams {
        path: None,
        path_b64: None,
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let normalized = normalize_api_params(params).expect("normalize defaults");
    assert_eq!(normalized.path, b"/".to_vec());
    assert_eq!(normalized.group, None);
    assert!(normalized.recursive);
    assert_eq!(normalized.max_depth, None);
    assert_eq!(normalized.pit_id, None);
    assert_eq!(normalized.group_order, GroupOrder::GroupKey);
    assert_eq!(normalized.group_page_size, None);
    assert_eq!(normalized.group_after, None);
    assert_eq!(normalized.entry_page_size, None);
    assert_eq!(normalized.entry_after, None);
    assert_eq!(normalized.read_class, None);
}

#[test]
fn normalize_api_params_keeps_group_pagination_shape() {
    let params = ApiParams {
        path: Some("/mnt".into()),
        path_b64: None,
        group: None,
        recursive: Some(false),
        max_depth: Some(3),
        pit_id: Some("pit-1".into()),
        group_order: Some(GroupOrder::FileAge),
        group_page_size: Some(25),
        group_after: Some("group-cursor-1".into()),
        entry_page_size: Some(100),
        entry_after: Some("entry-cursor-bundle-1".into()),
        read_class: Some(ReadClass::Materialized),
    };
    let normalized = normalize_api_params(params).expect("normalize explicit params");
    assert_eq!(normalized.path, b"/mnt".to_vec());
    assert_eq!(normalized.group, None);
    assert!(!normalized.recursive);
    assert_eq!(normalized.max_depth, Some(3));
    assert_eq!(normalized.pit_id.as_deref(), Some("pit-1"));
    assert_eq!(normalized.group_order, GroupOrder::FileAge);
    assert_eq!(normalized.group_page_size, Some(25));
    assert_eq!(normalized.group_after.as_deref(), Some("group-cursor-1"));
    assert_eq!(normalized.entry_page_size, Some(100));
    assert_eq!(
        normalized.entry_after.as_deref(),
        Some("entry-cursor-bundle-1")
    );
    assert_eq!(normalized.read_class, Some(ReadClass::Materialized));
}

#[test]
fn normalize_api_params_decodes_path_b64() {
    let params = ApiParams {
        path: None,
        path_b64: Some(B64URL.encode(b"/bad/\xffname")),
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let normalized = normalize_api_params(params).expect("normalize path_b64");
    assert_eq!(normalized.path, b"/bad/\xffname".to_vec());
}

#[test]
fn normalize_api_params_rejects_path_and_path_b64_together() {
    let params = ApiParams {
        path: Some("/mnt".into()),
        path_b64: Some(B64URL.encode(b"/mnt")),
        group: None,
        recursive: None,
        max_depth: None,
        pit_id: None,
        group_order: None,
        group_page_size: None,
        group_after: None,
        entry_page_size: None,
        entry_after: None,
        read_class: None,
    };
    let err = normalize_api_params(params).expect_err("reject mixed path inputs");
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn tree_json_includes_bytes_safe_fields() {
    let root = TreePageRoot {
        path: b"/bad/\xffname".to_vec(),
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        exists: true,
        has_children: false,
    };
    let entry = TreePageEntry {
        path: b"/bad/\xffname".to_vec(),
        depth: 1,
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        has_children: false,
    };

    let root_json = tree_root_json(&root);
    assert_eq!(
        root_json["path_b64"],
        serde_json::json!(B64URL.encode(b"/bad/\xffname"))
    );
    assert_eq!(
        root_json["path"],
        serde_json::json!(path_to_string_lossy(b"/bad/\xffname"))
    );

    let entry_json = tree_entry_json(&entry);
    assert_eq!(
        entry_json["path_b64"],
        serde_json::json!(B64URL.encode(b"/bad/\xffname"))
    );
}

#[test]
fn tree_json_omits_bytes_safe_fields_for_utf8_names() {
    let root = TreePageRoot {
        path: b"/utf8/hello.txt".to_vec(),
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        exists: true,
        has_children: false,
    };
    let entry = TreePageEntry {
        path: b"/utf8/hello.txt".to_vec(),
        depth: 1,
        size: 1,
        modified_time_us: 2,
        is_dir: false,
        has_children: false,
    };

    let root_json = tree_root_json(&root);
    assert!(root_json.get("path_b64").is_none());

    let entry_json = tree_entry_json(&entry);
    assert!(entry_json.get("path_b64").is_none());
}

#[test]
fn decode_stats_groups_keeps_decode_error_group() {
    let ok_payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
        total_files: 1,
        ..SubtreeStats::default()
    }))
    .expect("encode stats");
    let events = vec![
        mk_event("n1", ok_payload),
        mk_event("n2", b"bad-msgpack".to_vec()),
    ];
    let groups = decode_stats_groups(events, &origin_policy(), None, ReadClass::Materialized);
    assert_eq!(groups.len(), 2);
    assert_eq!(groups["n1"]["status"], "ok");
    assert_eq!(groups["n2"]["status"], "error");
}

#[test]
fn decode_stats_groups_materialized_zeroes_blind_spot_count() {
    let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
        total_nodes: 5,
        blind_spot_count: 5,
        ..SubtreeStats::default()
    }))
    .expect("encode stats");

    let materialized = decode_stats_groups(
        vec![mk_event("nfs2", payload.clone())],
        &origin_policy(),
        None,
        ReadClass::Materialized,
    );
    assert_eq!(materialized["nfs2"]["data"]["blind_spot_count"], 0);

    let trusted = decode_stats_groups(
        vec![mk_event("nfs2", payload)],
        &origin_policy(),
        None,
        ReadClass::TrustedMaterialized,
    );
    assert_eq!(trusted["nfs2"]["data"]["blind_spot_count"], 0);
}

#[test]
fn decode_stats_groups_preserves_utf8_group_keys_exactly() {
    let composed = "café-👩🏽‍💻";
    let decomposed = "cafe\u{301}-👩🏽‍💻";
    let mk_stats_event = |origin: &str| -> Event {
        let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
            total_files: 1,
            ..SubtreeStats::default()
        }))
        .expect("encode stats");
        mk_event(origin, payload)
    };

    let groups = decode_stats_groups(
        vec![mk_stats_event(composed), mk_stats_event(decomposed)],
        &origin_policy(),
        None,
        ReadClass::Materialized,
    );

    assert!(groups.contains_key(composed));
    assert!(groups.contains_key(decomposed));
    assert_ne!(composed, decomposed);
    assert_eq!(groups[composed]["status"], "ok");
    assert_eq!(groups[decomposed]["status"], "ok");
}

#[test]
fn materialized_query_readiness_waits_for_initial_audit_completion() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: Vec::new(),
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "obj-a".into(),
            status: "ok".into(),
            coverage_mode: "audit_only".into(),
            watch_enabled: false,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 0,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
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
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot {
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        estimated_heap_bytes: 0,
        scheduled_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "root-a".into(),
            primary_object_ref: "obj-a".into(),
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
        ..SinkStatusSnapshot::default()
    };

    let err = materialized_query_readiness_error(&source_status, &sink_status)
        .expect("initial audit should gate materialized queries");
    assert!(err.contains("initial audit incomplete"));
    assert!(err.contains("root-a"));
}

#[test]
fn cached_sink_status_drops_cached_ready_group_when_fresh_snapshot_explicitly_reports_same_group_empty(
) {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "nfs1".into(),
            status: "ok".into(),
            matched_grants: 1,
            active_members: 1,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs1-key".into(),
            logical_root_id: "nfs1".into(),
            object_ref: "node-a::nfs1".into(),
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
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
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
            current_revision: None,
            current_stream_generation: None,
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
            groups: vec![sink_group_status("nfs1", true)],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        },
    };
    let allowed_groups = BTreeSet::from(["nfs1".to_string()]);
    let fresh = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "nfs1-owner".to_string(),
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
        scheduled_groups_by_node: BTreeMap::new(),
        ..SinkStatusSnapshot::default()
    };

    let merged = merge_with_cached_sink_status_snapshot(Some(&cached), &allowed_groups, fresh);
    let merged_groups = merged
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
    assert_eq!(merged_groups, vec![("nfs1", false, 0, 0, 0)]);
    assert!(
        materialized_query_readiness_error(&source_status, &merged).is_some(),
        "fresh explicit empty same-group sink snapshot must clear cached trusted-materialized readiness instead of preserving stale ready state"
    );
}

#[test]
fn materialized_query_readiness_fail_closed_when_sink_group_missing() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "ok".into(),
            matched_grants: 1,
            active_members: 1,
            coverage_mode: "audit_only".into(),
        }],
        concrete_roots: Vec::new(),
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot::default();

    let err = materialized_query_readiness_error(&source_status, &sink_status)
        .expect("missing sink group must gate materialized queries");
    assert!(err.contains("initial audit incomplete"));
    assert!(err.contains("root-a"));
}

#[test]
fn materialized_query_readiness_ignores_inactive_or_non_scan_groups() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: Vec::new(),
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "watch_only".into(),
                watch_enabled: true,
                scan_enabled: false,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 128,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: false,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot::default();
    assert!(materialized_query_readiness_error(&source_status, &sink_status).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_retries_empty_first_ranked_group_for_non_root_subtree_once(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let payload = if group_id == "nfs1" && nth == 0 {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root subtree retry should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted-ready non-root subtree should retry a transient empty first-ranked owner result instead of settling an empty root: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "first-ranked non-root trusted subtree should retry the owner once before accepting later groups"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_retries_empty_first_ranked_group_for_non_root_subtree_with_max_depth_once(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-max-depth-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-max-depth-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else if group_id == "nfs2" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    } else {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-max-depth-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let payload = if group_id == "nfs1" && nth == 0 {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    } else if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    };
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root max-depth subtree retry should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted-ready non-root max-depth subtree should retry a transient empty first-ranked owner result instead of settling an empty root: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "first-ranked max-depth trusted subtree should retry the owner once before accepting later groups"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_first_ranked_non_root_subtree_retry_times_out(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-subtree-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-subtree-proxy-fallback-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-subtree-owner-a-timeout-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = if group_id == "nfs1" {
                        owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    if group_id == "nfs1" && nth == 0 {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            empty_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                        continue;
                    }
                    if group_id == "nfs1" && nth == 1 {
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        continue;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-subtree-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/data".to_vec(),
        group: None,
        recursive: false,
        max_depth: None,
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
        Duration::from_secs(6),
        build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(3400),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted non-root subtree timeout fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted-ready non-root subtree should fall back to generic proxy when the owner retry times out after an initial empty result: {:?}",
        session
            .groups
            .iter()
            .map(|group| {
                (
                    group.group.clone(),
                    group
                        .root
                        .as_ref()
                        .map(|root| (root.exists, root.path.clone())),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "first-ranked non-root trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_first_ranked_non_root_max_depth_proxy_payload_after_owner_retry_timeout(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-proxy-rich-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-proxy-rich-then-empty-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let Some(group_id) = params.scope.selected_group.clone() else {
                    continue;
                };
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("proxy request correlation");
                if group_id == "nfs1" {
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: 1,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )),
                    ));
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: 2,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(empty_materialized_tree_payload_for_test(&params.scope.path)),
                    ));
                } else if group_id == "nfs2" {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        ),
                    ));
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-owner-a-timeout-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a request");
                    let nth = if group_id == "nfs1" {
                        owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    } else {
                        usize::MAX
                    };
                    if group_id == "nfs1" && nth == 0 {
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            empty_materialized_tree_payload_for_test(&params.scope.path),
                        ));
                        continue;
                    }
                    if group_id == "nfs1" && nth == 1 {
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        continue;
                    }
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-a request correlation"),
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        ),
                    ));
                }
                responses
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-first-ranked-non-root-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted first-ranked non-root max-depth proxy rich fallback should settle within the PIT budget")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted first-ranked non-root max-depth proxy fallback must preserve a richer same-batch payload instead of letting a later empty root overwrite it after owner retry timeout: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "first-ranked max-depth trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_middle_ranked_non_root_max_depth_owner_remains_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    let root_c = tmp.path().join("node-b-nfs3");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    fs::create_dir_all(&root_c).expect("create node-b nfs3 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.2".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-b::nfs3".to_string(),
            host_ref: "node-b".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_c,
            fs_source: "nfs3".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
            RootSpec::new("nfs3", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-ready-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let sink_status_payload = sink_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            sink_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        )
                    } else if group_id == "nfs2" {
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        )
                    } else {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
                    };
                    Some(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        payload,
                    ))
                })
                .collect::<Vec<_>>()
        },
    );
    let owner_a_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_calls_for_handler = owner_a_calls.clone();
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-owner-a-empty-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_calls = owner_a_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let params =
                            rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                                .expect("decode owner-a query request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for owner-a request");
                        let payload = if group_id == "nfs1" {
                            real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[b"/nested/child"],
                            )
                        } else if group_id == "nfs2" {
                            owner_a_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            empty_materialized_tree_payload_for_test(&params.scope.path)
                        } else {
                            real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[b"/nested/peer.txt"],
                            )
                        };
                        mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("owner-a request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-max-depth-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner-b request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested".to_vec(),
        group: None,
        recursive: true,
        max_depth: Some(1),
        pit_id: None,
        group_order: GroupOrder::GroupKey,
        group_page_size: Some(3),
        group_after: None,
        entry_page_size: Some(ENTRY_PAGE_SIZE_DEFAULT),
        entry_after: None,
        read_class: Some(ReadClass::TrustedMaterialized),
    };
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs1", true),
            sink_group_status("nfs2", true),
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };

    let session = tokio::time::timeout(
            Duration::from_secs(6),
            build_tree_pit_session(
                &state,
                &ProjectionPolicy::default(),
                &params,
                Duration::from_millis(3400),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted middle-ranked non-root max-depth proxy fallback should settle within the PIT budget")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group
                    .root
                    .as_ref()
                    .map(|root| (root.exists, root.path.clone(), group.entries.len())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec(), 1))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec(), 0))),
        ],
        "trusted middle-ranked non-root max-depth proxy fallback must preserve the richer nfs2 subtree instead of settling an ok empty root after the owner stays empty: {group_roots:?}"
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "middle-ranked non-root max-depth trusted subtree should try the owner twice before falling back to proxy"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[test]
fn materialized_query_readiness_ignores_retired_logical_root_without_active_scan_primary() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs2".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "nfs4".into(),
                status: "ok".into(),
                matched_grants: 3,
                active_members: 3,
                coverage_mode: "realtime_hotset_plus_audit".into(),
            },
        ],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2-key".into(),
            logical_root_id: "nfs2".into(),
            object_ref: "node-a::nfs2".into(),
            status: "running".into(),
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
            emitted_batch_count: 1,
            emitted_event_count: 1,
            emitted_control_event_count: 0,
            emitted_data_event_count: 1,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(30),
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 1,
            forwarded_event_count: 1,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(30),
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
    let sink_status = SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs2", true)],
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "a stale logical root without any active scan-enabled primary concrete root must not keep trusted-materialized readiness blocked after contraction to nfs2"
    );
}

#[test]
fn filter_source_status_snapshot_drops_groups_outside_current_roots() {
    let snapshot = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-a".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-b".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
        ],
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: vec![
            ("root-a".into(), "degraded".into()),
            ("root-b".into(), "degraded".into()),
        ],
    };
    let filtered = filter_source_status_snapshot(snapshot, &BTreeSet::from(["root-b".to_string()]));
    assert_eq!(filtered.logical_roots.len(), 1);
    assert_eq!(filtered.logical_roots[0].root_id, "root-b");
    assert_eq!(filtered.concrete_roots.len(), 1);
    assert_eq!(filtered.concrete_roots[0].logical_root_id, "root-b");
    assert_eq!(
        filtered.degraded_roots,
        vec![("root-b".to_string(), "degraded".to_string())]
    );
}

#[test]
fn merge_source_status_snapshots_prefers_later_ready_truth_for_same_group() {
    let stale = SourceStatusSnapshot {
        current_stream_generation: Some(1),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "output_closed".into(),
            matched_grants: 1,
            active_members: 0,
            coverage_mode: "audit_only".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "node-a::root-a".into(),
            status: "output_closed".into(),
            coverage_mode: "audit_only".into(),
            watch_enabled: false,
            scan_enabled: true,
            is_group_primary: true,
            active: false,
            watch_lru_capacity: 0,
            audit_interval_ms: 10_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: true,
            last_rescan_reason: Some("manual".into()),
            last_error: Some("bridge stopped".into()),
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
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
    let ready = SourceStatusSnapshot {
        current_stream_generation: Some(2),
        logical_roots: vec![crate::source::SourceLogicalRootHealthSnapshot {
            root_id: "root-a".into(),
            status: "ok".into(),
            matched_grants: 2,
            active_members: 2,
            coverage_mode: "realtime_hotset_plus_audit".into(),
        }],
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "root-a-key".into(),
            logical_root_id: "root-a".into(),
            object_ref: "node-a::root-a".into(),
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
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(1),
            emitted_batch_count: 6,
            emitted_event_count: 18,
            emitted_control_event_count: 6,
            emitted_data_event_count: 12,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(30),
            last_emitted_origins: vec!["node-a".into()],
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: Some(2),
            current_stream_generation: Some(2),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };

    let merged = merge_source_status_snapshots(vec![stale, ready]);

    assert_eq!(merged.current_stream_generation, Some(2));
    assert_eq!(merged.logical_roots.len(), 1);
    assert_eq!(merged.logical_roots[0].root_id, "root-a");
    assert_eq!(merged.logical_roots[0].status, "ok");
    assert_eq!(merged.logical_roots[0].matched_grants, 2);
    assert_eq!(merged.logical_roots[0].active_members, 2);
    assert_eq!(merged.concrete_roots.len(), 1);
    assert_eq!(merged.concrete_roots[0].root_key, "root-a-key");
    assert_eq!(merged.concrete_roots[0].status, "ok");
    assert!(merged.concrete_roots[0].active);
    assert!(!merged.concrete_roots[0].rescan_pending);
    assert_eq!(merged.concrete_roots[0].current_stream_generation, Some(2));
}

#[test]
fn materialized_query_readiness_ignores_stale_source_groups_outside_current_roots() {
    let source_status = SourceStatusSnapshot {
        current_stream_generation: None,
        logical_roots: vec![
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-a".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
            crate::source::SourceLogicalRootHealthSnapshot {
                root_id: "root-b".into(),
                status: "ok".into(),
                matched_grants: 1,
                active_members: 1,
                coverage_mode: "audit_only".into(),
            },
        ],
        concrete_roots: vec![
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-a-key".into(),
                logical_root_id: "root-a".into(),
                object_ref: "obj-a".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
            crate::source::SourceConcreteRootHealthSnapshot {
                root_key: "root-b-key".into(),
                logical_root_id: "root-b".into(),
                object_ref: "obj-b".into(),
                status: "ok".into(),
                coverage_mode: "audit_only".into(),
                watch_enabled: false,
                scan_enabled: true,
                is_group_primary: true,
                active: true,
                watch_lru_capacity: 0,
                audit_interval_ms: 10_000,
                overflow_count: 0,
                overflow_pending: false,
                rescan_pending: false,
                last_rescan_reason: None,
                last_error: None,
                last_audit_started_at_us: None,
                last_audit_completed_at_us: None,
                last_audit_duration_ms: None,
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
                current_revision: None,
                current_stream_generation: None,
                candidate_revision: None,
                candidate_stream_generation: None,
                candidate_status: None,
                draining_revision: None,
                draining_stream_generation: None,
                draining_status: None,
            },
        ],
        degraded_roots: Vec::new(),
    };
    let sink_status = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "root-b".into(),
            primary_object_ref: "obj-b".into(),
            total_nodes: 4,
            live_nodes: 4,
            tombstoned_count: 0,
            attested_count: 4,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 1,
            estimated_heap_bytes: 1,
        }],
        ..SinkStatusSnapshot::default()
    };
    let filtered_source =
        filter_source_status_snapshot(source_status, &BTreeSet::from(["root-b".to_string()]));
    assert!(materialized_query_readiness_error(&filtered_source, &sink_status).is_none());
}

#[tokio::test]
async fn rpc_metrics_endpoint_returns_ok() {
    let resp = get_bound_route_metrics().await.into_response();
    assert_eq!(resp.status(), StatusCode::OK);
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_count_top_bucket_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-count&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-count");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-b");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/winner-b"))
        })));
    assert_eq!(json["groups"][0]["stability"]["state"], "not-evaluated");
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_age_top_bucket_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-age");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-a");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/winner-a"))
        })));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_age_keeps_empty_groups_eligible_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::FileAgeNoFiles);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-age");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-a");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/empty-a"))
        })));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.NO_CROSS_GROUP_ENTRY_MERGE", mode="system")
#[tokio::test]
async fn force_find_defaults_to_group_key_multi_group_response_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "group-key");
    assert!(json["groups"]
        .as_array()
        .is_some_and(|groups| !groups.is_empty()));
    assert!(
        json["group_page"]["returned_groups"]
            .as_u64()
            .unwrap_or_default()
            >= 1
    );
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_explicit_group_returns_only_that_group_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group=sink-b")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    let groups = json["groups"].as_array().expect("groups array");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group"], "sink-b");
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
#[tokio::test]
async fn force_find_defaults_when_query_params_omitted_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "group-key");
    assert!(json["groups"]
        .as_array()
        .is_some_and(|groups| !groups.is_empty()));
    assert_eq!(
        json["group_page"]["returned_groups"],
        json["groups"]
            .as_array()
            .map(|groups| groups.len())
            .unwrap_or_default()
    );
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY", mode="system")
#[tokio::test]
async fn projection_rpc_metrics_endpoint_shape_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/bound-route-metrics")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert!(payload.get("call_timeout_total").is_some());
    assert!(payload.get("correlation_mismatch_total").is_some());
    assert!(payload.get("uncorrelated_reply_total").is_some());
    assert!(payload.get("recv_loop_iterations").is_some());
    assert!(payload.get("pending_calls").is_some());
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL", mode="system")
// @verify_spec("CONTRACTS.API_BOUNDARY.QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE", mode="system")
#[tokio::test]
async fn force_find_rejects_status_only_and_keeps_pagination_axis_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=group-key&read_class=trusted-materialized")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json error payload");
    let msg = payload
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(msg.contains("read_class must be fresh on /on-demand-force-find"));
    assert_eq!(
        payload.get("code").and_then(|v| v.as_str()),
        Some("INVALID_INPUT")
    );
    assert_eq!(payload.get("path"), Some(&serde_json::json!("/")));
}

// @verify_spec("CONTRACTS.API_BOUNDARY.API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH", mode="system")
#[tokio::test]
async fn namespace_projection_endpoints_removed_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);

    let req_tree = Request::builder()
        .uri("/namespace-tree?path=/peers/sink-b/mnt/nfs1&best=false")
        .method("GET")
        .body(Body::empty())
        .expect("build request");
    let resp_tree = fixture
        .app
        .clone()
        .oneshot(req_tree)
        .await
        .expect("serve namespace tree request");
    assert_eq!(resp_tree.status(), StatusCode::NOT_FOUND);

    let req_force_find = Request::builder()
        .uri("/namespace-on-demand-force-find?path=/peers/sink-b/mnt/nfs1&group_order=group-key")
        .method("GET")
        .body(Body::empty())
        .expect("build request");
    let resp_force_find = fixture
        .app
        .oneshot(req_force_find)
        .await
        .expect("serve namespace force-find request");
    assert_eq!(resp_force_find.status(), StatusCode::NOT_FOUND);
}

include!("pit_public/trusted_materialized_tree_root_parity.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_public_tree_requests_both_settle_under_multi_group_selected_route_stall() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    let root_b = tmp.path().join("node-a-nfs2");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    fs::create_dir_all(&root_b).expect("create node-a nfs2 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_a,
            fs_source: "nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: root_b,
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs1", "/unused"),
            RootSpec::new("nfs2", "/unused"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let owner_route = sink_query_request_route_for("node-a");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
    .expect("encode multi-group sink-status payload");
    let boundary = Arc::new(
        MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload(
            owner_route.0.clone(),
            sink_status_route.0,
            sink_status_payload,
        ),
    );
    let app = create_local_router(
        sink,
        source,
        Some(boundary.clone()),
        NodeId("node-d".to_string()),
        Arc::new(RwLock::new(ProjectionPolicy {
            query_timeout_ms: Some(1200),
            ..ProjectionPolicy::default()
        })),
        Arc::new(Mutex::new(BTreeSet::new())),
    );

    let make_req = || {
        Request::builder()
            .uri("/tree?path=/&recursive=true&group_page_size=2")
            .method("GET")
            .body(Body::empty())
            .expect("build tree request")
    };

    let mut first = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(make_req()).await }
    });
    match tokio::time::timeout(
        Duration::from_secs(8),
        boundary.wait_for_owner_request_batches(1),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            first.abort();
            panic!("first tree request should reach the selected-group owner route");
        }
    }
    let mut second = tokio::spawn(async move { app.oneshot(make_req()).await });

    let first_response = match tokio::time::timeout(Duration::from_secs(8), &mut first).await {
        Ok(result) => result
            .expect("join first tree request")
            .expect("serve first tree request"),
        Err(_) => {
            first.abort();
            second.abort();
            panic!("first tree request should settle");
        }
    };
    let second_response = match tokio::time::timeout(Duration::from_secs(8), &mut second).await {
        Ok(result) => result
            .expect("join second tree request")
            .expect("serve second tree request"),
        Err(_) => {
            first.abort();
            second.abort();
            panic!("second tree request should settle");
        }
    };

    assert!(first_response.status().is_success() || first_response.status().is_server_error());
    assert!(second_response.status().is_success() || second_response.status().is_server_error());
}
