fn root_spec_with_fs_source(id: &str) -> RootSpec {
    let mut root = RootSpec::new(id, "/unused");
    root.selector = crate::source::config::RootSelector {
        fs_source: Some(id.to_string()),
        ..crate::source::config::RootSelector::default()
    };
    root.subpath_scope = std::path::PathBuf::from("/");
    root
}

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
            root_spec_with_fs_source("nfs1"),
            root_spec_with_fs_source("nfs2"),
            root_spec_with_fs_source("nfs3"),
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
async fn build_tree_pit_session_preserves_materialized_target_groups_when_first_sink_status_snapshot_is_empty(
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
            root_spec_with_fs_source("nfs1"),
            root_spec_with_fs_source("nfs2"),
            root_spec_with_fs_source("nfs3"),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let routed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let routed_groups_for_handler = routed_groups.clone();
    let empty_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot::default())
        .expect("encode empty sink status snapshot");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-empty-first-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let empty_status_payload = empty_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            empty_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-empty-first-owner-sink-query-endpoint",
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
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-a request correlation"),
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
    .expect("tree pit session after empty first sink status");

    assert_eq!(
        session
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2", "nfs3"],
        "an empty first sink-status snapshot on a routed node must not clear all materialized target groups before owner routing can use source primary truth"
    );
    let routed_groups = routed_groups.lock().expect("routed groups lock").clone();
    assert_eq!(
        routed_groups.first().map(String::as_str),
        Some("nfs1"),
        "empty first sink-status snapshots must still let build_tree_pit_session begin rerouting via source primary truth instead of collapsing directly into generic proxy fallback"
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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
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
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_after_first_empty_owner_reply_for_first_ranked_ready_group(
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
        "test-first-ranked-ready-sink-status-endpoint",
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
        "test-first-ranked-empty-owner-sink-query-endpoint",
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
                    owner_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-first-ranked-proxy-sink-query-endpoint",
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
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary.clone(), NodeId("node-d".to_string()));
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
        "trusted-materialized first-ranked ready root should fall back to generic proxy when the owner returns an empty root payload and the proxy can decode the same path",
    );

    assert_eq!(session.groups.len(), 1);
    assert!(
        session.groups[0]
            .root
            .as_ref()
            .is_some_and(|root| root.exists),
        "trusted-materialized first-ranked ready root should settle on the proxy payload instead of failing closed after the first empty owner reply",
    );
    assert_eq!(
        owner_call_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "trusted-materialized first-ranked ready root should fall back to proxy after the first empty owner reply when the proxy can decode the same path"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "trusted-materialized first-ranked ready root should issue one generic proxy fallback after the owner remains empty"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_prefers_richer_proxy_payload_when_first_ranked_ready_root_proxy_batch_contains_later_empty_tree(
) {
    fn mk_event_with_correlation_and_timestamp(
        origin: &str,
        correlation: u64,
        timestamp_us: u64,
        payload: Vec<u8>,
    ) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us,
                logical_ts: None,
                correlation_id: Some(correlation),
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

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
        "test-first-ranked-ready-mixed-proxy-sink-status-endpoint",
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
        "test-first-ranked-always-empty-owner-sink-query-endpoint",
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
                    owner_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-first-ranked-mixed-proxy-sink-query-endpoint",
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
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("proxy request correlation");
                responses.push(mk_event_with_correlation_and_timestamp(
                    &group_id,
                    correlation,
                    1,
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
                responses.push(mk_event_with_correlation_and_timestamp(
                    &group_id,
                    correlation,
                    2,
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary.clone(), NodeId("node-d".to_string()));
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
        "trusted-materialized first-ranked ready root should settle on the richer same-path proxy payload instead of fail-closing on a later empty tree from the same proxy batch",
    );

    assert_eq!(session.groups.len(), 1);
    assert!(
        session.groups[0]
            .root
            .as_ref()
            .is_some_and(|root| root.exists),
        "trusted-materialized first-ranked ready root should rescue the richer same-path proxy payload even when a later empty tree event is also present",
    );
    assert_eq!(
        owner_call_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "first-ranked ready root should not reissue owner retry once the proxy batch already contains a richer same-path tree",
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "first-ranked ready root should only issue one proxy fallback for the mixed proxy batch",
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
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_first_ranked_non_root_subtree_owner_remains_empty(
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
        "test-trusted-first-ranked-non-root-ready-sink-status-endpoint",
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
        "test-trusted-first-ranked-non-root-proxy-endpoint",
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
    let owner_a_nfs1_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_a_nfs1_calls_for_handler = owner_a_nfs1_calls.clone();
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-first-ranked-non-root-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_a_nfs1_calls = owner_a_nfs1_calls_for_handler.clone();
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
                        if group_id == "nfs1" {
                            owner_a_nfs1_calls
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        let payload = if group_id == "nfs1" {
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
        "test-trusted-first-ranked-non-root-owner-b-endpoint",
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
    .expect("trusted first-ranked non-root proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted first-ranked non-root subtree should fall back to generic proxy when the first-ranked owner keeps returning an empty subtree despite ready sink status: {:?}",
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
        owner_a_nfs1_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "first-ranked non-root trusted subtree should try the owner twice before falling back to proxy",
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

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
async fn build_tree_pit_session_trusted_materialized_restores_middle_ranked_non_root_max_depth_payload_after_empty_owner_reply(
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
        "test-trusted-middle-ranked-max-depth-ready-sink-status-endpoint",
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
        "test-trusted-middle-ranked-max-depth-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = match group_id.as_str() {
                        "nfs1" => real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/child"],
                        ),
                        "nfs2" => real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/peer.txt"],
                        ),
                        "nfs3" => real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/nested/leaf.txt"],
                        ),
                        _ => unreachable!("unexpected group"),
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
        "test-trusted-middle-ranked-max-depth-owner-a-endpoint",
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
                        let payload = match group_id.as_str() {
                            "nfs1" => real_materialized_tree_payload_with_entries_for_test(
                                &params.scope.path,
                                &[b"/nested/child"],
                            ),
                            "nfs2" => empty_materialized_tree_payload_for_test(&params.scope.path),
                            _ => unreachable!("unexpected node-a group"),
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
        "test-trusted-middle-ranked-max-depth-owner-b-endpoint",
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
                            &[b"/nested/leaf.txt"],
                        ),
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
    let groups = session
        .groups
        .iter()
        .map(|group| (group.group.as_str(), group))
        .collect::<std::collections::BTreeMap<_, _>>();
    let nfs1 = groups.get("nfs1").expect("nfs1 group");
    let nfs2 = groups.get("nfs2").expect("nfs2 group");
    let nfs3 = groups.get("nfs3").expect("nfs3 group");
    let group_contains_path = |group: &&GroupPitSnapshot, path: &[u8]| {
        group.entries.iter().any(|entry| entry.path.as_slice() == path)
    };

    assert!(
        nfs1.root.as_ref().is_some_and(|root| root.exists && root.path == b"/nested"),
        "nfs1 nested root must stay materialized: {:?}",
        nfs1.root
    );
    assert!(
        group_contains_path(nfs1, b"/nested/child"),
        "nfs1 nested max-depth payload must preserve child entry: {:?}",
        nfs1.entries
    );
    assert!(
        nfs2.root.as_ref().is_some_and(|root| root.exists && root.path == b"/nested"),
        "trusted middle-ranked non-root max-depth fallback must restore nfs2 nested root after owner-empty reply: {:?}",
        nfs2.root
    );
    assert!(
        group_contains_path(nfs2, b"/nested/peer.txt"),
        "trusted middle-ranked non-root max-depth fallback must restore nfs2 peer.txt after owner-empty reply: {:?}",
        nfs2.entries
    );
    assert!(
        nfs3.root.as_ref().is_some_and(|root| root.exists && root.path == b"/nested"),
        "nfs3 nested root must stay materialized: {:?}",
        nfs3.root
    );
    assert!(
        group_contains_path(nfs3, b"/nested/leaf.txt"),
        "nfs3 nested max-depth payload must preserve leaf entry: {:?}",
        nfs3.entries
    );
    assert_eq!(
        owner_a_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "middle-ranked non-root max-depth trusted subtree should try the owner twice before falling back to proxy",
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_middle_ranked_non_root_subtree_owner_is_empty_after_selected_group_sink_status_regresses_zeroish(
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
    let mut nfs2_zeroish_status = sink_group_status("nfs2", false);
    nfs2_zeroish_status.live_nodes = 0;
    nfs2_zeroish_status.total_nodes = 0;
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
            nfs2_zeroish_status,
            sink_group_status("nfs3", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-middle-ranked-non-root-zeroish-ready-sink-status-endpoint",
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
        "test-trusted-middle-ranked-non-root-zeroish-proxy-endpoint",
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
        "test-trusted-middle-ranked-non-root-zeroish-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
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
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-non-root-zeroish-owner-b-endpoint",
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
    .expect("trusted middle-ranked non-root zeroish-status path should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 3);
    assert!(
        session.groups.iter().all(|group| {
            group
                .root
                .as_ref()
                .is_some_and(|root| root.exists && root.path == b"/data")
        }),
        "trusted middle-ranked non-root subtree should not let a zeroish selected-group sink status settle an ok empty root when the owner is empty but the proxy can still decode the real subtree: {:?}",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_first_and_middle_ranked_groups_return_empty_roots_but_last_group_decodes(
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
        "test-first-middle-empty-root-ready-sink-status-endpoint",
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
        "test-first-middle-empty-root-owner-a-endpoint",
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
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("owner-a request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_b.clone(),
        "test-first-middle-empty-root-owner-b-endpoint",
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
        "test-first-middle-empty-root-proxy-endpoint",
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
                let payload = if group_id == "nfs3" {
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
    .expect("trusted-materialized multi-empty-root failure should finish within the shared PIT timeout budget")
    .expect_err(
        "trusted-materialized PIT must fail closed when first and middle ranked ready groups return empty roots even if the last ranked group still decodes",
    );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted-materialized first+middle ranked ready groups empty roots should fail closed instead of rendering an ok body: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}
