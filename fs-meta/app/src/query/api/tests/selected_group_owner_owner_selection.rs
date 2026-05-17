#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_uses_owner_route_when_owner_is_caller_node() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
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
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let node_a_route = sink_query_request_route_for("node-a");
    let observed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_groups_for_handler = observed_groups.clone();
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-same-node-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let observed_groups = observed_groups_for_handler.clone();
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
                    observed_groups
                        .lock()
                        .expect("observed groups lock")
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

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-a".to_string()),
    );
    let policy = ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::MountPoint,
        mount_point_by_object_ref: HashMap::from([(
            "node-a::nfs1".to_string(),
            "nfs1".to_string(),
        )]),
        ..ProjectionPolicy::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &policy,
        build_materialized_tree_request(
            b"/layout-a",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs1".to_string()),
        ),
        Duration::from_secs(1),
        None,
        false,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "route backend must query the owner-scoped sink route even when owner node equals caller node; owner_route_calls={} err={:?}",
        boundary.send_batch_count(&node_a_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("same-node owner route result"),
        &policy,
        "nfs1",
        b"/layout-a",
    )
    .expect("decode same-node owner route response");
    assert!(payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&node_a_route.0),
        1,
        "same-node owner must still use the owner-scoped route because the route backend may use an external sink worker on the same host"
    );
    assert_eq!(
        observed_groups.lock().expect("observed groups lock").as_slice(),
        &["nfs1".to_string()]
    );

    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[test]
fn decode_selected_group_response_prefers_live_payload_over_newer_empty_peer_payload() {
    let events = vec![
        mk_event_with_correlation_and_timestamp(
            "nfs1",
            42,
            10,
            real_materialized_tree_payload_for_test(b"/"),
        ),
        mk_event_with_correlation_and_timestamp(
            "nfs1",
            42,
            20,
            empty_materialized_tree_payload_for_test(b"/"),
        ),
    ];

    let payload = decode_materialized_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs1",
        b"/",
    )
    .expect("decode selected-group materialized response with mixed live and empty replies");

    assert!(
        payload.root.exists || payload.root.has_children || !payload.entries.is_empty(),
        "selected-group materialized decode must not let a newer empty fanout reply mask an older live same-group reply"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_fans_out_candidate_owners_when_sink_status_is_fully_empty(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(node_a_root.join("layout")).expect("create node-a dir");
    fs::create_dir_all(node_c_root.join("layout")).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
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
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs2".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let node_a_route = sink_query_request_route_for("node-a");
    let node_c_route = sink_query_request_route_for("node-c");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-empty-status-node-a-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-a candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-a candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-a request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut node_c_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_c_route.clone(),
        "test-empty-status-node-c-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-c candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-c candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-c request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-empty-status-generic-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("api-node".to_string()),
    );
    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs2".to_string()),
        ),
        Duration::from_secs(2),
        Some(SinkStatusSnapshot::default()),
        false,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fan out to candidate owner routes when first sink status is fully empty; node_a_calls={} node_c_calls={} proxy_calls={} err={:?}",
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_c_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let events = result.expect("empty-status candidate owner result");
    let payload = decode_materialized_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs2",
        b"/",
    )
    .expect("decode empty-status candidate owner response");
    assert!(
        payload.root.exists,
        "selected-group query must not settle empty while a candidate owner has materialized data"
    );
    assert_eq!(
        boundary.send_batch_count(&node_c_route.0),
        1,
        "candidate owner route with live materialized data should be queried"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        0,
        "fully empty sink status is a collection gap; generic proxy must not be the first selected-group answer"
    );

    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_c_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_fans_out_candidate_owners_when_sink_status_has_pending_zero_node_group(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(node_a_root.join("layout")).expect("create node-a dir");
    fs::create_dir_all(node_c_root.join("layout")).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
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
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs2".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let node_a_route = sink_query_request_route_for("node-a");
    let node_c_route = sink_query_request_route_for("node-c");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-pending-status-node-a-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-a candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-a candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-a request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut node_c_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_c_route.clone(),
        "test-pending-status-node-c-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-c candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-c candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-c request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-pending-status-generic-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("api-node".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        primary_host_ref_by_group: BTreeMap::from([("nfs2".to_string(), "node-a".to_string())]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
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
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 0,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs2".to_string()),
        ),
        Duration::from_secs(2),
        Some(selected_group_sink_status),
        false,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fan out to candidate owner routes when sink status only has a pending zero-node placeholder; node_a_calls={} node_c_calls={} proxy_calls={} err={:?}",
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_c_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let events = result.expect("pending-status candidate owner result");
    let payload = decode_materialized_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs2",
        b"/",
    )
    .expect("decode pending-status candidate owner response");
    assert!(
        payload.root.exists,
        "pending zero-node status is not materialized owner proof and must not settle empty"
    );
    assert_eq!(
        boundary.send_batch_count(&node_c_route.0),
        1,
        "candidate owner route with live materialized data should be queried"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        0,
        "pending zero-node status is a collection gap; generic proxy must not be the first selected-group answer"
    );

    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_c_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_uses_request_source_status_as_candidate_owner_evidence_when_sink_status_is_pending_gap(
) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout")).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs2".to_string(),
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
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let node_a_route = sink_query_request_route_for("node-a");
    let node_c_route = sink_query_request_route_for("node-c");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-source-status-gap-node-a-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-a candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-a candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-a request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut node_c_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_c_route.clone(),
        "test-source-status-gap-node-c-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-c candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-c candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-c request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-source-status-gap-generic-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("api-node".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        primary_host_ref_by_group: BTreeMap::from([("nfs2".to_string(), "node-a".to_string())]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
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
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 0,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let selected_group_source_status = SourceStatusSnapshot {
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2@node-c::nfs2@/mnt/nfs2".to_string(),
            logical_root_id: "nfs2".to_string(),
            object_ref: "node-c::nfs2".to_string(),
            status: "running".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 1024,
            audit_interval_ms: 300_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: true,
            last_rescan_requested_at_us: Some(1),
            last_rescan_reason: Some("manual".to_string()),
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 1,
            emitted_event_count: 1,
            emitted_control_event_count: 0,
            emitted_data_event_count: 1,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(1),
            last_emitted_origins: vec!["node-c::nfs2=1".to_string()],
            forwarded_batch_count: 1,
            forwarded_event_count: 1,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(1),
            last_forwarded_origins: vec!["node-c::nfs2=1".to_string()],
            current_revision: Some(1),
            current_stream_generation: Some(1),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        ..SourceStatusSnapshot::default()
    };
    let timeout = Duration::from_secs(2);
    let group_plan = TreePitSessionPlan::new(timeout, 1).selected_group_owner_snapshot_plan(
        TreePitOwnerSnapshotPlanInput {
            trusted_materialized_tree_query: false,
            reserve_proxy_budget: false,
            allow_empty_owner_retry: false,
            empty_root_requires_fail_closed: false,
        },
    );
    let result =
        query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
            &state,
            &ProjectionPolicy::default(),
            build_materialized_tree_request(
                b"/",
                true,
                None,
                ReadClass::Materialized,
                Some("nfs2".to_string()),
            ),
            timeout,
            Some(&selected_group_source_status),
            Some(selected_group_sink_status),
            None,
            group_plan,
        )
        .await;

    assert!(
        result.is_ok(),
        "request source status should expand candidate owner routes when sink status is only a pending placeholder; node_a_calls={} node_c_calls={} proxy_calls={} err={:?}",
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_c_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let events = result.expect("source-status candidate owner result");
    let payload = decode_materialized_selected_group_response(
        &events,
        &ProjectionPolicy::default(),
        "nfs2",
        b"/",
    )
    .expect("decode source-status candidate owner response");
    assert!(
        payload.root.exists,
        "selected-group query must not settle empty while request source status points to a same-group candidate owner with live materialized data"
    );
    assert_eq!(
        boundary.send_batch_count(&node_c_route.0),
        1,
        "source-status candidate owner route with live materialized data should be queried"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        0,
        "request-scoped source evidence should be tried before generic proxy fallback"
    );

    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_c_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_pending_gap_caps_slow_candidate_and_preserves_proxy_budget() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(node_a_root.join("layout")).expect("create node-a dir");
    fs::create_dir_all(node_c_root.join("layout")).expect("create node-c dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
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
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs2".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root,
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let node_a_route = sink_query_request_route_for("node-a");
    let node_c_route = sink_query_request_route_for("node-c");
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");

    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-pending-gap-slow-node-a-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode slow node-a candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for slow node-a candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("slow node-a request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut node_c_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_c_route.clone(),
        "test-pending-gap-node-c-empty-candidate-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode node-c candidate query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for node-c candidate request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("node-c request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-pending-gap-generic-proxy-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(
                        req.payload_bytes(),
                    )
                    .expect("decode proxy query request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for proxy request");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("proxy request correlation"),
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("api-node".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        primary_host_ref_by_group: BTreeMap::from([("nfs2".to_string(), "node-a".to_string())]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
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
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 0,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let selected_group_source_status = SourceStatusSnapshot {
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2@node-c::nfs2@/mnt/nfs2".to_string(),
            logical_root_id: "nfs2".to_string(),
            object_ref: "node-c::nfs2".to_string(),
            status: "running".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 1024,
            audit_interval_ms: 300_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: true,
            last_rescan_requested_at_us: Some(1),
            last_rescan_reason: Some("manual".to_string()),
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 1,
            emitted_event_count: 1,
            emitted_control_event_count: 0,
            emitted_data_event_count: 1,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(1),
            last_emitted_origins: vec!["node-c::nfs2=1".to_string()],
            forwarded_batch_count: 1,
            forwarded_event_count: 1,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(1),
            last_forwarded_origins: vec!["node-c::nfs2=1".to_string()],
            current_revision: Some(1),
            current_stream_generation: Some(1),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        ..SourceStatusSnapshot::default()
    };
    let timeout = Duration::from_secs(5);
    let group_plan = TreePitSessionPlan::new(timeout, 2).selected_group_stage_plan(
        TreePitGroupPlanInput {
            read_class: ReadClass::TrustedMaterialized,
            observation_state: ObservationState::TrustedMaterialized,
            selected_group_sink_reports_live_materialized: false,
            prior_materialized_group_decoded: false,
            prior_materialized_exact_file_decoded: false,
            rank_index: 0,
            is_last_ranked_group: false,
            selected_group_sink_unready_empty: true,
            empty_root_requires_fail_closed: true,
        },
    );

    let started_at = Instant::now();
    let result =
        query_materialized_events_with_selected_group_owner_snapshot_and_request_scoped_omissions(
            &state,
            &ProjectionPolicy::default(),
            build_materialized_tree_request(
                b"/",
                true,
                None,
                ReadClass::TrustedMaterialized,
                Some("nfs2".to_string()),
            ),
            timeout,
            Some(&selected_group_source_status),
            Some(selected_group_sink_status),
            None,
            group_plan,
        )
        .await;
    let elapsed = started_at.elapsed();

    assert!(
        result.is_ok(),
        "pending collection-gap query should preserve proxy budget instead of waiting for one slow configured candidate; node_a_calls={} node_c_calls={} proxy_calls={} elapsed_ms={} err={:?}",
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_c_route.0),
        boundary.send_batch_count(&proxy_route.0),
        elapsed.as_millis(),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("pending-gap proxy-backed result"),
        &ProjectionPolicy::default(),
        "nfs2",
        b"/",
    )
    .expect("decode pending-gap proxy-backed response");
    assert!(payload.root.exists);
    assert!(
        elapsed < Duration::from_millis(3400),
        "slow candidate plus bounded collection-gap proxy fallback should not fall back to a full owner retry, elapsed={elapsed:?}"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "proxy fallback must retain enough budget after a slow heuristic candidate"
    );
    assert_eq!(
        boundary.send_batch_count(&node_a_route.0),
        1,
        "collection-gap recovery must not retry the configured primary with a full owner budget after its bounded candidate probe already timed out"
    );

    node_a_endpoint.shutdown(Duration::from_secs(3)).await;
    node_c_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_prefers_sink_scheduled_owner_over_stale_source_primary()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("retired-layout")).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs4".to_string(),
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
    let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs4".to_string()]),
        ]),
        groups: vec![sink_group_status("nfs4", true)],
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
                            "node-b",
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
        "test-stale-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let node_a_groups = node_a_groups_for_handler.clone();
            async move {
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode node-a internal query request");
                    node_a_groups.lock().expect("node-a groups lock").push(
                        params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for node-a request"),
                    );
                }
                Vec::new()
            }
        },
    );
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-scheduled-owner-sink-query-endpoint",
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

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/retired-layout",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(250),
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should prefer the sink-scheduled owner over stale source primary; sink_status_calls={} node_a_calls={} node_b_calls={} node_a_groups={:?} node_b_groups={:?} err={:?}",
        boundary.send_batch_count(&sink_status_route.0),
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_b_route.0),
        node_a_groups.lock().expect("node-a groups lock").clone(),
        node_b_groups.lock().expect("node-b groups lock").clone(),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("scheduled owner route result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/retired-layout",
    )
    .expect("decode selected-group scheduled owner response");
    assert!(payload.root.exists);
    assert!(
        node_a_groups.lock().expect("node-a groups lock").is_empty(),
        "stale source-primary owner should not receive the selected-group materialized request once sink scheduling points elsewhere"
    );
    assert_eq!(
        node_b_groups.lock().expect("node-b groups lock").as_slice(),
        &["nfs4".to_string()],
        "scheduled sink owner should receive the selected-group materialized request"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_prefers_sink_primary_object_ref_when_scheduled_groups_include_stale_node()
 {
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
    let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs4".to_string()]),
            ("node-b".to_string(), vec!["nfs4".to_string()]),
        ]),
        primary_host_ref_by_group: BTreeMap::from([("nfs4".to_string(), "node-b".to_string())]),
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
                            "node-d",
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
        "test-stale-scheduled-sink-query-endpoint",
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
        "test-primary-owner-sink-query-endpoint",
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

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(250),
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should prefer sink primary_object_ref over duplicated scheduled groups; sink_status_calls={} node_a_calls={} node_b_calls={} node_a_groups={:?} node_b_groups={:?} err={:?}",
        boundary.send_batch_count(&sink_status_route.0),
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_b_route.0),
        node_a_groups.lock().expect("node-a groups lock").clone(),
        node_b_groups.lock().expect("node-b groups lock").clone(),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group materialized route result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group materialized response");
    assert!(payload.root.exists);
    assert!(
        node_a_groups.lock().expect("node-a groups lock").is_empty(),
        "stale scheduled owner should not receive selected-group materialized query once sink primary_object_ref names another node"
    );
    assert_eq!(
        node_b_groups.lock().expect("node-b groups lock").as_slice(),
        &["nfs4".to_string()],
        "primary_object_ref owner should receive selected-group materialized query"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_does_not_let_stale_stream_counts_override_current_sink_owner()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    let node_c_root = tmp.path().join("node-c");
    fs::create_dir_all(node_a_root.join("current-layout")).expect("create node-a dir");
    fs::create_dir_all(node_c_root.join("stale-layout")).expect("create node-c dir");
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
            object_ref: "node-c::nfs4".to_string(),
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
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let node_c_route = sink_query_request_route_for("node-c");
    let node_a_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_c_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let node_a_groups_for_handler = node_a_groups.clone();
    let node_c_groups_for_handler = node_c_groups.clone();
    let status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs4".to_string()],
        )]),
        stream_applied_origin_counts_by_node: BTreeMap::from([(
            "node-c".to_string(),
            vec!["node-c::nfs4=9".to_string()],
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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink status snapshot");

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-current-owner-sink-status-with-stale-stream-counts",
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
        "test-current-owner-sink-query-endpoint",
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
                        real_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
                responses
            }
        },
    );
    let mut node_c_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_c_route.clone(),
        "test-stale-stream-count-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let node_c_groups = node_c_groups_for_handler.clone();
            async move {
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode node-c internal query request");
                    node_c_groups.lock().expect("node-c groups lock").push(
                        params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for node-c request"),
                    );
                }
                Vec::new()
            }
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );

    let result = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(250),
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should use the current sink owner instead of stale stream-applied counts; sink_status_calls={} node_a_calls={} node_c_calls={} node_a_groups={:?} node_c_groups={:?} err={:?}",
        boundary.send_batch_count(&sink_status_route.0),
        boundary.send_batch_count(&node_a_route.0),
        boundary.send_batch_count(&node_c_route.0),
        node_a_groups.lock().expect("node-a groups lock").clone(),
        node_c_groups.lock().expect("node-c groups lock").clone(),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("current owner route result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/",
    )
    .expect("decode selected-group current owner response");
    assert!(payload.root.exists);
    assert_eq!(
        node_a_groups.lock().expect("node-a groups lock").as_slice(),
        &["nfs4".to_string()],
        "current scheduled/primary sink owner should receive the selected-group materialized request"
    );
    assert!(
        node_c_groups.lock().expect("node-c groups lock").is_empty(),
        "stale stream-applied node must not receive selected-group materialized requests when it is no longer scheduled or primary"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_c_endpoint.shutdown(Duration::from_secs(2)).await;
}
