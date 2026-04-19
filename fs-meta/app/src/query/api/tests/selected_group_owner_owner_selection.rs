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
