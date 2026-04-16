#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_when_owner_empty_tree_only_matches_selected_group_after_policy_normalization()
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
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-b");

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-normalized-empty-owner-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner query request");
                responses.push(mk_event_with_correlation(
                    "node-b::nfs4",
                    req.metadata()
                        .correlation_id
                        .expect("owner request correlation"),
                    empty_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-normalized-proxy-fallback-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                responses.push(mk_event_with_correlation(
                    "node-b::nfs4",
                    req.metadata()
                        .correlation_id
                        .expect("proxy request correlation"),
                    real_materialized_tree_payload_for_test(&params.scope.path),
                ));
            }
            responses
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let selected_group_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-b::nfs4".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let policy = ProjectionPolicy {
        member_grouping: MemberGroupingStrategy::MountPoint,
        mount_point_by_object_ref: HashMap::from([(
            "node-b::nfs4".to_string(),
            "nfs4".to_string(),
        )]),
        ..ProjectionPolicy::default()
    };

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &policy,
        build_materialized_tree_request(
            b"/",
            true,
            None,
            ReadClass::TrustedMaterialized,
            Some("nfs4".to_string()),
        ),
        Duration::from_millis(1200),
        Some(selected_group_sink_status),
        true,
        true,
    )
    .await;

    assert!(
        result.is_ok(),
        "selected-group materialized route should fall back to generic proxy when the owner's empty tree only matches the selected group after policy normalization; owner_send_batches={} proxy_send_batches={} err={:?}",
        boundary.send_batch_count(&owner_route.0),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("normalized empty owner fallback result"),
        &policy,
        "nfs4",
        b"/",
    )
    .expect("decode normalized empty owner fallback response");
    assert!(payload.root.exists);
    assert_eq!(boundary.send_batch_count(&owner_route.0), 1);
    assert_eq!(boundary.send_batch_count(&proxy_route.0), 1);

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}
