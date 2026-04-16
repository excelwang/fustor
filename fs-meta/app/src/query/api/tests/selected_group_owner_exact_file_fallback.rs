#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_group_materialized_route_falls_back_to_generic_proxy_when_exact_file_owner_retry_still_returns_empty_for_ready_group()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("nested").join("child")).expect("create node-a nested dir");
    let grants = vec![GrantedMountRoot {
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
    }];
    let source = source_facade_with_group("nfs4", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let owner_route = sink_query_request_route_for("node-a");
    let owner_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let owner_calls_for_handler = owner_calls.clone();

    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-exact-file-empty-owner-retry-endpoint",
        CancellationToken::new(),
        move |requests| {
            let owner_calls = owner_calls_for_handler.clone();
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
                    owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        "test-exact-file-proxy-fallback-endpoint",
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
                        real_materialized_exact_file_payload_for_test(
                            &params.scope.path,
                            10,
                            1775979979238478,
                        ),
                    ))
                })
                .collect::<Vec<_>>()
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
            "node-a".to_string(),
            vec!["nfs4".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs4".to_string(),
            primary_object_ref: "node-a::nfs4".to_string(),
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

    let result = query_materialized_events_with_selected_group_owner_snapshot(
        &state,
        &ProjectionPolicy::default(),
        build_materialized_tree_request(
            b"/nested/child/deep.txt",
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
        "trusted-materialized exact-file route should fall back to generic proxy when a ready owner's initial reply and retry both settle as empty trees; owner_calls={} proxy_calls={} err={:?}",
        owner_calls.load(std::sync::atomic::Ordering::SeqCst),
        boundary.send_batch_count(&proxy_route.0),
        result.as_ref().err(),
    );
    let payload = decode_materialized_selected_group_response(
        &result.expect("selected-group exact-file fallback result"),
        &ProjectionPolicy::default(),
        "nfs4",
        b"/nested/child/deep.txt",
    )
    .expect("decode selected-group exact-file fallback response");
    assert!(
        payload.root.exists,
        "ready trusted exact-file selected-group route must not settle an empty owner reply after the retry also stays empty when the generic proxy can still materialize the file"
    );
    assert_eq!(payload.root.size, 10);
    assert_eq!(
        owner_calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "trusted exact-file selected-group route should try the owner twice before falling back to generic proxy"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "generic proxy should receive one fallback request after the ready exact-file owner retry still returns empty"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}
