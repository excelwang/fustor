macro_rules! pit_public_missing_dir_follow_up_tests {
    () => {
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_settles_first_ranked_missing_subtree_after_first_empty_proxy_payload_without_reissuing_no_owner_proxy()
 {
    let source = source_facade_with_roots(vec![RootSpec::new("nfs1", "/unused")], &[]);
    let sink = sink_facade_with_group(&[]);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode sink-status payload");
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-missing-subtree-first-ranked-ready-sink-status-endpoint",
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
        "test-trusted-missing-subtree-first-ranked-proxy-endpoint",
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

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let params = NormalizedApiParams {
        path: b"/missing-dir".to_vec(),
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
        groups: vec![sink_group_status("nfs1", true)],
        ..SinkStatusSnapshot::default()
    };

    let session = build_tree_pit_session(
            &state,
            &ProjectionPolicy::default(),
            &params,
            Duration::from_millis(1800),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        )
        .await
        .expect(
            "trusted-materialized first-ranked missing subtree should settle once the generic proxy already returned an empty subtree payload instead of re-entering proxy fallback with no selected-group owner",
        );

    assert_eq!(session.groups.len(), 1);
    assert!(
        session.groups[0]
            .root
            .as_ref()
            .is_some_and(|root| !root.exists && root.path == b"/missing-dir"),
        "trusted-materialized first-ranked missing subtree should settle as an empty subtree instead of reissuing proxy work: {session:?}"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "trusted-materialized first-ranked missing subtree should not reissue the generic proxy after the first empty subtree payload already settled the selected group when no selected-group owner exists"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_settles_first_ranked_owner_known_missing_subtree_after_first_empty_proxy_payload_without_reissuing_proxy()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root_a = tmp.path().join("node-a-nfs1");
    fs::create_dir_all(&root_a).expect("create node-a nfs1 dir");
    let grants = vec![GrantedMountRoot {
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
    }];
    let source = source_facade_with_roots(vec![RootSpec::new("nfs1", "/unused")], &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
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
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-trusted-missing-subtree-first-ranked-owner-known-ready-sink-status-endpoint",
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
        "test-trusted-missing-subtree-first-ranked-owner-known-proxy-endpoint",
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
    let owner_route = sink_query_request_route_for("node-a");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route.clone(),
        "test-trusted-missing-subtree-first-ranked-owner-known-owner-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner query request");
                    mk_event_with_correlation(
                        "nfs1",
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
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
        NodeId("node-d".to_string()),
    );
    let params = NormalizedApiParams {
        path: b"/missing-dir".to_vec(),
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
            Duration::from_millis(1800),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        )
        .await
        .expect(
            "trusted-materialized first-ranked owner-known missing subtree should settle once the first empty proxy payload already proved the selected subtree is absent",
        );

    assert_eq!(session.groups.len(), 1);
    assert!(
        session.groups[0]
            .root
            .as_ref()
            .is_some_and(|root| !root.exists && root.path == b"/missing-dir"),
        "trusted-materialized first-ranked owner-known missing subtree should settle as an empty subtree instead of reissuing proxy work: {session:?}"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "trusted-materialized first-ranked owner-known missing subtree should not reissue the generic proxy after the first empty subtree payload already settled the selected group"
    );
    assert_eq!(
        boundary.send_batch_count(&owner_route.0),
        2,
        "trusted-materialized first-ranked owner-known missing subtree should stop after the initial owner attempt, one empty owner retry, and the first empty proxy payload instead of reissuing the selected-group owner during post-decode settle"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_allows_empty_later_ranked_group_for_missing_subtree_path()
 {
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
        "test-trusted-missing-subtree-ready-sink-status-endpoint",
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
        "test-trusted-missing-subtree-empty-proxy-endpoint",
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-missing-subtree-owner-a-endpoint",
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
        "test-trusted-missing-subtree-owner-b-endpoint",
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
            "trusted-materialized later-ranked group with a legitimately missing subtree path should not fail closed",
        );

    assert_eq!(session.groups.len(), 3);
    let nfs3 = session
        .groups
        .iter()
        .find(|group| group.group == "nfs3")
        .expect("nfs3 group snapshot");
    assert!(
        nfs3.root.as_ref().is_some_and(|root| !root.exists),
        "later-ranked trusted group with a missing subtree path should settle as an empty subtree instead of NOT_READY: {session:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

    };
}

macro_rules! pit_public_exact_file_missing_dir_follow_up_tests {
    () => {
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_bounds_exact_file_third_group_stage_after_first_group_and_empty_second_group()
 {
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
        "test-trusted-exact-file-ready-sink-status-endpoint",
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-owner-a-endpoint",
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
                let payload = if group_id == "nfs1" {
                    real_materialized_tree_payload_for_test(&params.scope.path)
                } else {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-exact-file-owner-b-stall-endpoint",
        CancellationToken::new(),
        move |_requests| async move { std::future::pending::<Vec<Event>>().await },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
            "trusted exact-file PIT must stay bounded after the first group succeeds and the second group empties, even when the third group stalls",
        )
        .expect_err("trusted exact-file PIT must fail closed instead of consuming the full caller timeout budget on the third stalled group");

    assert!(
        matches!(err, CnxError::Timeout | CnxError::NotReady(_)),
        "bounded exact-file later-group stage should still settle as timeout/not-ready: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_falls_back_to_proxy_when_later_ranked_exact_file_owner_times_out_for_missing_path()
 {
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
        "test-trusted-exact-file-ready-sink-status-timeout-fallback-endpoint",
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
        "test-trusted-exact-file-proxy-timeout-fallback-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-owner-a-timeout-fallback-endpoint",
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
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        empty_materialized_tree_payload_for_test(&params.scope.path)
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
        "test-trusted-exact-file-owner-b-timeout-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(900)).await;
            Vec::<Event>::new()
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
    .expect("trusted exact-file timeout fallback should settle within the PIT budget")
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
                    .map(|root| (root.exists, root.path.clone())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs3".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "trusted exact-file query should fall back to generic proxy when the later-ranked owner times out for a missing path: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_delayed_exact_file_reply_after_later_ranked_owner_timeout()
 {
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
        "test-trusted-exact-file-delayed-proxy-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(650)).await;
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-delayed-owner-endpoint",
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
                } else {
                    tokio::time::sleep(Duration::from_millis(900)).await;
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
    .expect("trusted delayed exact-file proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    let group_roots = session
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
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "trusted exact-file query should leave enough proxy budget for a delayed empty proxy reply after the later-ranked owner times out: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_exact_file_proxy_times_out_after_owner_timeout()
 {
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
        "test-trusted-exact-file-proxy-timeout-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-proxy-timeout-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            Vec::new()
        },
    );
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-owner-timeout-endpoint",
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
                        real_materialized_exact_file_payload_for_test(
                            &params.scope.path,
                            10,
                            1775709709283318,
                        ),
                    ));
                } else {
                    tokio::time::sleep(Duration::from_millis(900)).await;
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
        .expect("trusted exact-file proxy-timeout path should settle within the PIT budget")
        .expect_err(
            "trusted exact-file query must fail closed when a later-ranked owner timeout is followed by a proxy timeout",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "later-ranked exact-file owner+proxy timeout should fail closed as NotReady instead of leaking Timeout/504: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_fails_closed_when_middle_ranked_exact_file_proxy_times_out_after_owner_timeout()
 {
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
        "test-trusted-middle-ranked-exact-file-proxy-timeout-ready-sink-status-endpoint",
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
        "test-trusted-middle-ranked-exact-file-proxy-timeout-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy query request");
                let Some(group_id) = params.scope.selected_group.clone() else {
                    continue;
                };
                if group_id == "nfs2" {
                    tokio::time::sleep(Duration::from_millis(2_500)).await;
                    continue;
                }
                let payload = if group_id == "nfs1" {
                    real_materialized_exact_file_payload_for_test(
                        &params.scope.path,
                        10,
                        1775709709283318,
                    )
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-middle-ranked-exact-file-owner-timeout-endpoint",
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
                        real_materialized_exact_file_payload_for_test(
                            &params.scope.path,
                            10,
                            1775709709283318,
                        ),
                    ));
                } else if group_id == "nfs2" {
                    tokio::time::sleep(Duration::from_millis(900)).await;
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-middle-ranked-exact-file-owner-b-endpoint",
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
        path: b"/nested/child/deep.txt".to_vec(),
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
        .expect("trusted middle-ranked exact-file proxy-timeout path should settle within the PIT budget")
        .expect_err(
            "trusted exact-file query must fail closed when a middle-ranked owner timeout is followed by a proxy timeout after the first group already decoded",
        );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "middle-ranked exact-file owner+proxy timeout should fail closed as NotReady instead of leaking Timeout/504: {err:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_slower_delayed_exact_file_reply_after_later_ranked_owner_timeout()
 {
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
        "test-trusted-exact-file-slower-delayed-proxy-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-slower-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-slower-delayed-owner-endpoint",
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
                } else {
                    tokio::time::sleep(Duration::from_millis(3_000)).await;
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
            Duration::from_secs(60),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted slower delayed exact-file proxy fallback should settle within the PIT budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    let group_roots = session
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
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "trusted exact-file query should leave enough proxy budget for a slower delayed empty proxy reply after the later-ranked owner times out: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_delayed_exact_file_reply_after_first_ranked_owner_timeout()
 {
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
        "test-trusted-exact-file-first-ranked-delayed-proxy-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-first-ranked-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-first-ranked-delayed-owner-endpoint",
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
                    tokio::time::sleep(Duration::from_millis(3_000)).await;
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("owner request correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    ));
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
                Duration::from_secs(60),
                ObservationStatus::trusted_materialized(),
                Some(&request_sink_status),
            ),
        )
        .await
        .expect("trusted first-ranked delayed exact-file proxy fallback should settle within the PIT budget")
        .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    let group_roots = session
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
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "trusted exact-file query should leave enough proxy budget for a delayed first-ranked proxy reply after owner timeout: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_leaves_enough_proxy_budget_for_delayed_missing_subtree_reply_after_later_ranked_owner_timeout()
 {
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
        "test-trusted-missing-subtree-delayed-proxy-ready-sink-status-endpoint",
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
        "test-trusted-missing-subtree-delayed-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" || group_id == "nfs2" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-missing-subtree-delayed-owner-a-endpoint",
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
        "test-trusted-missing-subtree-delayed-owner-b-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(3_000)).await;
            Vec::<Event>::new()
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
            Duration::from_secs(60),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted missing-subtree delayed proxy fallback should settle within the PIT budget")
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
                    .map(|root| (root.exists, root.path.clone())),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            ("nfs1".to_string(), Some((true, b"/nested".to_vec()))),
            ("nfs2".to_string(), Some((true, b"/nested".to_vec()))),
            ("nfs3".to_string(), Some((false, b"/nested".to_vec()))),
        ],
        "trusted /nested query should leave enough proxy budget for a delayed empty proxy reply after the later-ranked owner times out: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_later_ranked_non_root_payload_when_same_batch_also_contains_empty_root()
 {
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
        "test-trusted-non-root-ready-sink-status-endpoint",
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-data-owner-a-endpoint",
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
        "test-trusted-data-owner-b-rich-then-empty-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner-b query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner-b request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("owner-b request correlation");
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
                        &[b"/data/a.txt", b"/data/b.txt"],
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
                        &[b"/data/a.txt", b"/data/b.txt"],
                    )),
                ));
                responses.push(Event::new(
                    EventMetadata {
                        origin_id: NodeId(group_id.clone()),
                        timestamp_us: 3,
                        logical_ts: None,
                        correlation_id: Some(correlation),
                        ingress_auth: None,
                        trace: None,
                    },
                    Bytes::from(empty_materialized_tree_payload_for_test(&params.scope.path)),
                ));
            }
            responses
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
    .expect("trusted non-recursive /data pit should settle within the PIT budget")
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
            ("nfs1".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs2".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs3".to_string(), Some((true, b"/data".to_vec(), 2))),
        ],
        "trusted non-recursive later-ranked group must preserve a richer same-batch payload instead of letting a later empty root overwrite it: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_later_ranked_non_root_proxy_payload_after_owner_timeout()
 {
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
        "test-trusted-later-ranked-non-root-proxy-rich-ready-sink-status-endpoint",
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
        "test-trusted-later-ranked-non-root-proxy-rich-then-empty-endpoint",
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
                if group_id == "nfs3" {
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
                            &[b"/data/a.txt", b"/data/b.txt"],
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
                            &[b"/data/a.txt", b"/data/b.txt"],
                        )),
                    ));
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: 3,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(empty_materialized_tree_payload_for_test(&params.scope.path)),
                    ));
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        ),
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
        "test-trusted-later-ranked-non-root-proxy-rich-owner-a-endpoint",
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
        "test-trusted-later-ranked-non-root-proxy-rich-owner-b-timeout-endpoint",
        CancellationToken::new(),
        move |_requests| async move {
            tokio::time::sleep(Duration::from_millis(3_000)).await;
            Vec::<Event>::new()
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
    .expect("trusted later-ranked non-root proxy rich fallback should settle within the PIT budget")
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
            ("nfs1".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs2".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs3".to_string(), Some((true, b"/data".to_vec(), 2))),
        ],
        "trusted later-ranked non-root proxy fallback must preserve a richer same-batch payload instead of letting a later empty root overwrite it after owner timeout: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_later_ranked_non_root_proxy_payload_after_owner_returns_empty_twice()
 {
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
        "test-trusted-later-ranked-non-root-owner-empty-then-proxy-rich-sink-status-endpoint",
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
        "test-trusted-later-ranked-non-root-owner-empty-then-proxy-rich-proxy-endpoint",
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
                if group_id == "nfs3" {
                    let rich = real_materialized_tree_payload_with_entries_for_test(
                        &params.scope.path,
                        &[b"/data/a.txt", b"/data/b.txt"],
                    );
                    let empty = empty_materialized_tree_payload_for_test(&params.scope.path);
                    for (ts, payload) in [
                        (1_u64, empty.clone()),
                        (2_u64, rich.clone()),
                        (3_u64, empty.clone()),
                    ] {
                        responses.push(Event::new(
                            EventMetadata {
                                origin_id: NodeId(group_id.clone()),
                                timestamp_us: ts,
                                logical_ts: None,
                                correlation_id: Some(correlation),
                                ingress_auth: None,
                                trace: None,
                            },
                            Bytes::from(payload),
                        ));
                    }
                } else {
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        correlation,
                        real_materialized_tree_payload_with_entries_for_test(
                            &params.scope.path,
                            &[b"/data/a.txt", b"/data/b.txt"],
                        ),
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
        "test-trusted-later-ranked-non-root-owner-empty-then-proxy-rich-owner-a-endpoint",
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
        "test-trusted-later-ranked-non-root-owner-empty-then-proxy-rich-owner-b-endpoint",
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
    .expect("trusted later-ranked owner-empty then proxy-rich non-root pit should settle")
    .expect("build tree pit session");

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
            ("nfs1".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs2".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs3".to_string(), Some((true, b"/data".to_vec(), 2))),
        ],
        "trusted later-ranked non-root proxy fallback after an empty owner retry must preserve a richer same-path payload instead of settling an ok empty root: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_later_ranked_non_root_payload_across_alternating_empty_and_rich_same_batch()
 {
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
        "test-trusted-alternating-empty-rich-ready-sink-status-endpoint",
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-alternating-empty-rich-owner-a-endpoint",
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
        "test-trusted-alternating-empty-rich-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode owner-b query request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for owner-b request");
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("owner-b request correlation");
                let rich = real_materialized_tree_payload_with_entries_for_test(
                    &params.scope.path,
                    &[b"/data/a.txt", b"/data/b.txt"],
                );
                let empty = empty_materialized_tree_payload_for_test(&params.scope.path);
                for (ts, payload) in [
                    (1_u64, empty.clone()),
                    (2_u64, rich.clone()),
                    (3_u64, empty.clone()),
                    (4_u64, rich.clone()),
                    (5_u64, empty.clone()),
                ] {
                    responses.push(Event::new(
                        EventMetadata {
                            origin_id: NodeId(group_id.clone()),
                            timestamp_us: ts,
                            logical_ts: None,
                            correlation_id: Some(correlation),
                            ingress_auth: None,
                            trace: None,
                        },
                        Bytes::from(payload),
                    ));
                }
            }
            responses
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
    .expect("trusted alternating empty/rich later-ranked non-root pit should settle")
    .expect("build tree pit session");

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
            ("nfs1".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs2".to_string(), Some((true, b"/data".to_vec(), 2))),
            ("nfs3".to_string(), Some((true, b"/data".to_vec(), 2))),
        ],
        "trusted later-ranked non-root group must preserve a richer payload across alternating empty/rich replies instead of settling an ok empty root: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_preserves_richer_first_ranked_exact_file_payload_when_same_batch_also_contains_empty_root()
 {
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
        "test-trusted-exact-file-ready-sink-status-endpoint",
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-owner-a-rich-then-empty-endpoint",
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
                let correlation = req
                    .metadata()
                    .correlation_id
                    .expect("owner-a request correlation");
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
                        Bytes::from(real_materialized_exact_file_payload_for_test(
                            &params.scope.path,
                            10,
                            1775709709283318,
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
    let owner_b_route = sink_query_request_route_for("node-b");
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_b_route.clone(),
        "test-trusted-exact-file-owner-b-empty-endpoint",
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
        path: b"/nested/child/deep.txt".to_vec(),
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
    .expect("trusted exact-file pit should settle within the PIT budget")
    .expect("build tree pit session");

    let group_roots = session
        .groups
        .iter()
        .map(|group| {
            (
                group.group.clone(),
                group.root.as_ref().map(|root| {
                    (
                        root.exists,
                        root.path.clone(),
                        root.is_dir,
                        root.size,
                        group.entries.len(),
                    )
                }),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec(), false, 10, 0)),
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec(), true, 0, 0)),
            ),
            (
                "nfs3".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec(), true, 0, 0)),
            ),
        ],
        "trusted exact-file first-ranked group must preserve a richer same-batch exact-file payload instead of letting a later empty same-path payload overwrite it: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_rejects_middle_ranked_exact_file_payload_with_mismatched_root_path()
 {
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
        "test-trusted-exact-file-mismatched-root-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-mismatched-root-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-mismatched-root-owner-a-endpoint",
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
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
                    } else {
                        real_materialized_tree_payload_for_test_with_root_path(b"/", b"/data/a.txt")
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
        "test-trusted-exact-file-mismatched-root-owner-b-endpoint",
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
        path: b"/nested/child/deep.txt".to_vec(),
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
    .expect("trusted exact-file mismatched-root path should settle within the PIT budget")
    .expect("build tree pit session");

    let group_roots = session
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
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs3".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "trusted exact-file query must reject a later-ranked payload whose root.path does not match the requested path: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_tree_pit_session_trusted_materialized_uses_stage_timeout_for_later_ranked_exact_file_owner_attempt()
 {
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
        "test-trusted-exact-file-stage-timeout-ready-sink-status-endpoint",
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
        "test-trusted-exact-file-stage-timeout-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .filter_map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode proxy query request");
                    let group_id = params.scope.selected_group?;
                    let payload = if group_id == "nfs1" {
                        real_materialized_tree_payload_for_test(&params.scope.path)
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
    let owner_a_route = sink_query_request_route_for("node-a");
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_a_route.clone(),
        "test-trusted-exact-file-stage-timeout-owner-endpoint",
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
                } else {
                    tokio::time::sleep(Duration::from_millis(3_000)).await;
                }
            }
            responses
        },
    );
    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let params = NormalizedApiParams {
        path: b"/nested/child/deep.txt".to_vec(),
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
            Duration::from_secs(60),
            ObservationStatus::trusted_materialized(),
            Some(&request_sink_status),
        ),
    )
    .await
    .expect("trusted exact-file stage-timeout path should settle within the stage budget")
    .expect("build tree pit session");

    assert_eq!(session.groups.len(), 2);
    let group_roots = session
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
        .collect::<Vec<_>>();
    assert_eq!(
        group_roots,
        vec![
            (
                "nfs1".to_string(),
                Some((true, b"/nested/child/deep.txt".to_vec()))
            ),
            (
                "nfs2".to_string(),
                Some((false, b"/nested/child/deep.txt".to_vec()))
            ),
        ],
        "later-ranked exact-file owner attempts must honor the bounded stage timeout instead of inheriting the full caller timeout: {group_roots:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
}

    };
}
