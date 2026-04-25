#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_selected_group_materialized_route_reaches_owner_twice_for_same_owner_node() {
    let route = sink_query_request_route_for("node-a");
    let reply_route = format!("{}:reply", route.0);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let observed_groups = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_groups_for_handler = observed_groups.clone();
    let mut endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        route.clone(),
        "test-sequential-sink-query-endpoint",
        CancellationToken::new(),
        move |requests| {
            let observed_groups = observed_groups_for_handler.clone();
            async move {
                let mut responses = Vec::new();
                for req in requests {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode internal query request");
                    let group_id = params.scope.selected_group.clone().expect("selected group");
                    observed_groups
                        .lock()
                        .expect("observed groups lock")
                        .push(group_id.clone());
                    let payload = real_materialized_tree_payload_for_test(&params.scope.path);
                    responses.push(mk_event_with_correlation(
                        &group_id,
                        req.metadata()
                            .correlation_id
                            .expect("route request correlation"),
                        payload,
                    ));
                }
                responses
            }
        },
    );

    let first = route_materialized_events_via_node(
        boundary.clone(),
        NodeId("node-a".to_string()),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs1".to_string()),
        ),
        SelectedGroupOwnerRoutePlan::new(Duration::from_secs(2)),
    )
    .await;
    let second = route_materialized_events_via_node(
        boundary.clone(),
        NodeId("node-a".to_string()),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs2".to_string()),
        ),
        SelectedGroupOwnerRoutePlan::new(Duration::from_secs(2)),
    )
    .await;

    assert!(
        first.is_ok(),
        "first same-owner selected-group materialized route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} owner_groups={:?} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        observed_groups
            .lock()
            .expect("observed groups lock")
            .clone(),
        first.as_ref().err(),
    );
    assert!(
        second.is_ok(),
        "second same-owner selected-group materialized route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} owner_groups={:?} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        observed_groups
            .lock()
            .expect("observed groups lock")
            .clone(),
        second.as_ref().err(),
    );

    let first = first.expect("first route result");
    let second = second.expect("second route result");
    let first_payload = decode_materialized_selected_group_response(
        &first,
        &ProjectionPolicy::default(),
        "nfs1",
        b"/force-find-stress",
    )
    .expect("decode first selected-group materialized response");
    let second_payload = decode_materialized_selected_group_response(
        &second,
        &ProjectionPolicy::default(),
        "nfs2",
        b"/force-find-stress",
    )
    .expect("decode second selected-group materialized response");

    assert!(first_payload.root.exists);
    assert!(second_payload.root.exists);
    assert_eq!(
        observed_groups
            .lock()
            .expect("observed groups lock")
            .as_slice(),
        &["nfs1".to_string(), "nfs2".to_string()],
        "owner endpoint should receive both sequential same-owner selected-group materialized reads"
    );
    assert_eq!(
        boundary.send_batch_count(&route.0),
        2,
        "both sequential reads should send one internal sink-query request batch each"
    );
    assert_eq!(
        boundary.recv_batch_count(&route.0),
        2,
        "owner endpoint should receive both internal sink-query request batches"
    );
    assert_eq!(
        boundary.send_batch_count(&reply_route),
        2,
        "owner endpoint should send one reply batch for each sequential same-owner read"
    );
    assert_eq!(
        boundary.recv_batch_count(&reply_route),
        2,
        "query-side collection should receive one reply batch for each sequential same-owner read"
    );

    endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_group_materialized_route_reaches_external_owner_worker_twice_across_nodes() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let roots = vec![
        external_worker_root("nfs1", &nfs1),
        external_worker_root("nfs2", &nfs2),
    ];
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs1".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.11".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: nfs1.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.12".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: nfs2.clone(),
            fs_source: "nfs".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let cfg = SourceConfig {
        roots: roots.clone(),
        host_object_grants: grants.clone(),
        ..SourceConfig::default()
    };
    let source_runtime =
        FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
            .expect("init owner source runtime");
    let mut stream = source_runtime
        .pub_()
        .await
        .expect("start owner source stream");

    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink_worker = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink_worker.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");
    sink_worker
        .on_control_frame(vec![
            capanix_runtime_entry_sdk::control::encode_runtime_exec_control(
                &capanix_runtime_entry_sdk::control::RuntimeExecControl::Activate(
                    capanix_runtime_entry_sdk::control::RuntimeExecActivate {
                        route_key: crate::runtime::routes::ROUTE_KEY_QUERY.to_string(),
                        unit_id: "runtime.exec.sink".to_string(),
                        lease: None,
                        generation: 1,
                        expires_at_ms: 1,
                        bound_scopes: vec![
                            capanix_runtime_entry_sdk::control::RuntimeBoundScope {
                                scope_id: "nfs1".to_string(),
                                resource_ids: vec!["node-a::nfs1".to_string()],
                            },
                            capanix_runtime_entry_sdk::control::RuntimeBoundScope {
                                scope_id: "nfs2".to_string(),
                                resource_ids: vec!["node-a::nfs2".to_string()],
                            },
                        ],
                    },
                ),
            )
            .expect("encode sink activate"),
        ])
        .await
        .expect("activate sink worker groups");

    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < ready_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink_worker.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_materialized_selected_group_response(
            &sink_worker
                .materialized_query_with_failure(build_materialized_tree_request(
                    b"/force-find-stress",
                    false,
                    Some(0),
                    ReadClass::Materialized,
                    Some("nfs1".to_string()),
                ))
                .await
                .expect("query owner worker nfs1"),
            &ProjectionPolicy::default(),
            "nfs1",
            b"/force-find-stress",
        )
        .expect("decode owner worker nfs1")
        .root
        .exists;
        let nfs2_ready = decode_materialized_selected_group_response(
            &sink_worker
                .materialized_query_with_failure(build_materialized_tree_request(
                    b"/force-find-stress",
                    false,
                    Some(0),
                    ReadClass::Materialized,
                    Some("nfs2".to_string()),
                ))
                .await
                .expect("query owner worker nfs2"),
            &ProjectionPolicy::default(),
            "nfs2",
            b"/force-find-stress",
        )
        .expect("decode owner worker nfs2")
        .root
        .exists;
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    let source = source_facade_with_roots(roots, &grants);
    let owner_map_before = source
        .source_primary_by_group_snapshot_with_failure()
        .await
        .expect("source primary snapshot before");
    assert_eq!(
        owner_map_before.get("nfs1").map(String::as_str),
        Some("node-a::nfs1")
    );
    assert_eq!(
        owner_map_before.get("nfs2").map(String::as_str),
        Some("node-a::nfs2")
    );
    let state = test_api_state_for_route_source(
        source.clone(),
        sink_facade_with_group(&grants),
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let route = sink_query_request_route_for("node-a");
    let reply_route = format!("{}:reply", route.0);

    let first = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs1".to_string()),
        ),
        Duration::from_secs(5),
    )
    .await;
    let owner_map_after_first = source
        .source_primary_by_group_snapshot_with_failure()
        .await
        .expect("source primary snapshot after first");
    let second = query_materialized_events(
        state.backend.clone(),
        build_materialized_tree_request(
            b"/force-find-stress",
            true,
            None,
            ReadClass::Materialized,
            Some("nfs2".to_string()),
        ),
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(
        owner_map_after_first.get("nfs2").map(String::as_str),
        Some("node-a::nfs2"),
        "caller-side owner map should remain stable for the second same-owner selected-group read"
    );
    assert!(
        first.is_ok(),
        "first caller->owner selected-group route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        first.as_ref().err(),
    );
    assert!(
        second.is_ok(),
        "second caller->owner selected-group route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        second.as_ref().err(),
    );

    let first_payload = decode_materialized_selected_group_response(
        &first.expect("first selected-group route result"),
        &ProjectionPolicy::default(),
        "nfs1",
        b"/force-find-stress",
    )
    .expect("decode first selected-group response");
    let second_payload = decode_materialized_selected_group_response(
        &second.expect("second selected-group route result"),
        &ProjectionPolicy::default(),
        "nfs2",
        b"/force-find-stress",
    )
    .expect("decode second selected-group response");

    assert!(first_payload.root.exists);
    assert!(second_payload.root.exists);
    assert_eq!(
        boundary.send_batch_count(&route.0),
        2,
        "caller should send two direct internal sink-query request batches across nodes"
    );
    assert_eq!(
        boundary.recv_batch_count(&route.0),
        2,
        "owner worker runtime endpoint should receive both direct internal sink-query request batches"
    );
    assert_eq!(
        boundary.send_batch_count(&reply_route),
        2,
        "owner worker runtime endpoint should send one reply batch for each direct internal sink-query request"
    );
    assert_eq!(
        boundary.recv_batch_count(&reply_route),
        2,
        "caller should receive one reply batch for each direct internal sink-query request"
    );

    source_runtime
        .close()
        .await
        .expect("close owner source runtime");
    sink_worker.close().await.expect("close sink worker");
}
