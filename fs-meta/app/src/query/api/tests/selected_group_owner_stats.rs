#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_materialized_stats_groups_trusted_fails_closed_when_ready_root_stats_settle_zero()
{
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
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
    let node_a_route = sink_query_request_route_for("node-a");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-trusted-root-zero-stats-owner-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a stats request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a stats request");
                let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                    SubtreeStats::default(),
                ))
                .expect("encode zero stats payload");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a stats request correlation"),
                    payload,
                ));
            }
            responses
        },
    );

    let state =
        test_api_state_for_route_source(source, sink, boundary, NodeId("node-d".to_string()));
    let request_sink_status = SinkStatusSnapshot {
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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };

    let err = collect_materialized_stats_groups(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/".to_vec(),
            group: Some("nfs4".to_string()),
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::GroupKey,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        ReadClass::TrustedMaterialized,
        None,
        Some(&request_sink_status),
    )
    .await
    .expect_err("trusted root stats must fail closed when a ready group settles zero stats");

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted root stats with ready sink truth must fail closed as NotReady instead of rendering ok zero stats: {err:?}"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_materialized_stats_groups_trusted_prefers_richer_proxy_stats_when_ready_owner_returns_zeroish_stats()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs2".to_string(),
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
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let node_a_route = sink_query_request_route_for("node-a");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-trusted-root-zeroish-stats-owner-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a stats request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a stats request");
                let payload =
                    rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
                        total_nodes: 1,
                        total_dirs: 1,
                        total_files: 0,
                        total_size: 0,
                        latest_file_mtime_us: None,
                        ..SubtreeStats::default()
                    }))
                    .expect("encode zeroish stats payload");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a stats request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-root-richer-stats-proxy-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode proxy stats request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for proxy stats request");
                let payload =
                    rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(SubtreeStats {
                        total_nodes: 7,
                        total_files: 5,
                        total_dirs: 2,
                        total_size: 41,
                        latest_file_mtime_us: Some(1775804122265404),
                        ..SubtreeStats::default()
                    }))
                    .expect("encode richer proxy stats payload");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("proxy stats request correlation"),
                    payload,
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
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-a::nfs2".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
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
    };

    let groups = collect_materialized_stats_groups(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/".to_vec(),
            group: Some("nfs2".to_string()),
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::GroupKey,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        ReadClass::TrustedMaterialized,
        None,
        Some(&request_sink_status),
    )
    .await
    .expect("collect trusted materialized stats groups");

    assert_eq!(
        groups["nfs2"]["data"]["total_nodes"],
        serde_json::json!(7u64),
        "trusted-materialized stats must prefer richer proxy stats instead of settling a zero-ish owner stats payload for a ready group; groups={groups:?}"
    );
    assert_eq!(
        boundary.send_batch_count(&proxy_route.0),
        1,
        "selected-group stats should route one proxy fallback when the ready owner only returns a zero-ish stats payload"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_materialized_stats_groups_trusted_fails_closed_when_ready_root_stats_remain_zeroish_below_sink_truth()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a");
    fs::create_dir_all(node_a_root.join("layout-a")).expect("create node-a dir");
    let grants = vec![GrantedMountRoot {
        object_ref: "node-a::nfs2".to_string(),
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
    let source = source_facade_with_group("nfs2", &grants);
    let sink = sink_facade_with_group(&grants);
    let boundary = Arc::new(ReusableObservedRouteBoundary::default());
    let proxy_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY_PROXY)
        .expect("resolve sink-query-proxy route");
    let node_a_route = sink_query_request_route_for("node-a");
    let zeroish_stats_payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
        SubtreeStats {
            total_nodes: 1,
            total_dirs: 1,
            total_files: 0,
            total_size: 0,
            latest_file_mtime_us: None,
            ..SubtreeStats::default()
        },
    ))
    .expect("encode zeroish stats payload");
    let mut owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-trusted-root-stats-owner-zeroish-below-sink-truth-endpoint",
        CancellationToken::new(),
        {
            let zeroish_stats_payload = zeroish_stats_payload.clone();
            move |requests| {
                let zeroish_stats_payload = zeroish_stats_payload.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                            .expect("decode node-a stats request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for node-a stats request");
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("node-a stats request correlation"),
                            zeroish_stats_payload.clone(),
                        ));
                    }
                    responses
                }
            }
        },
    );
    let mut proxy_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        proxy_route.clone(),
        "test-trusted-root-stats-proxy-zeroish-below-sink-truth-endpoint",
        CancellationToken::new(),
        {
            let zeroish_stats_payload = zeroish_stats_payload.clone();
            move |requests| {
                let zeroish_stats_payload = zeroish_stats_payload.clone();
                async move {
                    let mut responses = Vec::new();
                    for req in requests {
                        let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                            .expect("decode proxy stats request");
                        let group_id = params
                            .scope
                            .selected_group
                            .clone()
                            .expect("selected group for proxy stats request");
                        responses.push(mk_event_with_correlation(
                            &group_id,
                            req.metadata()
                                .correlation_id
                                .expect("proxy stats request correlation"),
                            zeroish_stats_payload.clone(),
                        ));
                    }
                    responses
                }
            }
        },
    );

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs2".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs2".to_string(),
            primary_object_ref: "node-a::nfs2".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
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
    };

    let err = collect_materialized_stats_groups(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/".to_vec(),
            group: Some("nfs2".to_string()),
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::GroupKey,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        ReadClass::TrustedMaterialized,
        None,
        Some(&request_sink_status),
    )
    .await
    .expect_err(
        "trusted root stats must fail closed when owner and proxy both remain structural-root zeroish below ready sink truth",
    );

    assert!(
        matches!(err, CnxError::NotReady(_)),
        "trusted root stats with ready sink truth must fail closed instead of rendering ok zeroish structural-root stats: {err:?}"
    );

    owner_endpoint.shutdown(Duration::from_secs(2)).await;
    proxy_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_materialized_stats_groups_trusted_file_path_keeps_zero_groups_when_request_sink_snapshot_omits_later_groups()
 {
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
    let node_a_route = sink_query_request_route_for("node-a");
    let node_b_route = sink_query_request_route_for("node-b");
    let mut node_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-file-path-stats-coverage-node-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-a stats request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-a stats request");
                let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                    SubtreeStats::default(),
                ))
                .expect("encode zero stats payload");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-a stats request correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let mut node_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-file-path-stats-coverage-node-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            let mut responses = Vec::new();
            for req in requests {
                let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                    .expect("decode node-b stats request");
                let group_id = params
                    .scope
                    .selected_group
                    .clone()
                    .expect("selected group for node-b stats request");
                let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                    SubtreeStats::default(),
                ))
                .expect("encode node-b zero stats payload");
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata()
                        .correlation_id
                        .expect("node-b stats request correlation"),
                    payload,
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
    let request_sink_status = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs1".to_string(),
            primary_object_ref: "node-a::nfs1".to_string(),
            total_nodes: 7,
            live_nodes: 6,
            tombstoned_count: 1,
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
    };

    let groups = collect_materialized_stats_groups(
        &state,
        &ProjectionPolicy::default(),
        &NormalizedApiParams {
            path: b"/nested/child/deep.txt".to_vec(),
            group: None,
            recursive: true,
            max_depth: None,
            pit_id: None,
            group_order: GroupOrder::GroupKey,
            group_page_size: None,
            group_after: None,
            entry_page_size: None,
            entry_after: None,
            read_class: Some(ReadClass::TrustedMaterialized),
        },
        ReadClass::TrustedMaterialized,
        None,
        Some(&request_sink_status),
    )
    .await
    .expect("collect trusted materialized file-path stats groups");

    assert_eq!(
        groups.keys().cloned().collect::<Vec<_>>(),
        vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        "all roots must stay represented as zero-stats groups for trusted-materialized file-path stats even when the request sink snapshot only scheduled the first group: {groups:?}"
    );

    node_a_endpoint.shutdown(Duration::from_secs(2)).await;
    node_b_endpoint.shutdown(Duration::from_secs(2)).await;
}
