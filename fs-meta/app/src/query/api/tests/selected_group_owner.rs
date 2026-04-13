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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_stats_trusted_materialized_keeps_request_sink_snapshot_when_selected_group_stats_route_would_otherwise_drift()
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
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let node_a_route = sink_query_request_route_for("node-a");
    let node_b_route = sink_query_request_route_for("node-b");

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
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    };
    let request_sink_status_payload =
        rmp_serde::to_vec_named(&request_sink_status).expect("encode request sink-status payload");
    let stale_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode stale sink-status payload");
    let source_status_payload =
        MaterializedSelectedGroupTreeStallBoundary::source_status_payload_for_sink_status_payload(
            &request_sink_status_payload,
        );
    let sink_status_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink_status_calls_for_handler = sink_status_calls.clone();

    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-stats-request-lifetime-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let request_sink_status_payload = request_sink_status_payload.clone();
            let stale_sink_status_payload = stale_sink_status_payload.clone();
            let sink_status_calls = sink_status_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let call_idx =
                            sink_status_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let payload = if call_idx == 0 {
                            request_sink_status_payload.clone()
                        } else {
                            stale_sink_status_payload.clone()
                        };
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status request correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut source_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        source_status_route.clone(),
        "test-stats-request-lifetime-source-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let source_status_payload = source_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
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
    let mut primary_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_a_route.clone(),
        "test-primary-owner-stats-endpoint",
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
                        total_nodes: 7,
                        total_files: 6,
                        total_dirs: 3,
                        total_size: 50,
                        latest_file_mtime_us: Some(1775705272691087),
                        ..SubtreeStats::default()
                    }))
                    .expect("encode node-a stats payload");
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
    let mut stale_owner_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        node_b_route.clone(),
        "test-stale-owner-zero-stats-endpoint",
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

    tokio::time::sleep(Duration::from_millis(50)).await;

    let state = test_api_state_for_route_source(
        source,
        sink,
        boundary.clone(),
        NodeId("node-d".to_string()),
    );
    let groups = collect_materialized_stats_groups(
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
        Some(&request_sink_status),
    )
    .await
    .expect("collect trusted materialized stats groups");
    assert_eq!(
        boundary.send_batch_count(&node_a_route.0),
        1,
        "the request-lifetime ready sink snapshot should keep selected-group stats on the original primary owner; node_b_calls={} sink_status_calls={} groups={:?}",
        boundary.send_batch_count(&node_b_route.0),
        sink_status_calls.load(std::sync::atomic::Ordering::SeqCst),
        groups,
    );
    assert_eq!(
        boundary.send_batch_count(&node_b_route.0),
        0,
        "a later stale sink-status drift must not retarget selected-group stats to the zero-stats stale owner"
    );
    assert_eq!(
        groups["nfs4"]["data"]["total_nodes"],
        serde_json::json!(7u64),
        "trusted-materialized stats must preserve the request-lifetime sink snapshot; a later per-group sink-status drift must not retarget selected-group stats to a zero-stats stale owner; groups={groups:?}"
    );

    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    source_status_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    primary_owner_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    stale_owner_endpoint.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_trusted_exact_file_path_b64_keeps_first_request_materialized_owner_after_later_explicit_empty_sink_status_drift()
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
    let source_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_STATUS)
        .expect("resolve source-status route");
    let sink_status_route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_STATUS)
        .expect("resolve sink-status route");
    let owner_route_a = sink_query_request_route_for("node-a");
    let owner_route_b = sink_query_request_route_for("node-b");
    let file_path = b"/nested/child/deep.txt".to_vec();

    let source_status_payload = rmp_serde::to_vec_named(&SourceObservabilitySnapshot {
        lifecycle_state: "ready".into(),
        host_object_grants_version: 1,
        grants: grants.clone(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot {
            current_stream_generation: Some(9),
            logical_roots: vec![
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "nfs1".into(),
                    status: "ready".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "realtime_hotset_plus_audit".into(),
                },
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "nfs2".into(),
                    status: "ready".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "realtime_hotset_plus_audit".into(),
                },
                crate::source::SourceLogicalRootHealthSnapshot {
                    root_id: "nfs3".into(),
                    status: "ready".into(),
                    matched_grants: 1,
                    active_members: 1,
                    coverage_mode: "realtime_hotset_plus_audit".into(),
                },
            ],
            ..SourceStatusSnapshot::default()
        },
        source_primary_by_group: BTreeMap::from([
            ("nfs1".to_string(), "node-a::nfs1".to_string()),
            ("nfs2".to_string(), "node-a::nfs2".to_string()),
            ("nfs3".to_string(), "node-b::nfs3".to_string()),
        ]),
        last_force_find_runner_by_group: BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        scheduled_scan_groups_by_node: BTreeMap::new(),
        last_control_frame_signals_by_node: BTreeMap::new(),
        published_batches_by_node: BTreeMap::from([
            ("node-a".to_string(), 2),
            ("node-b".to_string(), 1),
        ]),
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
    let ready_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                primary_object_ref: "node-a::nfs1".to_string(),
                ..sink_group_status("nfs1", true)
            },
            crate::sink::SinkGroupStatusSnapshot {
                primary_object_ref: "node-a::nfs2".to_string(),
                ..sink_group_status("nfs2", true)
            },
            crate::sink::SinkGroupStatusSnapshot {
                primary_object_ref: "node-b::nfs3".to_string(),
                ..sink_group_status("nfs3", true)
            },
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode ready sink-status payload");
    let explicit_empty_sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            (
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            ),
            ("node-b".to_string(), vec!["nfs3".to_string()]),
        ]),
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
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
            },
            crate::sink::SinkGroupStatusSnapshot {
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
                overflow_pending_audit: false,
                initial_audit_completed: false,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs3".to_string(),
                primary_object_ref: "node-b::nfs3".to_string(),
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
            },
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode explicit-empty sink-status payload");
    let sink_status_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink_status_calls_for_handler = sink_status_calls.clone();

    let mut source_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        source_status_route.clone(),
        "test-public-exact-file-path-b64-source-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let source_status_payload = source_status_payload.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("source-status correlation"),
                            source_status_payload.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut sink_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        sink_status_route.clone(),
        "test-public-exact-file-path-b64-sink-status-endpoint",
        CancellationToken::new(),
        move |requests| {
            let ready_sink_status_payload = ready_sink_status_payload.clone();
            let explicit_empty_sink_status_payload = explicit_empty_sink_status_payload.clone();
            let sink_status_calls = sink_status_calls_for_handler.clone();
            async move {
                requests
                    .into_iter()
                    .map(|req| {
                        let call_idx =
                            sink_status_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let payload = if call_idx == 0 {
                            ready_sink_status_payload.clone()
                        } else {
                            explicit_empty_sink_status_payload.clone()
                        };
                        mk_event_with_correlation(
                            "node-d",
                            req.metadata()
                                .correlation_id
                                .expect("sink-status correlation"),
                            payload,
                        )
                    })
                    .collect::<Vec<_>>()
            }
        },
    );
    let mut owner_a_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_a.clone(),
        "test-public-exact-file-path-b64-owner-a-endpoint",
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
                    real_materialized_exact_file_payload_for_test(
                        &params.scope.path,
                        10,
                        1775979979238478,
                    )
                } else {
                    empty_materialized_tree_payload_for_test(&params.scope.path)
                };
                responses.push(mk_event_with_correlation(
                    &group_id,
                    req.metadata().correlation_id.expect("owner-a correlation"),
                    payload,
                ));
            }
            responses
        },
    );
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_b.clone(),
        "test-public-exact-file-path-b64-owner-b-endpoint",
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
                        req.metadata().correlation_id.expect("owner-b correlation"),
                        empty_materialized_tree_payload_for_test(&params.scope.path),
                    )
                })
                .collect::<Vec<_>>()
        },
    );

    let app = create_local_router(
        sink,
        source,
        Some(boundary.clone()),
        NodeId("node-d".to_string()),
        Arc::new(RwLock::new(ProjectionPolicy {
            query_timeout_ms: Some(3400),
            ..ProjectionPolicy::default()
        })),
        Arc::new(Mutex::new(BTreeSet::new())),
    );
    let path = "/nested/child/deep.txt";

    let first_req = Request::builder()
        .uri(format!(
            "/tree?path={path}&recursive=true&group_order=group-key&read_class=trusted-materialized"
        ))
        .method("GET")
        .body(Body::empty())
        .expect("build first tree request");
    let first_resp = app
        .clone()
        .oneshot(first_req)
        .await
        .expect("serve first tree request");
    assert_eq!(first_resp.status(), StatusCode::OK);
    let first_body = to_bytes(first_resp.into_body(), 1024 * 1024)
        .await
        .expect("first response body");
    let first_payload: serde_json::Value =
        serde_json::from_slice(&first_body).expect("first response json");

    let second_req = Request::builder()
            .uri(format!(
                "/tree?path_b64={}&recursive=true&group_order=group-key&read_class=trusted-materialized",
                B64URL.encode(path.as_bytes())
            ))
            .method("GET")
            .body(Body::empty())
            .expect("build second tree request");
    let second_resp = app
        .oneshot(second_req)
        .await
        .expect("serve second tree request");
    assert_eq!(second_resp.status(), StatusCode::OK);
    let second_body = to_bytes(second_resp.into_body(), 1024 * 1024)
        .await
        .expect("second response body");
    let second_payload: serde_json::Value =
        serde_json::from_slice(&second_body).expect("second response json");

    let decode_group_roots = |payload: &serde_json::Value| {
        payload["groups"]
            .as_array()
            .expect("groups array")
            .iter()
            .map(|group| {
                (
                    group["group"].as_str().unwrap_or_default().to_string(),
                    group["root"]["exists"].as_bool().unwrap_or(false),
                    group["root"]["path"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    group["root"]["is_dir"].as_bool().unwrap_or(false),
                    group["root"]["size"].as_u64().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
    };
    let first_group_roots = decode_group_roots(&first_payload);
    let second_group_roots = decode_group_roots(&second_payload);

    assert_eq!(
        first_group_roots,
        vec![
            ("nfs1".to_string(), true, path.to_string(), false, 10),
            ("nfs2".to_string(), false, path.to_string(), true, 0),
            ("nfs3".to_string(), false, path.to_string(), true, 0),
        ],
        "first trusted exact-file request should materialize nfs1 and keep nfs2/nfs3 empty: {first_payload}"
    );
    assert_eq!(
        second_group_roots, first_group_roots,
        "trusted exact-file path_b64 request must preserve the same materialized owner result as the prior path request instead of letting a later explicit-empty sink-status drift collapse nfs1 to an empty root: first={first_payload} second={second_payload}"
    );

    source_status_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
    let _ = file_path;
}

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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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
        Duration::from_secs(2),
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
        Duration::from_secs(2),
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
                .materialized_query(build_materialized_tree_request(
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
                .materialized_query(build_materialized_tree_request(
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
        .source_primary_by_group_snapshot()
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
        .source_primary_by_group_snapshot()
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
            overflow_pending_audit: false,
            initial_audit_completed: true,
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
