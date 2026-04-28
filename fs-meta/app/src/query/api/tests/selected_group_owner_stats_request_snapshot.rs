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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
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
        None,
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
async fn public_trusted_file_path_stats_preserves_request_source_roots_when_local_source_cache_is_stale_subset()
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
    let source = source_facade_with_roots(vec![RootSpec::new("nfs1", "/unused")], &grants);
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
    let file_path = "/nested/child/deep.txt";

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
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
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
    .expect("encode sink-status payload");

    let mut source_status_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        source_status_route.clone(),
        "test-trusted-file-path-stats-source-status-endpoint",
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
        "test-trusted-file-path-stats-sink-status-endpoint",
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
                                .expect("sink-status correlation"),
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
        "test-trusted-file-path-stats-owner-a-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-a stats request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-a stats request");
                    let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                        SubtreeStats::default(),
                    ))
                    .expect("encode owner-a zero stats payload");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata().correlation_id.expect("owner-a correlation"),
                        payload,
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    let mut owner_b_endpoint = ManagedEndpointTask::spawn(
        boundary.clone(),
        owner_route_b.clone(),
        "test-trusted-file-path-stats-owner-b-endpoint",
        CancellationToken::new(),
        move |requests| async move {
            requests
                .into_iter()
                .map(|req| {
                    let params = rmp_serde::from_slice::<InternalQueryRequest>(req.payload_bytes())
                        .expect("decode owner-b stats request");
                    let group_id = params
                        .scope
                        .selected_group
                        .clone()
                        .expect("selected group for owner-b stats request");
                    let payload = rmp_serde::to_vec_named(&MaterializedQueryPayload::Stats(
                        SubtreeStats::default(),
                    ))
                    .expect("encode owner-b zero stats payload");
                    mk_event_with_correlation(
                        &group_id,
                        req.metadata().correlation_id.expect("owner-b correlation"),
                        payload,
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
        crate::api::state::ForceFindRunnerEvidence::default(),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/stats?path={file_path}&recursive=true&group_order=group-key&read_class=trusted-materialized"
                ))
                .method("GET")
                .body(Body::empty())
                .expect("build stats request"),
        )
        .await
        .expect("serve stats request");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "trusted exact-file stats request should stay 200 while route source/sink status advertise three ready groups; owner_a_send_batches={} owner_b_send_batches={}",
        boundary.send_batch_count(&owner_route_a.0),
        boundary.send_batch_count(&owner_route_b.0),
    );
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("stats response body");
    let payload: serde_json::Value =
        serde_json::from_slice(&body).expect("stats response json");
    let mut groups = payload["groups"]
        .as_object()
        .expect("groups object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    groups.sort();

    assert_eq!(
        groups,
        vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        "trusted exact-file stats must preserve the request-lifetime source roots instead of collapsing to the stale local source cache subset: {payload}"
    );

    source_status_endpoint
        .shutdown(Duration::from_secs(2))
        .await;
    sink_status_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_a_endpoint.shutdown(Duration::from_secs(2)).await;
    owner_b_endpoint.shutdown(Duration::from_secs(2)).await;
}
