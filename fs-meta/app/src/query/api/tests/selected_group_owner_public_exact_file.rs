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

