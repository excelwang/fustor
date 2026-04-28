async fn rpc_metrics_endpoint_returns_ok() {
    let resp = get_bound_route_metrics().await.into_response();
    assert_eq!(resp.status(), StatusCode::OK);
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_count_top_bucket_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-count&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-count");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-b");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/winner-b"))
        })));
    assert_eq!(json["groups"][0]["stability"]["state"], "not-evaluated");
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_age_top_bucket_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-age");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-a");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/winner-a"))
        })));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_group_order_file_age_keeps_empty_groups_eligible_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::FileAgeNoFiles);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=file-age&group_page_size=1")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "file-age");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    assert_eq!(json["groups"][0]["group"], "sink-a");
    assert!(json["groups"][0]["entries"]
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| {
            entry["path"]
                .as_str()
                .is_some_and(|path| path.ends_with("/empty-a"))
        })));
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
// @verify_spec("CONTRACTS.QUERY_OUTCOME.NO_CROSS_GROUP_ENTRY_MERGE", mode="system")
#[tokio::test]
async fn force_find_defaults_to_group_key_multi_group_response_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "group-key");
    assert!(json["groups"]
        .as_array()
        .is_some_and(|groups| !groups.is_empty()));
    assert!(
        json["group_page"]["returned_groups"]
            .as_u64()
            .unwrap_or_default()
            >= 1
    );
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.DUAL_QUERY_PATH_AVAILABILITY", mode="system")
#[tokio::test]
async fn force_find_explicit_group_returns_only_that_group_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group=sink-b")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_page"]["returned_groups"], 1);
    let groups = json["groups"].as_array().expect("groups array");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group"], "sink-b");
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE", mode="system")
#[tokio::test]
async fn force_find_defaults_when_query_params_omitted_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(json["group_order"], "group-key");
    assert!(json["groups"]
        .as_array()
        .is_some_and(|groups| !groups.is_empty()));
    assert_eq!(
        json["group_page"]["returned_groups"],
        json["groups"]
            .as_array()
            .map(|groups| groups.len())
            .unwrap_or_default()
    );
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY", mode="system")
#[tokio::test]
async fn projection_rpc_metrics_endpoint_shape_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/bound-route-metrics")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert!(payload.get("call_timeout_total").is_some());
    assert!(payload.get("correlation_mismatch_total").is_some());
    assert!(payload.get("uncorrelated_reply_total").is_some());
    assert!(payload.get("recv_loop_iterations").is_some());
    assert!(payload.get("pending_calls").is_some());
}

// @verify_spec("CONTRACTS.QUERY_OUTCOME.QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL", mode="system")
// @verify_spec("CONTRACTS.API_BOUNDARY.QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE", mode="system")
#[tokio::test]
async fn force_find_rejects_status_only_and_keeps_pagination_axis_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);
    let req = Request::builder()
        .uri("/on-demand-force-find?path=/&group_order=group-key&read_class=trusted-materialized")
        .method("GET")
        .body(Body::empty())
        .expect("build request");

    let resp = fixture.app.oneshot(req).await.expect("serve request");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json error payload");
    let msg = payload
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(msg.contains("read_class must be fresh on /on-demand-force-find"));
    assert_eq!(
        payload.get("code").and_then(|v| v.as_str()),
        Some("INVALID_INPUT")
    );
    assert_eq!(payload.get("path"), Some(&serde_json::json!("/")));
}

// @verify_spec("CONTRACTS.API_BOUNDARY.API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH", mode="system")
#[tokio::test]
async fn namespace_projection_endpoints_removed_local() {
    let fixture = ForceFindFixture::new(ForceFindFixtureScenario::Standard);

    let req_tree = Request::builder()
        .uri("/namespace-tree?path=/peers/sink-b/mnt/nfs1&best=false")
        .method("GET")
        .body(Body::empty())
        .expect("build request");
    let resp_tree = fixture
        .app
        .clone()
        .oneshot(req_tree)
        .await
        .expect("serve namespace tree request");
    assert_eq!(resp_tree.status(), StatusCode::NOT_FOUND);

    let req_force_find = Request::builder()
        .uri("/namespace-on-demand-force-find?path=/peers/sink-b/mnt/nfs1&group_order=group-key")
        .method("GET")
        .body(Body::empty())
        .expect("build request");
    let resp_force_find = fixture
        .app
        .oneshot(req_force_find)
        .await
        .expect("serve namespace force-find request");
    assert_eq!(resp_force_find.status(), StatusCode::NOT_FOUND);
}

include!("trusted_materialized_tree_root_parity.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_public_tree_requests_both_settle_under_multi_group_selected_route_stall() {
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
            host_ip: "10.0.0.1".to_string(),
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
    let owner_route = sink_query_request_route_for("node-a");
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
    .expect("encode multi-group sink-status payload");
    let boundary = Arc::new(
        MaterializedSelectedGroupTreeStallBoundary::new_with_sink_status_payload(
            owner_route.0.clone(),
            sink_status_route.0,
            sink_status_payload,
        ),
    );
    let app = create_local_router(
        sink,
        source,
        Some(boundary.clone()),
        NodeId("node-d".to_string()),
        Arc::new(RwLock::new(ProjectionPolicy {
            query_timeout_ms: Some(1200),
            ..ProjectionPolicy::default()
        })),
        Arc::new(Mutex::new(BTreeSet::new())),
        crate::api::state::ForceFindRunnerEvidence::default(),
    );

    let make_req = || {
        Request::builder()
            .uri("/tree?path=/&recursive=true&group_page_size=2")
            .method("GET")
            .body(Body::empty())
            .expect("build tree request")
    };

    let mut first = tokio::spawn({
        let app = app.clone();
        async move { app.oneshot(make_req()).await }
    });
    match tokio::time::timeout(
        Duration::from_secs(8),
        boundary.wait_for_owner_request_batches(1),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            first.abort();
            panic!("first tree request should reach the selected-group owner route");
        }
    }
    let mut second = tokio::spawn(async move { app.oneshot(make_req()).await });

    let first_response = match tokio::time::timeout(Duration::from_secs(8), &mut first).await {
        Ok(result) => result
            .expect("join first tree request")
            .expect("serve first tree request"),
        Err(_) => {
            first.abort();
            second.abort();
            panic!("first tree request should settle");
        }
    };
    let second_response = match tokio::time::timeout(Duration::from_secs(8), &mut second).await {
        Ok(result) => result
            .expect("join second tree request")
            .expect("serve second tree request"),
        Err(_) => {
            first.abort();
            second.abort();
            panic!("second tree request should settle");
        }
    };

    assert!(first_response.status().is_success() || first_response.status().is_server_error());
    assert!(second_response.status().is_success() || second_response.status().is_server_error());
}
