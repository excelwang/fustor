#[tokio::test]
async fn roots_put_contraction_returns_single_root_count_after_external_worker_update() {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
    fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_fs_source_root("nfs1", &nfs1.display().to_string()),
                        worker_fs_source_root("nfs2", &nfs2.display().to_string()),
                        worker_fs_source_root("nfs3", &nfs3.display().to_string()),
                    ],
                    host_object_grants: vec![
                        worker_export_with_fs_source(
                            "single-app-node::nfs1",
                            "single-app-node",
                            "127.0.0.1",
                            &nfs1.display().to_string(),
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "single-app-node::nfs2",
                            "single-app-node",
                            "127.0.0.1",
                            &nfs2.display().to_string(),
                            nfs2.clone(),
                        ),
                        worker_export_with_fs_source(
                            "single-app-node::nfs3",
                            "single-app-node",
                            "127.0.0.1",
                            &nfs3.display().to_string(),
                            nfs3.clone(),
                        ),
                    ],
                    ..SourceConfig::default()
                },
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker app"),
    );

    if cfg!(target_os = "linux") {
        match app.start().await {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("start app: {err}"),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

    let active_facade = match api::spawn(
        app.config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve facade config"),
        app.node_id.clone(),
        app.runtime_boundary.clone(),
        app.source.clone(),
        app.sink.clone(),
        app.query_sink.clone(),
        app.runtime_boundary.clone(),
        app.facade_pending_status.clone(),
        app.api_request_tracker.clone(),
        app.api_control_gate.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            app.close().await.expect("close app after bind restriction");
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let client = Client::new();
    let login = client
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "login failed: {}",
        login.status()
    );
    let login_body: serde_json::Value = login.json().await.expect("decode login");
    let token = login_body["token"].as_str().expect("token").to_string();

    let roots_body = json!({
        "roots": [{
            "id": "nfs1",
            "selector": { "fs_source": nfs1.display().to_string() },
            "subpath_scope": "/",
            "watch": false,
            "scan": true,
        }]
    });
    let response = client
        .put(format!(
            "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
        ))
        .bearer_auth(token)
        .json(&roots_body)
        .send()
        .await
        .expect("roots_put request");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("decode roots_put body");
    assert!(
        status.is_success(),
        "single-root roots_put should succeed: status={status} body={body}"
    );
    assert_eq!(
        body.get("roots_count").and_then(serde_json::Value::as_u64),
        Some(1),
        "single-root roots_put must report one logical root after external-worker update: {body}"
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn roots_put_contraction_with_fs_source_selectors_returns_single_root_count_after_external_worker_update()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");
    let fs_source_1 = "127.0.0.1:/exports/nfs1".to_string();
    let fs_source_2 = "127.0.0.1:/exports/nfs2".to_string();
    let fs_source_3 = "127.0.0.1:/exports/nfs3".to_string();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
    fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_fs_watch_scan_root("nfs1", &fs_source_1),
                        worker_fs_watch_scan_root("nfs2", &fs_source_2),
                        worker_fs_watch_scan_root("nfs3", &fs_source_3),
                    ],
                    host_object_grants: vec![
                        worker_export_with_fs_source(
                            "node-a::nfs1",
                            "node-a",
                            "10.0.0.41",
                            &fs_source_1,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-b::nfs1",
                            "node-b",
                            "10.0.0.42",
                            &fs_source_1,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-c::nfs1",
                            "node-c",
                            "10.0.0.43",
                            &fs_source_1,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "10.0.0.44",
                            &fs_source_2,
                            nfs2.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-c::nfs2",
                            "node-c",
                            "10.0.0.45",
                            &fs_source_2,
                            nfs2.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-d::nfs2",
                            "node-d",
                            "10.0.0.46",
                            &fs_source_2,
                            nfs2.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-b::nfs3",
                            "node-b",
                            "10.0.0.47",
                            &fs_source_3,
                            nfs3.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-d::nfs3",
                            "node-d",
                            "10.0.0.48",
                            &fs_source_3,
                            nfs3.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-e::nfs3",
                            "node-e",
                            "10.0.0.49",
                            &fs_source_3,
                            nfs3.clone(),
                        ),
                    ],
                    ..SourceConfig::default()
                },
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker app"),
    );

    if cfg!(target_os = "linux") {
        match app.start().await {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("start app: {err}"),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

    let active_facade = match api::spawn(
        app.config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve facade config"),
        app.node_id.clone(),
        app.runtime_boundary.clone(),
        app.source.clone(),
        app.sink.clone(),
        app.query_sink.clone(),
        app.runtime_boundary.clone(),
        app.facade_pending_status.clone(),
        app.api_request_tracker.clone(),
        app.api_control_gate.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            app.close().await.expect("close app after bind restriction");
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let client = Client::new();
    let login = client
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "login failed: {}",
        login.status()
    );
    let login_body: serde_json::Value = login.json().await.expect("decode login");
    let token = login_body["token"].as_str().expect("token").to_string();

    let roots_body = json!({
        "roots": [{
            "id": "nfs1",
            "selector": { "fs_source": fs_source_1 },
            "subpath_scope": "/",
            "watch": true,
            "scan": true,
        }]
    });
    let response = client
        .put(format!(
            "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
        ))
        .bearer_auth(token)
        .json(&roots_body)
        .send()
        .await
        .expect("roots_put request");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("decode roots_put body");
    assert!(
        status.is_success(),
        "single-root fs_source roots_put should succeed: status={status} body={body}"
    );
    assert_eq!(
        body.get("roots_count").and_then(serde_json::Value::as_u64),
        Some(1),
        "single-root fs_source roots_put must report one logical root after external-worker update: {body}"
    );

    app.close().await.expect("close app");
}
