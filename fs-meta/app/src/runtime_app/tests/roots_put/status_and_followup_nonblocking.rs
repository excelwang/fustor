#[tokio::test]
async fn control_frame_does_not_wait_for_inflight_status_request_before_source_reconfiguration()
{
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let fs_source = tmp.path().display().to_string();
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
                    roots: vec![worker_fs_source_root("test-root", &fs_source)],
                    host_object_grants: vec![worker_export_with_fs_source(
                        "single-app-node::root-1",
                        "single-app-node",
                        "127.0.0.1",
                        &fs_source,
                        tmp.path().to_path_buf(),
                    )],
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
        app.facade_service_state.clone(),
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

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _status_reset = StatusPauseHookReset;
    crate::api::install_status_pause_hook(crate::api::StatusPauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_started = Arc::new(Notify::new());
    let _control_reset = SourceWorkerControlFrameHookReset;
    crate::workers::source::install_source_worker_control_frame_hook(
        crate::workers::source::SourceWorkerControlFrameHook {
            entered: control_started.clone(),
        },
    );

    let mut status_request = tokio::spawn({
        let client = client.clone();
        let bind_addr = bind_addr.clone();
        let token = token.clone();
        async move {
            client
                .get(format!("http://{bind_addr}/api/fs-meta/v1/status"))
                .bearer_auth(token)
                .send()
                .await
        }
    });

    entered.notified().await;
    let control_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[activate_envelope("runtime.exec.source")])
                .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(600), control_started.notified())
            .await
            .is_ok(),
        "source control-frame reconfiguration should not wait for an in-flight GET /status"
    );

    release.notify_waiters();

    let response = status_request
        .await
        .expect("join status request")
        .expect("status request should complete");
    assert!(
        response.status().is_success() || response.status().is_server_error(),
        "status request should settle after release: {}",
        response.status()
    );
    control_task
        .await
        .expect("join control-frame task")
        .expect("handle control frame after status request");
    app.close().await.expect("close app");
}

#[tokio::test]
async fn initialized_source_followup_does_not_wait_for_inflight_internal_source_status_request()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1_dir = tmp.path().join("nfs1");
    let nfs2_dir = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1_dir.join("force-find-stress")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2_dir.join("force-find-stress")).expect("create nfs2 dir");
    fs::write(nfs1_dir.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2_dir.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");
    let fs_source = tmp.path().display().to_string();
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
                    roots: vec![worker_fs_watch_scan_root("test-root", &fs_source)],
                    host_object_grants: vec![worker_export_with_fs_source(
                        "single-app-node::root-1",
                        "single-app-node",
                        "127.0.0.1",
                        &fs_source,
                        tmp.path().to_path_buf(),
                    )],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    app.on_control_frame(&[
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            &[("test-root", &["listener-a"])],
            2,
        ),
    ])
    .await
    .expect("initial source/sink + peer source-status wave should succeed");
    assert!(
        app.control_initialized(),
        "initial control wave should leave runtime initialized before testing followup continuity"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = RuntimeProxyRequestPauseHookReset {
        label: "source_status",
    };
    install_runtime_proxy_request_pause_hook(
        "source_status",
        RuntimeProxyRequestPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let request_task = tokio::spawn({
        let boundary = boundary.clone();
        async move {
            internal_source_status_snapshots_with_timeout(
                boundary,
                NodeId("single-app-node".into()),
                Duration::from_secs(8),
                Duration::from_millis(50),
            )
            .await
        }
    });

    entered.notified().await;

    let control_started = Arc::new(Notify::new());
    let _control_reset = SourceWorkerControlFrameHookReset;
    crate::workers::source::install_source_worker_control_frame_hook(
        crate::workers::source::SourceWorkerControlFrameHook {
            entered: control_started.clone(),
        },
    );

    let followup = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                3,
            )])
            .await
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(600), control_started.notified())
            .await
            .is_ok(),
        "initialized source followup should not wait for an in-flight internal source-status request before source.apply begins"
    );

    release.notify_waiters();
    request_task.abort();
    tokio::task::yield_now().await;

    followup
        .await
        .expect("join source followup")
        .expect("initialized source followup should complete after paused request release");
    assert!(
        app.control_initialized(),
        "initialized source followup should preserve runtime control initialization continuity"
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn initialized_source_followup_does_not_wait_for_inflight_internal_sink_status_request() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1_dir = tmp.path().join("nfs1");
    let nfs2_dir = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1_dir.join("force-find-stress")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2_dir.join("force-find-stress")).expect("create nfs2 dir");
    fs::write(nfs1_dir.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2_dir.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");
    let fs_source = tmp.path().display().to_string();
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
                    roots: vec![worker_fs_watch_scan_root("test-root", &fs_source)],
                    host_object_grants: vec![worker_export_with_fs_source(
                        "single-app-node::root-1",
                        "single-app-node",
                        "127.0.0.1",
                        &fs_source,
                        tmp.path().to_path_buf(),
                    )],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    app.on_control_frame(&[
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            &[("test-root", &["listener-a"])],
            2,
        ),
    ])
    .await
    .expect("initial source/sink + peer sink-status wave should succeed");
    assert!(
        app.control_initialized(),
        "initial control wave should leave runtime initialized before testing followup continuity"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = RuntimeProxyRequestPauseHookReset {
        label: "sink_status",
    };
    install_runtime_proxy_request_pause_hook(
        "sink_status",
        RuntimeProxyRequestPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let request_task = tokio::spawn({
        let boundary = boundary.clone();
        async move { internal_sink_status_request(boundary, NodeId("single-app-node".into())).await }
    });
    entered.notified().await;

    let mut followup = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                3,
            )])
            .await
        }
    });

    let followup_result = tokio::time::timeout(Duration::from_millis(600), &mut followup).await;
    assert!(
        followup_result.is_ok(),
        "initialized source followup should not wait for an in-flight internal sink-status request"
    );

    release.notify_waiters();
    request_task
        .await
        .expect("join sink-status request")
        .expect("internal sink-status request after pause release");
    followup_result
        .expect("join source followup via timeout completion")
        .expect("join source followup task")
        .expect("initialized source followup should complete after paused sink-status request release");
    assert!(
        app.control_initialized(),
        "initialized source followup should preserve runtime control initialization continuity"
    );

    app.close().await.expect("close app");
}

