    fn roots_put_close_and_control_barrier_test_serial() -> &'static tokio::sync::Mutex<()> {
        static CELL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        CELL.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[tokio::test]
    async fn closing_app_waits_for_inflight_roots_put_before_tearing_down_facade() {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
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
                        local_listener_resources: vec![
                            api::config::ApiListenerResource {
                                resource_id: "listener-a".to_string(),
                                bind_addr: bind_addr.clone(),
                            },
                            api::config::ApiListenerResource {
                                resource_id: "listener-b".to_string(),
                                bind_addr: bind_addr.clone(),
                            },
                        ],
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

        app.on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("initialize app from runtime control before roots_put");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _reset = RootsPutPauseHookReset;
        crate::api::install_roots_put_pause_hook(crate::api::RootsPutPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
                    .send()
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("roots_put should reach the handler hook once runtime control readiness is initialized");
        let close_task = tokio::spawn({
            let app = app.clone();
            async move { app.close().await }
        });
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            !close_task.is_finished(),
            "app.close() must wait for the in-flight roots_put to finish before tearing down the facade"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "in-flight roots_put should complete before facade teardown: status={status} body={body}"
        );
        close_task
            .await
            .expect("join app close")
            .expect("close app after in-flight roots_put");
    }

    #[tokio::test]
    async fn closing_app_waits_for_control_ready_blocked_roots_put_to_settle_before_tearing_down_facade()
    {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
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
                        local_listener_resources: vec![
                            api::config::ApiListenerResource {
                                resource_id: "listener-a".to_string(),
                                bind_addr: bind_addr.clone(),
                            },
                            api::config::ApiListenerResource {
                                resource_id: "listener-b".to_string(),
                                bind_addr: bind_addr.clone(),
                            },
                        ],
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
        let _reset = RootsPutPauseHookReset;
        crate::api::install_roots_put_pause_hook(crate::api::RootsPutPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
                    .send()
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(300), entered.notified())
                .await
                .is_err(),
            "roots_put should still be blocked on control readiness before it reaches the handler hook"
        );

        let close_task = tokio::spawn({
            let app = app.clone();
            async move { app.close().await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !close_task.is_finished(),
            "app.close() must wait for a roots_put request that is already blocked on control readiness"
        );

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status == reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "roots_put blocked on control readiness during close should fail closed before facade teardown: status={status} body={body}"
        );
        assert!(
            body.contains("runtime control initializes the app"),
            "blocked roots_put should surface the control-readiness fail-closed body: {body}"
        );
        close_task
            .await
            .expect("join app close")
            .expect("close app after blocked roots_put settles");
    }

    #[tokio::test]
    async fn closing_app_waits_for_inflight_rescan_before_tearing_down_facade() {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
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
        let _reset = RescanPauseHookReset;
        crate::api::install_rescan_pause_hook(crate::api::RescanPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .post(format!("http://{bind_addr}/api/fs-meta/v1/index/rescan"))
                    .bearer_auth(token)
                    .json(&json!({}))
                    .send()
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("public /tree should reach sink query proxy before the late sink events deactivate wave runs");
        let close_task = tokio::spawn({
            let app = app.clone();
            async move { app.close().await }
        });
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            !close_task.is_finished(),
            "app.close() must wait for the in-flight rescan to finish before tearing down the facade"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        release.notify_waiters();

        let response = request
            .await
            .expect("join rescan request")
            .expect("rescan request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode rescan body");
        assert!(
            status.is_success(),
            "in-flight rescan should complete before facade teardown: status={status} body={body}"
        );
        close_task
            .await
            .expect("join app close")
            .expect("close app after in-flight rescan");
    }

    #[tokio::test]
    async fn closing_app_does_not_start_source_close_before_inflight_roots_put_reaches_worker_dispatch()
    {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
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

        app.on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("initialize app from runtime control before roots_put");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _roots_reset = RootsPutPauseHookReset;
        crate::api::install_roots_put_pause_hook(crate::api::RootsPutPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let close_started = Arc::new(Notify::new());
        let _close_reset = SourceWorkerCloseHookReset;
        crate::workers::source::install_source_worker_close_hook(
            crate::workers::source::SourceWorkerCloseHook {
                entered: close_started.clone(),
            },
        );

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
                    .send()
                    .await
            }
        });

        entered.notified().await;
        let close_task = tokio::spawn({
            let app = app.clone();
            async move { app.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), close_started.notified())
                .await
                .is_err(),
            "source close must not start while roots_put is still in flight before source worker dispatch"
        );

        release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "in-flight roots_put should complete before source close starts: status={status} body={body}"
        );
        close_task
            .await
            .expect("join app close")
            .expect("close app after roots_put");
    }

    #[tokio::test]
    async fn closing_app_does_not_start_facade_shutdown_while_inflight_roots_put_holds_response_open_after_updates()
    {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
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

        app.on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("initialize app from runtime control before roots_put");

        let response_entered = Arc::new(Notify::new());
        let response_release = Arc::new(Notify::new());
        let _response_reset = RootsPutBeforeResponseHookReset;
        crate::api::install_roots_put_before_response_hook(crate::api::RootsPutBeforeResponseHook {
            entered: response_entered.clone(),
            release: response_release.clone(),
        });

        let shutdown_started = Arc::new(Notify::new());
        let _shutdown_reset = FacadeShutdownStartHookReset;
        install_facade_shutdown_start_hook(FacadeShutdownStartHook {
            entered: shutdown_started.clone(),
        });

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
                    .send()
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), response_entered.notified())
            .await
            .expect(
                "roots_put should reach the before-response hook once runtime control initialization and source/sink updates complete",
            );
        let close_task = tokio::spawn({
            let app = app.clone();
            async move { app.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
                .await
                .is_err(),
            "facade shutdown must not start while roots_put is still holding the management response open after applying root updates"
        );

        response_release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "in-flight roots_put should complete before facade shutdown starts: status={status} body={body}"
        );
        close_task
            .await
            .expect("join app close")
            .expect("close app after roots_put");
    }

    #[tokio::test]
    async fn roots_put_waits_for_control_initialization_before_source_update_dispatch() {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
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

        let update_entered = Arc::new(Notify::new());
        let update_release = Arc::new(Notify::new());
        let _update_reset = SourceWorkerUpdateRootsHookReset;
        crate::workers::source::install_source_worker_update_roots_hook(
            crate::workers::source::SourceWorkerUpdateRootsHook {
                entered: update_entered.clone(),
                release: update_release.clone(),
            },
        );

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
                    .send()
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(600), update_entered.notified())
                .await
                .is_err(),
            "roots_put must not dispatch source update_logical_roots before runtime control initializes the restarted app"
        );

        app.on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("initialize app from runtime control");

        update_entered.notified().await;
        update_release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "roots_put should complete after runtime control initialization: status={status} body={body}"
        );

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn control_frame_waits_for_inflight_roots_put_before_source_reconfiguration() {
        let _serial = roots_put_close_and_control_barrier_test_serial()
            .lock()
            .await;
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
        let _roots_reset = RootsPutPauseHookReset;
        crate::api::install_roots_put_pause_hook(crate::api::RootsPutPauseHook {
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

        let roots_body = json!({
            "roots": [{
                "id": "test-root",
                "selector": { "fs_source": fs_source },
                "subpath_scope": "/",
                "watch": false,
                "scan": true,
            }]
        });
        let request = tokio::spawn({
            let client = client.clone();
            let bind_addr = bind_addr.clone();
            let token = token.clone();
            async move {
                client
                    .put(format!(
                        "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                    ))
                    .bearer_auth(token)
                    .json(&roots_body)
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
                .is_err(),
            "source control-frame reconfiguration must not start while roots_put is still in flight after previous source roots"
        );

        release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "in-flight roots_put should complete before source control reconfiguration starts: status={status} body={body}"
        );
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle control frame after roots_put");
        app.close().await.expect("close app");
    }
