    #[tokio::test]
    async fn closing_successor_app_waits_for_inflight_roots_put_before_shared_source_close() {
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

        let make_app = || {
            Arc::new(
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
                                passwd_path: passwd_path.clone(),
                                shadow_path: shadow_path.clone(),
                                ..api::ApiAuthConfig::default()
                            },
                        },
                    },
                    external_runtime_worker_binding("source", &source_socket_dir),
                    external_runtime_worker_binding("sink", &sink_socket_dir),
                    NodeId("single-app-node".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let app_1 = make_app();
        let app_2 = make_app();

        if cfg!(target_os = "linux") {
            match app_1.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start app: {err}"),
            }
        } else {
            let err = app_1.start().await.expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let active_facade = match api::spawn(
            app_1
                .config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            app_1.node_id.clone(),
            app_1.runtime_boundary.clone(),
            app_1.source.clone(),
            app_1.sink.clone(),
            app_1.query_sink.clone(),
            app_1.runtime_boundary.clone(),
            app_1.facade_pending_status.clone(),
            app_1.facade_service_state.clone(),
            app_1.api_request_tracker.clone(),
            app_1.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                app_1
                    .close()
                    .await
                    .expect("close app after bind restriction");
                app_2
                    .close()
                    .await
                    .expect("close second app after bind restriction");
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app_1.api_task.lock().await = Some(FacadeActivation {
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
            let app_2 = app_2.clone();
            async move { app_2.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), close_started.notified())
                .await
                .is_err(),
            "successor app close must not start shared source worker shutdown while the active instance roots_put is still in flight after previous source roots"
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
            "in-flight roots_put should complete before successor app closes the shared source worker: status={status} body={body}"
        );
        close_task
            .await
            .expect("join successor app close")
            .expect("close successor app after roots_put");
        app_1.close().await.expect("close first app");
    }

    #[tokio::test]
    async fn closing_successor_app_waits_for_inflight_roots_put_before_shared_sink_close() {
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

        let make_app = || {
            Arc::new(
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
                                passwd_path: passwd_path.clone(),
                                shadow_path: shadow_path.clone(),
                                ..api::ApiAuthConfig::default()
                            },
                        },
                    },
                    external_runtime_worker_binding("source", &source_socket_dir),
                    external_runtime_worker_binding("sink", &sink_socket_dir),
                    NodeId("single-app-node".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let app_1 = make_app();
        let app_2 = make_app();

        if cfg!(target_os = "linux") {
            match app_1.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start app: {err}"),
            }
        } else {
            let err = app_1.start().await.expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let active_facade = match api::spawn(
            app_1
                .config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            app_1.node_id.clone(),
            app_1.runtime_boundary.clone(),
            app_1.source.clone(),
            app_1.sink.clone(),
            app_1.query_sink.clone(),
            app_1.runtime_boundary.clone(),
            app_1.facade_pending_status.clone(),
            app_1.facade_service_state.clone(),
            app_1.api_request_tracker.clone(),
            app_1.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                app_1
                    .close()
                    .await
                    .expect("close app after bind restriction");
                app_2
                    .close()
                    .await
                    .expect("close second app after bind restriction");
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app_1.api_task.lock().await = Some(FacadeActivation {
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
        let _update_reset = SinkWorkerUpdateRootsHookReset;
        crate::workers::sink::install_sink_worker_update_roots_hook(
            crate::workers::sink::SinkWorkerUpdateRootsHook {
                entered: update_entered.clone(),
                release: update_release.clone(),
            },
        );

        let close_started = Arc::new(Notify::new());
        let _close_reset = SinkWorkerCloseHookReset;
        crate::workers::sink::install_sink_worker_close_hook(
            crate::workers::sink::SinkWorkerCloseHook {
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

        update_entered.notified().await;
        let close_task = tokio::spawn({
            let app_2 = app_2.clone();
            async move { app_2.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), close_started.notified())
                .await
                .is_err(),
            "successor app close must not start shared sink worker shutdown while the active instance roots_put is still in flight during sink update"
        );

        update_release.notify_waiters();

        let response = request
            .await
            .expect("join roots_put request")
            .expect("roots_put request should complete");
        let status = response.status();
        let body = response.text().await.expect("decode roots_put body");
        assert!(
            status.is_success(),
            "in-flight roots_put should complete before successor app closes the shared sink worker: status={status} body={body}"
        );
        close_task
            .await
            .expect("join successor app close")
            .expect("close successor app after roots_put");
        app_1.close().await.expect("close first app");
    }

