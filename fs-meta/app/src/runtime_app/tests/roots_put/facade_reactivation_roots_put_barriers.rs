    #[tokio::test]
    async fn facade_reactivation_waits_for_inflight_roots_put_after_source_update_begins() {
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

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["listener-a".to_string()],
            }],
        )
        .await
        .expect("queue pending facade replacement");

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

        app.on_control_frame(&[activate_envelope(execution_units::SOURCE_RUNTIME_UNIT_ID)])
            .await
            .expect("initialize source runtime control before roots_put");

        let update_entered = Arc::new(Notify::new());
        let update_release = Arc::new(Notify::new());
        let _update_reset = SourceWorkerUpdateRootsHookReset;
        crate::workers::source::install_source_worker_update_roots_hook(
            crate::workers::source::SourceWorkerUpdateRootsHook {
                entered: update_entered.clone(),
                release: update_release.clone(),
            },
        );

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

        update_entered.notified().await;
        let control_task = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[trusted_exposure_confirmed_envelope(
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    2,
                )])
                .await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
                .await
                .is_err(),
            "facade reactivation must not start shutdown while roots_put is still dispatching update_logical_roots to the source worker"
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
            "in-flight roots_put should complete before facade reactivation shutdown starts: status={status} body={body}"
        );
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle facade reactivation after roots_put");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn facade_reactivation_waits_for_inflight_roots_put_after_sink_update_begins() {
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

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["listener-a".to_string()],
            }],
        )
        .await
        .expect("queue pending facade replacement");

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

        app.on_control_frame(&[activate_envelope(execution_units::SOURCE_RUNTIME_UNIT_ID)])
            .await
            .expect("initialize source runtime control before roots_put");

        let update_entered = Arc::new(Notify::new());
        let update_release = Arc::new(Notify::new());
        let _update_reset = SinkWorkerUpdateRootsHookReset;
        crate::workers::sink::install_sink_worker_update_roots_hook(
            crate::workers::sink::SinkWorkerUpdateRootsHook {
                entered: update_entered.clone(),
                release: update_release.clone(),
            },
        );

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

        update_entered.notified().await;
        let control_task = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[trusted_exposure_confirmed_envelope(
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    2,
                )])
                .await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
                .await
                .is_err(),
            "facade reactivation must not start shutdown while roots_put is still dispatching update_logical_roots to the sink worker"
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
            "in-flight roots_put should complete before facade reactivation shutdown starts: status={status} body={body}"
        );
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle facade reactivation after roots_put");
        app.close().await.expect("close app");
    }
