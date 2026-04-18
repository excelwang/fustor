    #[tokio::test]
    async fn facade_deactivate_waits_for_inflight_roots_put_after_sink_update_begins() {
        let _serial = facade_deactivate_barrier_test_serial().lock().await;

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
        app.api_control_gate.set_ready(true);
        let _ = app.current_facade_service_state().await;

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
                app.on_control_frame(&[deactivate_envelope(
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    1,
                )])
                .await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
                .await
                .is_err(),
            "facade deactivate must not start shutdown while roots_put is still dispatching update_logical_roots to the sink worker"
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
            "in-flight roots_put should complete before facade deactivate shutdown starts: status={status} body={body}"
        );
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle facade deactivate after roots_put");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn facade_deactivate_waits_for_inflight_status_after_remote_collection_begins() {
        let _serial = facade_deactivate_barrier_test_serial().lock().await;

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
                Some(boundary.clone()),
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

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
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
        .expect("activate source/sink and internal status routes");

        internal_source_status_snapshots(boundary.clone(), NodeId("single-app-node".into()))
            .await
            .expect("source-status route should be live before the status pause");
        internal_sink_status_request(boundary.clone(), NodeId("single-app-node".into()))
            .await
            .expect("sink-status route should be live before the status pause");

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

        let shutdown_started = Arc::new(Notify::new());
        let _shutdown_reset = FacadeShutdownStartHookReset;
        install_facade_shutdown_start_hook(FacadeShutdownStartHook {
            entered: shutdown_started.clone(),
        });

        let status_request = tokio::spawn({
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
                app.on_control_frame(&[deactivate_envelope(
                    execution_units::FACADE_RUNTIME_UNIT_ID,
                    1,
                )])
                .await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
                .await
                .is_err(),
            "facade deactivate must not start shutdown while GET /status is already inside remote status collection"
        );

        release.notify_waiters();

        let response = status_request
            .await
            .expect("join status request")
            .expect("status request should complete");
        assert!(
            response.status().is_success() || response.status().is_server_error(),
            "in-flight status should complete before facade deactivate shutdown starts: {}",
            response.status()
        );
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle facade deactivate after status request");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn initialized_inflight_internal_sink_status_timeout_returns_explicit_empty_reply() {
        struct SinkWorkerControlFramePauseHookReset;
        struct SinkWorkerStatusErrorHookReset;

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
            }
        }

        impl Drop for SinkWorkerStatusErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_status_error_hook();
            }
        }

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
                Some(boundary.clone()),
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

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
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
        .expect("activate sink and internal sink-status routes");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let _status_error_reset = SinkWorkerStatusErrorHookReset;
        crate::workers::sink::install_sink_worker_status_error_hook(
            crate::workers::sink::SinkWorkerStatusErrorHook {
                err: CnxError::Timeout,
            },
        );

        let sink_followup = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    3,
                )])
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("sink followup should pause in sink.apply before probing internal sink-status");

        let request_result = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("single-app-node".into()),
            Duration::from_millis(350),
            Duration::from_millis(50),
        )
        .await;

        release.notify_waiters();
        sink_followup
            .await
            .expect("join sink followup task")
            .expect("sink followup should settle after releasing sink.apply pause");

        let events = request_result.expect(
            "initialized internal sink-status timeout while sink control is inflight must fail closed with an explicit empty reply instead of caller timeout",
        );
        let snapshots = events
            .into_iter()
            .map(|event| {
                rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                    .expect("decode internal sink-status snapshot")
            })
            .collect::<Vec<_>>();
        assert!(
            snapshots.iter().any(|snapshot| {
                snapshot.groups.is_empty()
                    && snapshot.scheduled_groups_by_node.is_empty()
                    && snapshot.last_control_frame_signals_by_node.is_empty()
            }),
            "initialized inflight internal sink-status timeout must emit an explicit empty reply: {snapshots:?}"
        );

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn uninitialized_cleanup_only_facade_deactivate_does_not_wait_for_inflight_status_request()
    {
        let _serial = facade_deactivate_barrier_test_serial().lock().await;

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
                Some(boundary.clone()),
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

        app.on_control_frame(&[
            activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                &[("test-root", &["listener-a"])],
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
        .expect("activate source/sink and internal status routes");

        internal_source_status_snapshots(boundary.clone(), NodeId("single-app-node".into()))
            .await
            .expect("source-status route should be live before the status pause");
        internal_sink_status_request(boundary.clone(), NodeId("single-app-node".into()))
            .await
            .expect("sink-status route should be live before the status pause");

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

        let status_request = tokio::spawn({
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
        app.mark_control_uninitialized_after_failure().await;
        assert!(
            !app.control_initialized(),
            "forced failure seam should leave runtime uninitialized before cleanup-only facade followup"
        );

        let control_task = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    3,
                )])
                .await
            }
        });
        let control_result = tokio::time::timeout(Duration::from_millis(600), control_task).await;
        assert!(
            control_result.is_ok(),
            "uninitialized cleanup-only facade deactivate must not wait for a stale in-flight GET /status request"
        );
        control_result
            .expect("cleanup-only deactivate join should finish before timeout")
            .expect("join cleanup-only deactivate task")
            .expect("cleanup-only facade deactivate should complete");

        release.notify_waiters();

        let response = status_request
            .await
            .expect("join status request")
            .expect("status request should complete");
        assert!(
            response.status().is_success() || response.status().is_server_error(),
            "paused status request should settle after release: {}",
            response.status()
        );

        app.close().await.expect("close app");
    }
