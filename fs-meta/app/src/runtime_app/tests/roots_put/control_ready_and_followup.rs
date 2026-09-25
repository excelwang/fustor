    #[tokio::test]
    async fn initialized_source_followup_does_not_wait_for_inflight_internal_sink_query_proxy_request()
     {
        let tmp = tempdir().expect("create temp dir");
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
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("test-root", &["listener-a"])],
                2,
            ),
        ])
        .await
        .expect("initial source/sink + peer sink query proxy wave should succeed");
        assert!(
            app.control_initialized(),
            "initial control wave should leave runtime initialized before testing followup continuity"
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = RuntimeProxyRequestPauseHookReset {
            label: "sink_query_proxy",
        };
        install_runtime_proxy_request_pause_hook(
            "sink_query_proxy",
            RuntimeProxyRequestPauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let request_task = tokio::spawn({
            let boundary = boundary.clone();
            async move {
                selected_group_proxy_tree(
                    boundary,
                    NodeId("single-app-node".into()),
                    &selected_group_dir_request(b"/", "test-root"),
                )
                .await
            }
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
            "initialized source followup should not wait for an in-flight internal sink query proxy request"
        );

        release.notify_waiters();
        request_task
            .await
            .expect("join sink query proxy request")
            .expect(
                "internal sink query proxy request should settle after route continuity followup",
            );
        followup_result
            .expect("join source followup via timeout completion")
            .expect("join source followup task")
            .expect("initialized source followup should complete after paused sink query proxy request release");
        assert!(
            app.control_initialized(),
            "initialized source followup should preserve runtime control initialization continuity"
        );

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn status_request_does_not_collect_remote_source_status_while_control_apply_is_inflight()
    {
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let root = tmp.path().join("root-a");
        fs::create_dir_all(&root).expect("create root");
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", &root)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", &root)],
                ..local_source_config()
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
        };
        let app = Arc::new(
            FSMetaApp::with_boundaries(
                cfg,
                NodeId("single-app-node".into()),
                Some(boundary.clone()),
            )
            .expect("init app"),
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

        let initial_control_result = app.on_control_frame(&[
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
                execution_units::FACADE_RUNTIME_UNIT_ID,
                facade_control_stream_route(),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_QUERY),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("test-root", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("test-root", &["listener-a"])],
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
        .await;
        match initial_control_result {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                app.close().await.expect("close app after bind restriction");
                return;
            }
            Err(err) => panic!("activate source/sink, facade, and internal status routes: {err}"),
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if app.current_facade_service_state().await == FacadeServiceState::Serving {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("complete facade route wave should publish the local listener as serving");

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

        let control_entered = Arc::new(Notify::new());
        let control_release = Arc::new(Notify::new());
        let _control_pause_reset = SourceApplyPauseHookReset;
        install_source_apply_pause_hook(
            SourceApplyPauseHook {
                entered: control_entered.clone(),
                release: control_release.clone(),
            },
        );

        let request_entered = Arc::new(Notify::new());
        let request_release = Arc::new(Notify::new());
        let _request_pause_reset = RuntimeProxyRequestPauseHookReset {
            label: "source_status",
        };
        install_runtime_proxy_request_pause_hook(
            "source_status",
            RuntimeProxyRequestPauseHook {
                entered: request_entered.clone(),
                release: request_release.clone(),
            },
        );

        let control_task = tokio::spawn({
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

        control_entered.notified().await;

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

        assert!(
            tokio::time::timeout(Duration::from_millis(600), request_entered.notified())
                .await
                .is_err(),
            "GET /status must not issue remote source-status collection while source control apply is in flight"
        );

        control_release.notify_waiters();
        control_task
            .await
            .expect("join control-frame task")
            .expect("handle control frame after status wait");

        // `/status` is a weak observation path. Once control settles it may either
        // collect live source status or return bounded degraded/not-ready evidence.
        request_release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(5), status_request)
            .await
            .expect("status request must settle inside its bounded observation window")
            .expect("join status request")
            .expect("status request should complete");
        assert!(
            response.status().is_success() || response.status().is_server_error(),
            "status request should settle after control readiness: {}",
            response.status()
        );
        app.close().await.expect("close app");
    }
