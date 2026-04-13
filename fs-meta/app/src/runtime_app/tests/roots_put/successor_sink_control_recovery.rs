    #[tokio::test]
    async fn successor_sink_control_recovers_after_predecessor_facade_deactivate_resets_shared_worker()
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

        let predecessor = make_app();
        let successor = make_app();

        if cfg!(target_os = "linux") {
            match predecessor.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start predecessor app: {err}"),
            }
        } else {
            let err = predecessor
                .start()
                .await
                .expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let active_facade = match api::spawn(
            predecessor
                .config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            predecessor.node_id.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.source.clone(),
            predecessor.sink.clone(),
            predecessor.query_sink.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.facade_pending_status.clone(),
            predecessor.api_request_tracker.clone(),
            predecessor.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                predecessor
                    .close()
                    .await
                    .expect("close predecessor after bind restriction");
                successor
                    .close()
                    .await
                    .expect("close successor after bind restriction");
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *predecessor.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["listener-a".to_string()],
            handle: active_facade,
        });

        predecessor
            .on_control_frame(&[deactivate_envelope(
                execution_units::FACADE_RUNTIME_UNIT_ID,
                1,
            )])
            .await
            .expect("predecessor facade deactivate should complete");

        successor
            .on_control_frame(&[activate_envelope("runtime.exec.sink")])
            .await
            .expect(
                "successor sink control should recover after predecessor facade deactivate reset the shared worker bridge",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_second_sink_control_survives_late_predecessor_facade_deactivate() {
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

        let predecessor = make_app();
        let successor = make_app();

        if cfg!(target_os = "linux") {
            match predecessor.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start predecessor app: {err}"),
            }
        } else {
            let err = predecessor
                .start()
                .await
                .expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let active_facade = match api::spawn(
            predecessor
                .config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            predecessor.node_id.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.source.clone(),
            predecessor.sink.clone(),
            predecessor.query_sink.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.facade_pending_status.clone(),
            predecessor.api_request_tracker.clone(),
            predecessor.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                predecessor
                    .close()
                    .await
                    .expect("close predecessor after bind restriction");
                successor
                    .close()
                    .await
                    .expect("close successor after bind restriction");
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *predecessor.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["listener-a".to_string()],
            handle: active_facade,
        });

        successor
            .on_control_frame(&[
                activate_envelope("runtime.exec.source"),
                activate_envelope("runtime.exec.sink"),
            ])
            .await
            .expect("initial successor source/sink control should succeed");

        predecessor
            .on_control_frame(&[deactivate_envelope(
                execution_units::FACADE_RUNTIME_UNIT_ID,
                1,
            )])
            .await
            .expect("late predecessor facade deactivate should complete");

        successor
            .on_control_frame(&[activate_envelope("runtime.exec.sink")])
            .await
            .expect(
                "successor second sink-only control must survive late predecessor facade deactivate after the first batch already succeeded",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_second_sink_control_handoff_survives_late_predecessor_facade_deactivate() {
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

        let predecessor = make_app();
        let successor = make_app();

        if cfg!(target_os = "linux") {
            match predecessor.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start predecessor app: {err}"),
            }
        } else {
            let err = predecessor
                .start()
                .await
                .expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let active_facade = match api::spawn(
            predecessor
                .config
                .api
                .resolve_for_candidate_ids(&["listener-a".to_string()])
                .expect("resolve facade config"),
            predecessor.node_id.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.source.clone(),
            predecessor.sink.clone(),
            predecessor.query_sink.clone(),
            predecessor.runtime_boundary.clone(),
            predecessor.facade_pending_status.clone(),
            predecessor.api_request_tracker.clone(),
            predecessor.api_control_gate.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                predecessor
                    .close()
                    .await
                    .expect("close predecessor after bind restriction");
                successor
                    .close()
                    .await
                    .expect("close successor after bind restriction");
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *predecessor.api_task.lock().await = Some(FacadeActivation {
            route_key: facade_control_stream_route(),
            generation: 1,
            resource_ids: vec!["listener-a".to_string()],
            handle: active_facade,
        });

        successor
            .on_control_frame(&[
                activate_envelope("runtime.exec.source"),
                activate_envelope("runtime.exec.sink"),
            ])
            .await
            .expect("initial successor source/sink control should succeed");

        let control_entered = Arc::new(Notify::new());
        let control_release = Arc::new(Notify::new());
        let _control_pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: control_entered.clone(),
                release: control_release.clone(),
            },
        );

        let successor_control = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[activate_envelope("runtime.exec.sink")])
                    .await
            }
        });

        control_entered.notified().await;

        let deactivate_task = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[deactivate_envelope(
                        execution_units::FACADE_RUNTIME_UNIT_ID,
                        1,
                    )])
                    .await
            }
        });

        control_release.notify_waiters();
        successor_control
            .await
            .expect("join successor control")
            .expect(
                "successor second sink-only control must survive late predecessor facade deactivate after the first batch already succeeded",
            );
        deactivate_task
            .await
            .expect("join predecessor deactivate")
            .expect("late predecessor facade deactivate");

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

