    #[tokio::test]
    async fn successor_exact_shaped_full_wave_does_not_wait_for_stale_predecessor_facade_only_recovery_after_second_wave_failure()
     {
        struct SourceControlErrorHookReset;
        struct SourceWorkerStartPauseHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerStartPauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_start_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let nfs1_source = nfs1.display().to_string();
        let nfs2_source = nfs2.display().to_string();
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
                            roots: vec![
                                worker_fs_watch_scan_root("nfs1", &nfs1_source),
                                worker_fs_watch_scan_root("nfs2", &nfs2_source),
                            ],
                            host_object_grants: vec![
                                worker_export_with_fs_source(
                                    "node-a::nfs1",
                                    "node-a",
                                    "10.0.0.11",
                                    &nfs1_source,
                                    nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-a::nfs2",
                                    "node-a",
                                    "10.0.0.12",
                                    &nfs2_source,
                                    nfs2.clone(),
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
                                passwd_path: passwd_path.clone(),
                                shadow_path: shadow_path.clone(),
                                ..api::ApiAuthConfig::default()
                            },
                        },
                    },
                    external_runtime_worker_binding("source", &source_socket_dir),
                    external_runtime_worker_binding("sink", &sink_socket_dir),
                    NodeId("node-a".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init app"),
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

        let predecessor_source_client = match &*predecessor.source {
            SourceFacade::Worker(client) => client.clone(),
            SourceFacade::Local(_) => panic!("expected external source worker client"),
        };

        let source_wave = |generation| {
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
            ]
        };
        let sink_wave = |generation| {
            let root_scopes = &[
                ("nfs1", &["node-a::nfs1"][..]),
                ("nfs2", &["node-a::nfs2"][..]),
            ];
            let mut signals = vec![
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_FORCE_FIND),
                    root_scopes,
                    generation,
                ),
            ];
            for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
                signals.push(activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_query_request_route_for(node_id).0,
                    root_scopes,
                    generation,
                ));
            }
            signals
        };
        let facade_wave = |generation| {
            let listener_scopes = &[("nfs1", &["listener-a"][..]), ("nfs2", &["listener-a"][..])];
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
            ]
        };

        let mut predecessor_initial = source_wave(2);
        predecessor_initial.extend(sink_wave(2));
        predecessor_initial.extend(facade_wave(2));
        predecessor
            .on_control_frame(&predecessor_initial)
            .await
            .expect("predecessor initial exact-shaped wave should succeed");

        let mut successor_initial = source_wave(2);
        successor_initial.extend(sink_wave(2));
        successor_initial.extend(facade_wave(2));
        successor
            .on_control_frame(&successor_initial)
            .await
            .expect("successor initial exact-shaped wave should succeed");

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                3,
            )])
            .await
            .expect("late predecessor sink events deactivate should succeed");

        let _source_error_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated predecessor second-wave source.apply failure".to_string(),
                ),
            },
        );
        let mut predecessor_second = source_wave(3);
        predecessor_second.extend(sink_wave(3));
        predecessor_second.extend(facade_wave(3));
        predecessor
            .on_control_frame(&predecessor_second)
            .await
            .expect_err("predecessor second exact-shaped wave should fail");
        crate::workers::source::clear_source_worker_control_frame_error_hook();
        assert!(
            !predecessor.control_initialized(),
            "predecessor should be uninitialized before stale facade-only recovery begins"
        );

        predecessor_source_client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown predecessor source worker before stale facade-only recovery");

        let entered = Arc::new(Notify::new());
        let _start_pause_reset = SourceWorkerStartPauseHookReset;
        crate::workers::source::install_source_worker_start_pause_hook(
            crate::workers::source::SourceWorkerStartPauseHook {
                entered: entered.clone(),
                release: Arc::new(Notify::new()),
            },
        );

        let predecessor_recovery = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 3)])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(300), entered.notified())
                .await
                .is_err(),
            "stale predecessor facade-only recovery must stay cleanup-only and must not re-enter source.start after the predecessor already failed its second wave",
        );

        let successor_started = Arc::new(Notify::new());
        let _start_hook_reset = RuntimeControlFrameStartHookReset;
        install_runtime_control_frame_start_hook(RuntimeControlFrameStartHook {
            entered: successor_started.clone(),
        });

        let successor_followup = tokio::spawn({
            let successor = successor.clone();
            async move {
                let mut followup = source_wave(4);
                followup.extend(sink_wave(4));
                followup.extend(facade_wave(4));
                successor.on_control_frame(&followup).await
            }
        });

        tokio::time::timeout(Duration::from_millis(600), successor_started.notified())
            .await
            .expect(
                "successor exact-shaped full wave should not be starved behind a stale predecessor facade-only recovery frame after the predecessor already failed its second wave",
            );

        tokio::time::timeout(Duration::from_secs(5), predecessor_recovery)
            .await
            .expect("join predecessor recovery task")
            .expect("join predecessor recovery task")
            .expect("predecessor stale facade-only recovery should finish");
        tokio::time::timeout(Duration::from_secs(5), successor_followup)
            .await
            .expect("join successor followup task")
            .expect("join successor followup task")
            .expect("successor followup exact-shaped full wave should succeed");

        let mut later_successor = source_wave(5);
        later_successor.extend(sink_wave(5));
        later_successor.extend(facade_wave(5));
        tokio::time::timeout(Duration::from_secs(5), successor.on_control_frame(&later_successor))
            .await
            .expect(
                "later successor exact-shaped full wave should settle promptly after stale predecessor facade-only recovery plus one successful intermediate reactivation",
            )
            .expect(
                "later successor exact-shaped full wave should still succeed after stale predecessor facade-only recovery plus one successful intermediate reactivation",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_second_exact_shaped_full_wave_source_apply_survives_interleaving_stale_predecessor_facade_only_recovery()
     {
        struct SourceControlErrorHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let nfs1_source = nfs1.display().to_string();
        let nfs2_source = nfs2.display().to_string();
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
                            roots: vec![
                                worker_fs_watch_scan_root("nfs1", &nfs1_source),
                                worker_fs_watch_scan_root("nfs2", &nfs2_source),
                            ],
                            host_object_grants: vec![
                                worker_export_with_fs_source(
                                    "node-a::nfs1",
                                    "node-a",
                                    "10.0.0.11",
                                    &nfs1_source,
                                    nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-a::nfs2",
                                    "node-a",
                                    "10.0.0.12",
                                    &nfs2_source,
                                    nfs2.clone(),
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
                                passwd_path: passwd_path.clone(),
                                shadow_path: shadow_path.clone(),
                                ..api::ApiAuthConfig::default()
                            },
                        },
                    },
                    external_runtime_worker_binding("source", &source_socket_dir),
                    external_runtime_worker_binding("sink", &sink_socket_dir),
                    NodeId("node-a".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init app"),
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

        let predecessor_source_client = match &*predecessor.source {
            SourceFacade::Worker(client) => client.clone(),
            SourceFacade::Local(_) => panic!("expected external source worker client"),
        };

        let source_wave = |generation| {
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
            ]
        };
        let sink_wave = |generation| {
            let root_scopes = &[
                ("nfs1", &["node-a::nfs1"][..]),
                ("nfs2", &["node-a::nfs2"][..]),
            ];
            let mut signals = vec![
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_FORCE_FIND),
                    root_scopes,
                    generation,
                ),
            ];
            for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
                signals.push(activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_query_request_route_for(node_id).0,
                    root_scopes,
                    generation,
                ));
            }
            signals
        };
        let facade_wave = |generation| {
            let listener_scopes = &[("nfs1", &["listener-a"][..]), ("nfs2", &["listener-a"][..])];
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
            ]
        };

        let mut predecessor_initial = source_wave(2);
        predecessor_initial.extend(sink_wave(2));
        predecessor_initial.extend(facade_wave(2));
        predecessor
            .on_control_frame(&predecessor_initial)
            .await
            .expect("predecessor initial exact-shaped wave should succeed");

        let mut successor_initial = source_wave(2);
        successor_initial.extend(sink_wave(2));
        successor_initial.extend(facade_wave(2));
        successor
            .on_control_frame(&successor_initial)
            .await
            .expect("successor initial exact-shaped wave should succeed");

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                3,
            )])
            .await
            .expect("late predecessor sink events deactivate should succeed");

        let _source_error_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated predecessor second-wave source.apply failure".to_string(),
                ),
            },
        );
        let mut predecessor_second = source_wave(3);
        predecessor_second.extend(sink_wave(3));
        predecessor_second.extend(facade_wave(3));
        predecessor
            .on_control_frame(&predecessor_second)
            .await
            .expect_err("predecessor second exact-shaped wave should fail");
        crate::workers::source::clear_source_worker_control_frame_error_hook();
        assert!(
            !predecessor.control_initialized(),
            "predecessor should be uninitialized before stale facade-only recovery begins"
        );

        predecessor_source_client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown predecessor source worker before stale facade-only recovery");

        let source_entered = Arc::new(Notify::new());
        let source_release = Arc::new(Notify::new());
        let _control_pause_reset = SourceWorkerControlFramePauseHookReset;
        crate::workers::source::install_source_worker_control_frame_pause_hook(
            crate::workers::source::SourceWorkerControlFramePauseHook {
                entered: source_entered.clone(),
                release: source_release.clone(),
            },
        );

        let successor_followup = tokio::spawn({
            let successor = successor.clone();
            async move {
                let mut followup = source_wave(4);
                followup.extend(sink_wave(4));
                followup.extend(facade_wave(4));
                successor.on_control_frame(&followup).await
            }
        });

        source_entered.notified().await;

        let predecessor_started = Arc::new(Notify::new());
        let _start_hook_reset = RuntimeControlFrameStartHookReset;
        install_runtime_control_frame_start_hook(RuntimeControlFrameStartHook {
            entered: predecessor_started.clone(),
        });

        let predecessor_recovery = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 3)])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_millis(600), predecessor_started.notified())
            .await
            .expect(
                "stale predecessor facade-only recovery must begin while the successor second exact-shaped full wave is paused in source.apply",
            );

        source_release.notify_waiters();

        predecessor_recovery
            .await
            .expect("join predecessor stale facade-only recovery")
            .expect("predecessor stale facade-only recovery should finish");
        successor_followup
            .await
            .expect("join successor second exact-shaped followup")
            .expect(
                "successor second exact-shaped full wave should survive an interleaving stale predecessor facade-only recovery during source.apply",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn stale_predecessor_facade_only_recovery_stays_cleanup_only_while_successor_second_wave_source_apply_is_live()
     {
        struct SourceControlErrorHookReset;
        struct SourceWorkerControlFramePauseHookReset;
        struct SourceWorkerStartPauseHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_pause_hook();
            }
        }

        impl Drop for SourceWorkerStartPauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_start_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let nfs1_source = nfs1.display().to_string();
        let nfs2_source = nfs2.display().to_string();
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
                            roots: vec![
                                worker_fs_watch_scan_root("nfs1", &nfs1_source),
                                worker_fs_watch_scan_root("nfs2", &nfs2_source),
                            ],
                            host_object_grants: vec![
                                worker_export_with_fs_source(
                                    "node-a::nfs1",
                                    "node-a",
                                    "10.0.0.11",
                                    &nfs1_source,
                                    nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-a::nfs2",
                                    "node-a",
                                    "10.0.0.12",
                                    &nfs2_source,
                                    nfs2.clone(),
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
                                passwd_path: passwd_path.clone(),
                                shadow_path: shadow_path.clone(),
                                ..api::ApiAuthConfig::default()
                            },
                        },
                    },
                    external_runtime_worker_binding("source", &source_socket_dir),
                    external_runtime_worker_binding("sink", &sink_socket_dir),
                    NodeId("node-a".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init app"),
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

        let predecessor_source_client = match &*predecessor.source {
            SourceFacade::Worker(client) => client.clone(),
            SourceFacade::Local(_) => panic!("expected external source worker client"),
        };

        let source_wave = |generation| {
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    generation,
                ),
            ]
        };
        let sink_wave = |generation| {
            let root_scopes = &[
                ("nfs1", &["node-a::nfs1"][..]),
                ("nfs2", &["node-a::nfs2"][..]),
            ];
            let mut signals = vec![
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                    root_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_FORCE_FIND),
                    root_scopes,
                    generation,
                ),
            ];
            for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
                signals.push(activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_query_request_route_for(node_id).0,
                    root_scopes,
                    generation,
                ));
            }
            signals
        };
        let facade_wave = |generation| {
            let listener_scopes = &[("nfs1", &["listener-a"][..]), ("nfs2", &["listener-a"][..])];
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    listener_scopes,
                    generation,
                ),
            ]
        };

        let mut predecessor_initial = source_wave(2);
        predecessor_initial.extend(sink_wave(2));
        predecessor_initial.extend(facade_wave(2));
        predecessor
            .on_control_frame(&predecessor_initial)
            .await
            .expect("predecessor initial exact-shaped wave should succeed");

        let mut successor_initial = source_wave(2);
        successor_initial.extend(sink_wave(2));
        successor_initial.extend(facade_wave(2));
        successor
            .on_control_frame(&successor_initial)
            .await
            .expect("successor initial exact-shaped wave should succeed");

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                3,
            )])
            .await
            .expect("late predecessor sink events deactivate should succeed");

        let _source_error_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated predecessor second-wave source.apply failure".to_string(),
                ),
            },
        );
        let mut predecessor_second = source_wave(3);
        predecessor_second.extend(sink_wave(3));
        predecessor_second.extend(facade_wave(3));
        predecessor
            .on_control_frame(&predecessor_second)
            .await
            .expect_err("predecessor second exact-shaped wave should fail");
        crate::workers::source::clear_source_worker_control_frame_error_hook();
        assert!(
            !predecessor.control_initialized(),
            "predecessor should be uninitialized before stale facade-only recovery begins"
        );

        predecessor_source_client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown predecessor source worker before stale facade-only recovery");

        let source_entered = Arc::new(Notify::new());
        let source_release = Arc::new(Notify::new());
        let _control_pause_reset = SourceWorkerControlFramePauseHookReset;
        crate::workers::source::install_source_worker_control_frame_pause_hook(
            crate::workers::source::SourceWorkerControlFramePauseHook {
                entered: source_entered.clone(),
                release: source_release.clone(),
            },
        );

        let successor_followup = tokio::spawn({
            let successor = successor.clone();
            async move {
                let mut followup = source_wave(4);
                followup.extend(sink_wave(4));
                followup.extend(facade_wave(4));
                successor.on_control_frame(&followup).await
            }
        });

        source_entered.notified().await;

        let predecessor_start_entered = Arc::new(Notify::new());
        let predecessor_start_release = Arc::new(Notify::new());
        let _start_pause_reset = SourceWorkerStartPauseHookReset;
        crate::workers::source::install_source_worker_start_pause_hook(
            crate::workers::source::SourceWorkerStartPauseHook {
                entered: predecessor_start_entered.clone(),
                release: predecessor_start_release.clone(),
            },
        );

        let predecessor_recovery = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 3)])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(
                Duration::from_millis(300),
                predecessor_start_entered.notified()
            )
            .await
            .is_err(),
            "stale predecessor facade-only recovery must stay cleanup-only and must not re-enter source.start while the successor second exact-shaped full wave is still paused in source.apply",
        );

        source_release.notify_waiters();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(600),
                predecessor_start_entered.notified()
            )
            .await
            .is_err(),
            "stale predecessor facade-only recovery must remain cleanup-only even after successor source.apply releases",
        );

        tokio::time::timeout(Duration::from_secs(5), predecessor_recovery)
            .await
            .expect("join predecessor stale facade-only recovery")
            .expect("join predecessor stale facade-only recovery")
            .expect("predecessor stale facade-only recovery should finish");
        tokio::time::timeout(Duration::from_secs(5), successor_followup)
            .await
            .expect("join successor second exact-shaped followup")
            .expect("join successor second exact-shaped followup")
            .expect(
                "successor second exact-shaped full wave should survive while stale predecessor facade-only recovery remains cleanup-only",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

