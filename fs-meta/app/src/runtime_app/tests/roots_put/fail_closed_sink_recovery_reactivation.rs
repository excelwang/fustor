    #[tokio::test]
    async fn stale_sink_events_tick_after_fail_closed_cleanup_and_later_reactivate_does_not_reenter_sink_worker()
     {
        struct SinkControlErrorHookReset;
        struct SinkWorkerControlFramePauseHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
            }
        }

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
            }
        }

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

        let source_wave = |generation| {
            vec![
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
            ]
        };
        let sink_wave = |generation| {
            vec![
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    generation,
                ),
            ]
        };

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        app.on_control_frame(&initial)
            .await
            .expect("initial source/sink wave should succeed");
        assert!(app.control_initialized());

        let _err_reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated fail-closed sink-only events deactivate failure".to_string(),
                ),
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect("events deactivate should fail-close into uninitialized cleanup");
        assert!(
            !app.control_initialized(),
            "fail-closed events deactivate should leave runtime uninitialized before later reactivation"
        );

        crate::workers::sink::clear_sink_worker_control_frame_error_hook();

        let mut reactivated = source_wave(4);
        reactivated.extend(sink_wave(4));
        app.on_control_frame(&reactivated)
            .await
            .expect("post-fail-closed source/sink reactivate should succeed");
        assert!(app.control_initialized());

        let mut later = source_wave(5);
        later.extend(sink_wave(5));
        app.on_control_frame(&later).await.expect(
            "later source/sink reactivate should succeed and supersede the prior generation",
        );
        assert!(app.control_initialized());

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let stale_tick = tokio::spawn({
            let app = app.clone();
            async move {
                app.on_control_frame(&[tick_envelope_with_route_key(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    4,
                )])
                .await
            }
        });

        tokio::time::timeout(Duration::from_millis(600), entered.notified())
            .await
            .expect_err(
                "stale sink-events tick from the prior post-fail-closed generation must not re-enter sink worker once a later generation is already active",
            );

        release.notify_waiters();
        stale_tick
            .await
            .expect("join stale sink tick control")
            .expect("stale sink tick should be ignored locally once a later generation is active");

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn later_exact_shaped_full_wave_reacquires_sink_worker_after_fail_closed_events_deactivate_and_intermediate_reactivation()
     {
        struct SinkControlErrorHookReset;
        struct SinkWorkerControlFramePauseHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
            }
        }

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
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

        let app = Arc::new(
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
                            passwd_path,
                            shadow_path,
                            ..api::ApiAuthConfig::default()
                        },
                    },
                },
                external_runtime_worker_binding("source", &source_socket_dir),
                external_runtime_worker_binding("sink", &sink_socket_dir),
                NodeId("node-a".into()),
                Some(boundary.clone()),
                Some(boundary.clone()),
                state_boundary,
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
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

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        initial.extend(facade_wave(2));
        app.on_control_frame(&initial)
            .await
            .expect("initial exact-shaped full wave should succeed");

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated fail-closed sink-only events deactivate failure".to_string(),
                ),
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect("events deactivate should fail-close into uninitialized cleanup");
        assert!(!app.control_initialized());

        crate::workers::sink::clear_sink_worker_control_frame_error_hook();

        let mut intermediate = source_wave(4);
        intermediate.extend(sink_wave(4));
        intermediate.extend(facade_wave(4));
        app.on_control_frame(&intermediate)
            .await
            .expect("intermediate exact-shaped full wave should recover after fail-closed cleanup");
        assert!(app.control_initialized());

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let later = tokio::spawn({
            let app = app.clone();
            async move {
                let mut later = source_wave(5);
                later.extend(sink_wave(5));
                later.extend(facade_wave(5));
                app.on_control_frame(&later).await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("later exact-shaped full wave should reach sink.apply before sink worker handoff is cut");
        app.sink
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown sink worker bridge during later exact-shaped sink activate");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(5), later)
            .await
            .expect("later exact-shaped full wave should settle promptly after the sink worker closes mid-handoff")
            .expect("join later exact-shaped full wave")
            .expect("later exact-shaped full wave should survive after fail-closed cleanup, intermediate reactivation, and mid-handoff sink-worker closure");

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn later_exact_shaped_full_wave_rematerializes_primary_roots_after_fail_closed_events_deactivate_and_intermediate_reactivation()
     {
        struct SinkControlErrorHookReset;
        struct SinkWorkerControlFramePauseHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
            }
        }

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        fs::write(
            nfs1.join("force-find-stress").join("seed.txt"),
            b"a
",
        )
        .expect("seed nfs1");
        fs::write(
            nfs2.join("force-find-stress").join("seed.txt"),
            b"b
",
        )
        .expect("seed nfs2");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let nfs1_source = nfs1.display().to_string();
        let nfs2_source = nfs2.display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

        let app = Arc::new(
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
                            passwd_path,
                            shadow_path,
                            ..api::ApiAuthConfig::default()
                        },
                    },
                },
                external_runtime_worker_binding("source", &source_socket_dir),
                external_runtime_worker_binding("sink", &sink_socket_dir),
                NodeId("node-a".into()),
                Some(boundary.clone()),
                Some(boundary.clone()),
                state_boundary,
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
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

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        initial.extend(facade_wave(2));
        app.on_control_frame(&initial)
            .await
            .expect("initial exact-shaped full wave should succeed");

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated fail-closed sink-only events deactivate failure".to_string(),
                ),
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect("events deactivate should fail-close into uninitialized cleanup");
        assert!(!app.control_initialized());

        crate::workers::sink::clear_sink_worker_control_frame_error_hook();

        let mut intermediate = source_wave(4);
        intermediate.extend(sink_wave(4));
        intermediate.extend(facade_wave(4));
        app.on_control_frame(&intermediate)
            .await
            .expect("intermediate exact-shaped full wave should recover after fail-closed cleanup");
        assert!(app.control_initialized());

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let later = tokio::spawn({
            let app = app.clone();
            async move {
                let mut later = source_wave(5);
                later.extend(sink_wave(5));
                later.extend(facade_wave(5));
                app.on_control_frame(&later).await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("later exact-shaped full wave should reach sink.apply before sink worker handoff is cut");
        app.sink
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown sink worker bridge during later exact-shaped sink activate");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(5), later)
            .await
            .expect("later exact-shaped full wave should settle promptly after the sink worker closes mid-handoff")
            .expect("join later exact-shaped full wave")
            .expect("later exact-shaped full wave should survive after fail-closed cleanup, intermediate reactivation, and mid-handoff sink-worker closure");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups")
                .unwrap_or_default();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && sink_groups == expected_groups
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < schedule_deadline,
                "timed out waiting for later-wave schedule convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let snapshot = app
                .sink
                .status_snapshot_with_failure()
                .await
                .expect("sink status snapshot");
            let ready_groups = snapshot
                .groups
                .iter()
                .filter(|group| group.is_ready())
                .map(|group| group.group_id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            if ready_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < readiness_deadline,
                "later exact-shaped full wave must restore initial-audit readiness for both groups: ready={ready_groups:?} snapshot={snapshot:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        for group_id in ["nfs1", "nfs2"] {
            let request = selected_group_file_request(b"/force-find-stress/seed.txt", group_id);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut direct_ready = false;
            let mut proxy_ready = false;
            let mut last_proxy_keys = Vec::<String>::new();
            let mut last_proxy_err = None::<String>;
            while tokio::time::Instant::now() < deadline {
                match app.query_tree(&request).await {
                    Ok(grouped) => {
                        if grouped
                            .get(group_id)
                            .is_some_and(|payload| payload.root.exists)
                        {
                            direct_ready = true;
                        }
                    }
                    Err(_) => {}
                }
                match selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".to_string()),
                    &request,
                )
                .await
                {
                    Ok(grouped) => {
                        last_proxy_keys = grouped.keys().cloned().collect();
                        last_proxy_err = None;
                        if grouped
                            .get(group_id)
                            .is_some_and(|payload| payload.root.exists)
                        {
                            proxy_ready = true;
                        }
                    }
                    Err(err) => {
                        last_proxy_err = Some(err.to_string());
                    }
                }
                if direct_ready && proxy_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                direct_ready,
                "later exact-shaped full wave must rematerialize selected group {group_id} on the direct sink query"
            );
            assert!(
                proxy_ready,
                "later exact-shaped full wave must rematerialize selected group {group_id} on selected-group proxy query: last_proxy_keys={last_proxy_keys:?} last_proxy_err={last_proxy_err:?}"
            );
        }

        let sink_status_events =
            internal_sink_status_request(boundary.clone(), NodeId("node-a".to_string()))
                .await
                .expect("internal sink-status request after later exact-shaped full wave");
        let sink_status_snapshots = sink_status_events
            .into_iter()
            .map(|event| {
                rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                    .expect("decode sink-status snapshot")
            })
            .collect::<Vec<_>>();
        assert!(
            sink_status_snapshots.iter().any(|snapshot| {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.is_ready())
                    .map(|group| group.group_id.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                ready_groups == std::collections::BTreeSet::from(["nfs1", "nfs2"])
            }),
            "internal sink-status route must expose the same ready groups as the recovered local sink after the later exact-shaped full wave: {sink_status_snapshots:?}"
        );

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn later_exact_shaped_full_wave_republishes_internal_sink_status_after_fail_closed_events_deactivate_and_intermediate_reactivation()
     {
        struct SinkControlErrorHookReset;
        struct SinkWorkerControlFramePauseHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
            }
        }

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
        fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
        fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a\n").expect("seed nfs1");
        fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b\n").expect("seed nfs2");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let nfs1_source = nfs1.display().to_string();
        let nfs2_source = nfs2.display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

        let app = Arc::new(
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
                            passwd_path,
                            shadow_path,
                            ..api::ApiAuthConfig::default()
                        },
                    },
                },
                external_runtime_worker_binding("source", &source_socket_dir),
                external_runtime_worker_binding("sink", &sink_socket_dir),
                NodeId("node-a".into()),
                Some(boundary.clone()),
                Some(boundary.clone()),
                state_boundary,
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    listener_scopes,
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
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
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
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

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        initial.extend(facade_wave(2));
        app.on_control_frame(&initial)
            .await
            .expect("initial exact-shaped full wave should succeed");

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::Timeout,
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect("events deactivate should fail-close into uninitialized cleanup");
        assert!(!app.control_initialized());

        crate::workers::sink::clear_sink_worker_control_frame_error_hook();

        let mut intermediate = source_wave(4);
        intermediate.extend(sink_wave(4));
        intermediate.extend(facade_wave(4));
        app.on_control_frame(&intermediate)
            .await
            .expect("intermediate exact-shaped full wave should recover after fail-closed cleanup");
        assert!(app.control_initialized());

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let later = tokio::spawn({
            let app = app.clone();
            async move {
                let mut later = source_wave(5);
                later.extend(sink_wave(5));
                later.extend(facade_wave(5));
                app.on_control_frame(&later).await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("later exact-shaped full wave should reach sink.apply before sink worker handoff is cut");
        app.sink
            .shutdown_shared_worker_for_tests(Duration::from_secs(2))
            .await
            .expect("shutdown sink worker bridge during later exact-shaped sink activate");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(5), later)
            .await
            .expect("later exact-shaped full wave should settle promptly after the sink worker closes mid-handoff")
            .expect("join later exact-shaped full wave")
            .expect("later exact-shaped full wave should survive after fail-closed cleanup, intermediate reactivation, and mid-handoff sink-worker closure");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let snapshot = app
                .sink
                .status_snapshot_with_failure()
                .await
                .expect("sink status snapshot");
            let ready_groups = snapshot
                .groups
                .iter()
                .filter(|group| group.is_ready())
                .map(|group| group.group_id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            if ready_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < readiness_deadline,
                "later exact-shaped full wave must restore initial-audit readiness for both groups before republishing sink-status: ready={ready_groups:?} snapshot={snapshot:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let sink_status_events = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_secs(2),
            Duration::from_millis(100),
        )
        .await
        .expect(
            "internal sink-status route should reply after later exact-shaped full wave recovery",
        );
        let sink_status_snapshots = sink_status_events
            .into_iter()
            .map(|event| {
                rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                    .expect("decode internal sink-status snapshot")
            })
            .collect::<Vec<_>>();
        assert!(
            sink_status_snapshots.iter().any(|snapshot| {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.is_ready())
                    .map(|group| group.group_id.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                ready_groups == std::collections::BTreeSet::from(["nfs1", "nfs2"])
            }),
            "internal sink-status must republish ready groups after later exact-shaped full wave recovery: {sink_status_snapshots:?}"
        );

        app.close().await.expect("close app");
    }

