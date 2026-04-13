    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/close_and_control_barriers.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/contraction_single_root_count.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/external_worker_update_later_refresh.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/status_and_followup_nonblocking.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/control_ready_and_followup.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/successor_shared_close_barriers.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/facade_reactivation_roots_put_barriers.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/facade_deactivate_barriers.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/facade_deactivate_shared_worker_and_successor_recovery.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/query_peer_deactivate_shared_worker_handoffs.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/successor_sink_control_recovery.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/successor_live_sink_schedule.rs"
    ));

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_successor_sink_only_mid_handoff_close_falls_into_cleanup_only_facade_then_one_uninitialized_sink_retry()
     {
        struct SinkWorkerControlFramePauseHookReset;

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

        let make_app = || {
            Arc::new(
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
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor = make_app();
        let successor = make_app();
        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        predecessor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial predecessor sink events wave should succeed");

        successor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial successor sink events wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_sink_only = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[activate_envelope_with_route_key_and_scope_rows(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        format!("{}.stream", ROUTE_KEY_EVENTS),
                        &[("test-root", &["single-app-node::root-1"])],
                        3,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("live successor sink-only control should reach sink.apply");
        successor
            .sink
            .close()
            .await
            .expect("close live successor shared sink worker bridge mid-handoff");
        release.notify_waiters();

        let err = successor_sink_only
            .await
            .expect("join live successor sink-only control")
            .expect_err(
                "preserved exact chain requires the live successor sink-only lane to fail under mid-handoff close before cleanup-only facade followup begins",
            );
        assert!(
            is_retryable_worker_control_reset(&err),
            "expected retryable sink transport-close error, got: {err}"
        );

        assert!(
            !successor.control_initialized(),
            "failed live successor sink-only lane must leave the runtime uninitialized before the cleanup-only facade followup",
        );

        successor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                3,
            )])
            .await
            .expect("cleanup-only facade followup should complete on uninitialized runtime");
        assert!(
            !successor.control_initialized(),
            "cleanup-only facade followup must leave the runtime uninitialized",
        );

        successor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                4,
            )])
            .await
            .expect("one uninitialized sink-only retry should complete after cleanup-only facade followup");
        assert!(
            !successor.control_initialized(),
            "one uninitialized sink-only retry must leave the runtime uninitialized",
        );
        match successor
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .get(&sink_events_route)
        {
            Some(SinkControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 4,
                "first uninitialized shared-owner sink cleanup retry must advance retained desired state to the later deactivate generation",
            ),
            other => panic!(
                "expected retained sink deactivate for {sink_events_route:?} after the first uninitialized retry: {other:?}"
            ),
        }

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_successor_sink_only_mid_handoff_close_allows_direct_uninitialized_sink_retry_without_facade_cleanup()
     {
        struct SinkWorkerControlFramePauseHookReset;

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

        let make_app = || {
            Arc::new(
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
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor = make_app();
        let successor = make_app();
        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        predecessor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial predecessor sink events wave should succeed");

        successor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial successor sink events wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_sink_only = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[activate_envelope_with_route_key_and_scope_rows(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        format!("{}.stream", ROUTE_KEY_EVENTS),
                        &[("test-root", &["single-app-node::root-1"])],
                        3,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("live successor sink-only control should reach sink.apply");
        successor
            .sink
            .close()
            .await
            .expect("close live successor shared sink worker bridge mid-handoff");
        release.notify_waiters();

        let err = successor_sink_only
            .await
            .expect("join live successor sink-only control")
            .expect_err(
                "live successor sink-only lane should fail under mid-handoff close before the direct uninitialized retry",
            );
        assert!(
            is_retryable_worker_control_reset(&err),
            "expected retryable sink transport-close error, got: {err}"
        );
        assert!(
            !successor.control_initialized(),
            "failed live successor sink-only lane must leave the runtime uninitialized before the direct retry",
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            successor.on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                4,
            )]),
        )
        .await
        .expect(
            "direct uninitialized sink-only retry should settle promptly after the live sink-only cutover lane fails",
        )
        .expect(
            "direct uninitialized sink-only retry should complete without requiring an intermediate facade cleanup frame",
        );

        assert!(
            !successor.control_initialized(),
            "direct uninitialized sink-only retry must leave the runtime uninitialized",
        );
        match successor
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .get(&sink_events_route)
        {
            Some(SinkControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 4,
                "direct uninitialized sink-only retry must advance retained desired state to the later deactivate generation",
            ),
            other => panic!(
                "expected retained sink deactivate for {sink_events_route:?} after the direct uninitialized retry: {other:?}"
            ),
        }

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn current_owner_sink_only_mid_handoff_close_fail_closes_into_direct_uninitialized_retry_without_in_process_shared_claims()
     {
        struct SinkWorkerControlFramePauseHookReset;

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

        let make_app = || {
            Arc::new(
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
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor = make_app();
        let successor = make_app();
        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        predecessor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_events_route.1.clone(),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial predecessor sink events wave should succeed");

        successor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_events_route.1.clone(),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial successor sink events wave should succeed");

        successor.shared_sink_route_claims.lock().await.clear();

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_sink_only = tokio::spawn({
            let successor = successor.clone();
            let sink_events_route_key = sink_events_route.1.clone();
            async move {
                successor
                    .on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        sink_events_route_key,
                        3,
                        "restart_deferred_retire_pending",
                        7,
                        11,
                        22,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("current-owner sink-only control should reach sink.apply");
        successor
            .sink
            .close()
            .await
            .expect("close current-owner sink worker bridge mid-handoff");
        release.notify_waiters();

        successor_sink_only
            .await
            .expect("join current-owner sink-only control")
            .expect(
                "current-owner sink-only lane should settle within the same control frame under mid-handoff close even when the restart path lacks in-process shared claims",
            );
        assert!(
            !successor.control_initialized(),
            "failed current-owner sink-only lane must leave the runtime uninitialized before direct retry",
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            successor.on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                sink_events_route.1.clone(),
                4,
            )]),
        )
        .await
        .expect(
            "direct uninitialized sink-only retry should settle promptly without in-process shared claims",
        )
        .expect(
            "direct uninitialized sink-only retry should complete after a current-owner mid-handoff close",
        );

        assert!(
            !successor.control_initialized(),
            "direct uninitialized sink-only retry must leave the runtime uninitialized",
        );
        match successor
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .get(&sink_events_route)
        {
            Some(SinkControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 4,
                "direct uninitialized sink-only retry must advance retained desired state to the later deactivate generation",
            ),
            other => panic!(
                "expected retained sink deactivate for {sink_events_route:?} after the direct uninitialized retry: {other:?}"
            ),
        }

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn current_owner_sink_only_mid_handoff_close_settles_within_same_control_frame_via_uninitialized_recovery()
     {
        struct SinkWorkerControlFramePauseHookReset;

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

        let make_app = || {
            Arc::new(
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
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor = make_app();
        let successor = make_app();
        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        predecessor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_events_route.1.clone(),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial predecessor sink events wave should succeed");

        successor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    sink_events_route.1.clone(),
                    &[("test-root", &["single-app-node::root-1"])],
                    2,
                ),
            ])
            .await
            .expect("initial successor sink events wave should succeed");

        successor.shared_sink_route_claims.lock().await.clear();

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_sink_only = tokio::spawn({
            let successor = successor.clone();
            let sink_events_route_key = sink_events_route.1.clone();
            async move {
                successor
                    .on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        sink_events_route_key,
                        3,
                        "restart_deferred_retire_pending",
                        7,
                        11,
                        22,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("current-owner sink-only control should reach sink.apply");
        successor
            .sink
            .close()
            .await
            .expect("close current-owner sink worker bridge mid-handoff");
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(5), successor_sink_only)
            .await
            .expect(
                "current-owner sink-only lane should settle within the same control frame after mid-handoff close",
            )
            .expect("join current-owner sink-only control")
            .expect(
                "current-owner sink-only lane should complete via in-frame uninitialized recovery instead of requiring a second control frame",
            );

        assert!(
            !successor.control_initialized(),
            "in-frame sink-only fail-closed recovery may still leave runtime uninitialized for later authoritative source replay",
        );
        match successor
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .get(&sink_events_route)
        {
            Some(SinkControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 3,
                "same-frame sink-only recovery must retain the incoming deactivate generation",
            ),
            other => panic!(
                "expected retained sink deactivate for {sink_events_route:?} after same-frame sink-only recovery: {other:?}"
            ),
        }

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn current_owner_sink_only_events_deactivate_fail_closes_within_same_control_frame_after_generation_two_source_sink_activate()
     {
        struct SinkControlErrorHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
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

        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        app.on_control_frame(&[
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                sink_events_route.1.clone(),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
        ])
        .await
        .expect(
            "generation-two source/sink activate should succeed before the sink-only cutover seam",
        );
        assert!(
            app.control_initialized(),
            "generation-two source/sink activate should leave runtime initialized before the fail-closed sink-only lane"
        );

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
            sink_events_route.1.clone(),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect(
            "current-owner sink-only events deactivate should settle within the same control frame by fail-closing into uninitialized state even when sink.apply reports an error after generation-two source/sink activate",
        );

        assert!(
            !app.control_initialized(),
            "same-frame fail-closed sink-only recovery may leave runtime uninitialized for later authoritative source replay",
        );
        match app
            .retained_sink_control_state
            .lock()
            .await
            .active_by_route
            .get(&sink_events_route)
        {
            Some(SinkControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 3,
                "same-frame sink-only fail-close must retain the incoming deactivate generation",
            ),
            other => panic!(
                "expected retained sink deactivate for {sink_events_route:?} after same-frame sink-only fail-close: {other:?}"
            ),
        }

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn current_owner_sink_only_events_deactivate_retries_stale_drained_fenced_pid_after_generation_two_source_sink_activate()
     {
        struct SinkControlErrorHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
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

        let sink_events_route = (
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_EVENTS),
        );

        app.on_control_frame(&[
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                sink_events_route.1.clone(),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
        ])
        .await
        .expect(
            "generation-two source/sink activate should succeed before the stale drained/fenced sink-only cutover seam",
        );
        assert!(
            app.control_initialized(),
            "generation-two source/sink activate should leave runtime initialized before retryable sink-only cutover",
        );

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::AccessDenied(
                    "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SINK_RUNTIME_UNIT_ID,
            sink_events_route.1.clone(),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect(
            "retryable stale drained/fenced sink-only events deactivate should settle within the same control frame",
        );

        assert!(
            app.control_initialized(),
            "retryable stale drained/fenced sink-only cutover should keep runtime initialized instead of fail-closing",
        );

        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn current_owner_source_only_roots_deactivate_fail_closes_within_same_control_frame_after_generation_two_source_activate()
     {
        struct SourceControlErrorHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
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

        let source_roots_route = (
            execution_units::SOURCE_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        );

        app.on_control_frame(&[
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                source_roots_route.1.clone(),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("test-root", &["single-app-node::root-1"])],
                2,
            ),
        ])
        .await
        .expect(
            "generation-two source activate should succeed before the source-only cutover seam",
        );
        assert!(
            app.control_initialized(),
            "generation-two source activate should leave runtime initialized before the fail-closed source-only lane"
        );

        let _reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated fail-closed source-only roots deactivate failure".to_string(),
                ),
            },
        );

        app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            source_roots_route.1.clone(),
            3,
            "restart_deferred_retire_pending",
            7,
            11,
            22,
        )])
        .await
        .expect(
            "current-owner source-only roots deactivate should settle within the same control frame by fail-closing into uninitialized state even when source.apply reports an error after generation-two source activate",
        );

        assert!(
            !app.control_initialized(),
            "same-frame fail-closed source-only recovery may leave runtime uninitialized for later authoritative source replay",
        );
        match app
            .retained_source_control_state
            .lock()
            .await
            .active_by_route
            .get(&source_roots_route)
        {
            Some(SourceControlSignal::Deactivate { generation, .. }) => assert_eq!(
                *generation, 3,
                "same-frame source-only fail-close must retain the incoming deactivate generation",
            ),
            other => panic!(
                "expected retained source deactivate for {source_roots_route:?} after same-frame source-only fail-close: {other:?}"
            ),
        }

        app.close().await.expect("close app");
    }

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/fail_closed_sink_recovery_reactivation.rs"
    ));

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn first_uninitialized_materialized_find_node_a_deactivate_stays_cleanup_only_after_fail_closed_cutover_source_failure_and_proxy_cleanup()
     {
        struct SinkWorkerControlFramePauseHookReset;
        struct SourceControlErrorHookReset;

        impl Drop for SinkWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
            }
        }

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
        let worker_socket_root = worker_role_socket_dir(worker_socket_tempdir().path(), "tmp");
        let source_socket_dir = worker_role_socket_dir(&worker_socket_root, "source");
        let sink_socket_dir = worker_role_socket_dir(&worker_socket_root, "sink");

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

        predecessor
            .on_control_frame(&sink_wave(2))
            .await
            .expect("initial predecessor sink wave should succeed");

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        initial.extend(facade_wave(2));
        successor
            .on_control_frame(&initial)
            .await
            .expect("initial successor exact-shaped full wave should succeed");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let fail_closed_events = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[activate_envelope_with_route_key_and_scope_rows(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        format!("{}.stream", ROUTE_KEY_EVENTS),
                        &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                        3,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("current-owner sink-only fs-meta.events lane should reach sink.apply");
        successor
            .sink
            .close()
            .await
            .expect("close live successor sink worker bridge mid-handoff");
        release.notify_waiters();

        fail_closed_events
            .await
            .expect("join fail-closed events lane")
            .expect(
                "current-owner sink-only fs-meta.events lane should settle within the same control frame after fail-closed cutover",
            );
        assert!(
            !successor.control_initialized(),
            "fail-closed current-owner sink-only events lane must leave runtime uninitialized",
        );

        let _source_error_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated source apply failure after fail-closed sink cutover".to_string(),
                ),
            },
        );

        let mut followup = source_wave(4);
        followup.extend(sink_wave(4));
        followup.extend(facade_wave(4));
        successor
            .on_control_frame(&followup)
            .await
            .expect_err("immediate exact-shaped full wave should fail in source.apply");

        successor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                3,
            )])
            .await
            .expect("cleanup-only materialized-find-proxy deactivate should complete");
        assert!(
            !successor.control_initialized(),
            "cleanup-only proxy deactivate must keep runtime uninitialized",
        );

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let node_a_deactivate = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[deactivate_envelope_with_route_key(
                        execution_units::SINK_RUNTIME_UNIT_ID,
                        sink_query_request_route_for("node-a").0,
                        3,
                    )])
                    .await
            }
        });

        tokio::time::timeout(Duration::from_millis(600), entered.notified())
            .await
            .expect_err(
                "first uninitialized materialized-find.node_a deactivate should stay cleanup-only and must not re-enter sink worker after the preserved fail-closed/source-failure/proxy-cleanup precondition",
            );
        release.notify_waiters();
        node_a_deactivate
            .await
            .expect("join node-a deactivate")
            .expect("node-a deactivate should complete as cleanup-only without sink worker entry");

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/successor_late_predecessor_sink_events.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/peer_sink_second_wave_status_recovery.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/peer_source_second_wave_status.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/stale_predecessor_facade_only_recovery.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/source_wave_and_query_peer_route_deactivates.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/query_peer_route_mutation_during_source_apply.rs"
    ));

    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app/tests/roots_put/fixed_bind_pending_facade.rs"
    ));
