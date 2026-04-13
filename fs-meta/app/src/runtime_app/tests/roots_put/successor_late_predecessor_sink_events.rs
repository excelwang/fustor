    #[tokio::test]
    async fn successor_full_control_survives_late_predecessor_sink_events_deactivate_reset() {
        struct SinkControlErrorHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
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
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
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
            .expect("initial successor full source/sink/internal-status wave should succeed");

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::AccessDenied(
                    "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                3,
            )])
            .await
            .expect("late predecessor sink events deactivate should recover");

        successor
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    3,
                ),
                activate_envelope_with_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    3,
                ),
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("test-root", &["single-app-node::root-1"])],
                    3,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("test-root", &["single-app-node::root-1"])],
                    3,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    3,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    3,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    3,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    3,
                ),
            ])
            .await
            .expect(
                "successor full source/sink/internal-status wave must recover after late predecessor sink events deactivate reset on the shared worker",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_exact_shaped_sink_nine_wave_survives_late_predecessor_sink_events_deactivate()
     {
        struct SinkControlErrorHookReset;

        impl Drop for SinkControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::sink::clear_sink_worker_control_frame_error_hook();
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
            .on_control_frame(&[
                activate_envelope_with_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    2,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SINK_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_EVENTS),
                    &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                    2,
                ),
            ])
            .await
            .expect("initial predecessor sink events wave should succeed");

        let mut initial = source_wave(2);
        initial.extend(sink_wave(2));
        initial.extend(facade_wave(2));
        successor
            .on_control_frame(&initial)
            .await
            .expect("initial successor exact-shaped full wave should succeed");

        let _reset = SinkControlErrorHookReset;
        crate::workers::sink::install_sink_worker_control_frame_error_hook(
            crate::workers::sink::SinkWorkerControlFrameErrorHook {
                err: CnxError::AccessDenied(
                    "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                3,
            )])
            .await
            .expect(
                "late predecessor sink events deactivate should already recover when a successor exact-shaped wave owns the shared sink route",
            );
        assert!(
            predecessor.control_initialized(),
            "predecessor should remain initialized when the successor already owns the exact-shaped shared sink route set"
        );

        let mut followup = source_wave(3);
        followup.extend(sink_wave(3));
        followup.extend(facade_wave(3));
        successor
            .on_control_frame(&followup)
            .await
            .expect(
                "successor exact-shaped full wave must survive a late predecessor sink events deactivate on the shared worker",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

