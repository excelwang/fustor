    #[tokio::test]
    async fn stale_predecessor_query_peer_deactivate_waits_before_route_mutation_during_successor_second_wave_source_apply()
     {
        struct SourceControlErrorHookReset;
        struct SourceWorkerControlFramePauseHookReset;
        struct FacadeDeactivatePauseHookReset {
            unit_id: String,
            route_key: String,
        }

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

        impl Drop for FacadeDeactivatePauseHookReset {
            fn drop(&mut self) {
                clear_facade_deactivate_pause_hook(&self.unit_id, &self.route_key);
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
            "predecessor should be uninitialized before stale query-peer deactivate begins"
        );

        predecessor_source_client
            .shutdown_shared_worker_for_tests()
            .await
            .expect("shutdown predecessor source worker before stale query-peer deactivate");

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

        let deactivate_entered = Arc::new(Notify::new());
        let deactivate_release = Arc::new(Notify::new());
        let route_key = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
        let _deactivate_pause_reset = FacadeDeactivatePauseHookReset {
            unit_id: execution_units::QUERY_PEER_RUNTIME_UNIT_ID.to_string(),
            route_key: route_key.clone(),
        };
        install_facade_deactivate_pause_hook(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            &route_key,
            FacadeDeactivatePauseHook {
                entered: deactivate_entered.clone(),
                release: deactivate_release.clone(),
            },
        );

        let predecessor_deactivate = tokio::spawn({
            let predecessor = predecessor.clone();
            let route_key = route_key.clone();
            async move {
                predecessor
                    .on_control_frame(&[deactivate_envelope_with_route_key(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        route_key,
                        3,
                    )])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(300), deactivate_entered.notified())
                .await
                .is_err(),
            "stale predecessor query-peer deactivate must not reach route-state mutation while the successor second exact-shaped full wave is still paused in source.apply",
        );

        source_release.notify_waiters();

        tokio::time::timeout(Duration::from_millis(600), deactivate_entered.notified())
            .await
            .expect("stale predecessor query-peer deactivate should reach route-state mutation after successor source.apply releases");
        deactivate_release.notify_waiters();

        predecessor_deactivate
            .await
            .expect("join predecessor stale query-peer deactivate")
            .expect("predecessor stale query-peer deactivate should finish");
        successor_followup
            .await
            .expect("join successor second exact-shaped followup")
            .expect(
                "successor second exact-shaped full wave should survive the stale predecessor query-peer deactivate overlap",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn peer_source_owner_query_peer_sink_deactivate_waits_before_route_mutation_during_remote_sink_owner_exact_wave_source_apply()
     {
        struct SourceWorkerControlFramePauseHookReset;

        impl Drop for SourceWorkerControlFramePauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_pause_hook();
            }
        }

        struct FacadeDeactivatePauseHookReset {
            unit_id: String,
            route_key: String,
        }

        impl Drop for FacadeDeactivatePauseHookReset {
            fn drop(&mut self) {
                clear_facade_deactivate_pause_hook(&self.unit_id, &self.route_key);
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let node_a_nfs1 = tmp.path().join("node-a-nfs1");
        let node_a_nfs2 = tmp.path().join("node-a-nfs2");
        let node_b_nfs1 = tmp.path().join("node-b-nfs1");
        let node_b_nfs2 = tmp.path().join("node-b-nfs2");
        fs::create_dir_all(&node_a_nfs1).expect("create node-a nfs1 dir");
        fs::create_dir_all(&node_a_nfs2).expect("create node-a nfs2 dir");
        fs::create_dir_all(&node_b_nfs1).expect("create node-b nfs1 dir");
        fs::create_dir_all(&node_b_nfs2).expect("create node-b nfs2 dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_root = worker_socket_tempdir();
        let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
        let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
        fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

        let make_node_a = || {
            Arc::new(
                FSMetaApp::with_boundaries_and_state(
                    FSMetaConfig {
                        source: SourceConfig {
                            roots: vec![
                                worker_fs_watch_scan_root(
                                    "nfs1",
                                    &node_a_nfs1.display().to_string(),
                                ),
                                worker_fs_watch_scan_root(
                                    "nfs2",
                                    &node_a_nfs2.display().to_string(),
                                ),
                            ],
                            host_object_grants: vec![
                                worker_export_with_fs_source(
                                    "node-a::nfs1",
                                    "node-a",
                                    "10.0.0.11",
                                    &node_a_nfs1.display().to_string(),
                                    node_a_nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-a::nfs2",
                                    "node-a",
                                    "10.0.0.12",
                                    &node_a_nfs2.display().to_string(),
                                    node_a_nfs2.clone(),
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
                .expect("init node-a app"),
            )
        };

        let make_node_b = || {
            Arc::new(
                FSMetaApp::with_boundaries_and_state(
                    FSMetaConfig {
                        source: SourceConfig {
                            roots: vec![
                                worker_fs_watch_scan_root(
                                    "nfs1",
                                    &node_b_nfs1.display().to_string(),
                                ),
                                worker_fs_watch_scan_root(
                                    "nfs2",
                                    &node_b_nfs2.display().to_string(),
                                ),
                            ],
                            host_object_grants: vec![
                                worker_export_with_fs_source(
                                    "node-b::nfs1",
                                    "node-b",
                                    "10.0.0.21",
                                    &node_b_nfs1.display().to_string(),
                                    node_b_nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-b::nfs2",
                                    "node-b",
                                    "10.0.0.22",
                                    &node_b_nfs2.display().to_string(),
                                    node_b_nfs2.clone(),
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
                    NodeId("node-b".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init node-b app"),
            )
        };

        let node_a = make_node_a();
        let node_b = make_node_b();

        if cfg!(target_os = "linux") {
            match node_a.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start node-a app: {err}"),
            }
        } else {
            let err = node_a
                .start()
                .await
                .expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        let node_a_source_wave = |generation| {
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
        let node_a_sink_wave = |generation| {
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
        let node_a_facade_wave = |generation| {
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
        let node_b_initial = vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
                2,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
                2,
            ),
        ];

        let mut node_a_initial = node_a_source_wave(2);
        node_a_initial.extend(node_a_sink_wave(2));
        node_a_initial.extend(node_a_facade_wave(2));
        node_a
            .on_control_frame(&node_a_initial)
            .await
            .expect("node-a initial exact-shaped full wave should succeed");
        node_b
            .on_control_frame(&node_b_initial)
            .await
            .expect("node-b initial peer source-owner wave should succeed");

        let source_entered = Arc::new(Notify::new());
        let source_release = Arc::new(Notify::new());
        let _source_pause_reset = SourceWorkerControlFramePauseHookReset;
        crate::workers::source::install_source_worker_control_frame_pause_hook(
            crate::workers::source::SourceWorkerControlFramePauseHook {
                entered: source_entered.clone(),
                release: source_release.clone(),
            },
        );

        let node_a_followup = tokio::spawn({
            let node_a = node_a.clone();
            async move {
                let mut followup = node_a_source_wave(3);
                followup.extend(node_a_sink_wave(3));
                followup.extend(node_a_facade_wave(3));
                node_a.on_control_frame(&followup).await
            }
        });

        source_entered.notified().await;

        let deactivate_entered = Arc::new(Notify::new());
        let deactivate_release = Arc::new(Notify::new());
        let route_key = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
        let _deactivate_pause_reset = FacadeDeactivatePauseHookReset {
            unit_id: execution_units::QUERY_PEER_RUNTIME_UNIT_ID.to_string(),
            route_key: route_key.clone(),
        };
        install_facade_deactivate_pause_hook(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            &route_key,
            FacadeDeactivatePauseHook {
                entered: deactivate_entered.clone(),
                release: deactivate_release.clone(),
            },
        );

        let node_b_deactivate = tokio::spawn({
            let node_b = node_b.clone();
            async move {
                node_b
                    .on_control_frame(&[deactivate_envelope_with_route_key(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                        3,
                    )])
                    .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(300), deactivate_entered.notified())
                .await
                .is_err(),
            "peer source-owner query-peer sink-route deactivate must not reach route-state mutation while the remote sink-owner exact-shaped wave is still paused in source.apply",
        );

        source_release.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_millis(600), deactivate_entered.notified())
            .await
            .expect("peer source-owner query-peer sink-route deactivate should reach route-state mutation after remote sink-owner source.apply releases");
        deactivate_release.notify_waiters();

        node_b_deactivate
            .await
            .expect("join node-b deactivate")
            .expect("node-b deactivate should finish");
        node_a_followup
            .await
            .expect("join node-a followup")
            .expect("node-a followup exact-shaped wave should finish");

        node_b.close().await.expect("close node-b app");
        node_a.close().await.expect("close node-a app");
    }

