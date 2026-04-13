    #[tokio::test]
    async fn successor_exact_shaped_source_wave_survives_late_predecessor_source_roots_deactivate()
    {
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
                                    "node-b::nfs1",
                                    "node-b",
                                    "10.0.0.21",
                                    &nfs1_source,
                                    nfs1.clone(),
                                ),
                                worker_export_with_fs_source(
                                    "node-b::nfs2",
                                    "node-b",
                                    "10.0.0.22",
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
                    NodeId("node-b".into()),
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
                    &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                    generation,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    &[("nfs1", &["node-b::nfs1"]), ("nfs2", &["node-b::nfs2"])],
                    generation,
                ),
            ]
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
            .on_control_frame(&source_wave(2))
            .await
            .expect("initial predecessor source wave should succeed");

        let mut initial = source_wave(2);
        initial.extend(facade_wave(2));
        successor
            .on_control_frame(&initial)
            .await
            .expect("initial successor exact-shaped source wave should succeed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = successor
                .source
                .scheduled_source_group_ids()
                .await
                .expect("successor source groups")
                .unwrap_or_default();
            let scan_groups = successor
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("successor scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for successor source groups before predecessor late deactivate: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        predecessor
            .on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                3,
            )])
            .await
            .expect("late predecessor source-roots deactivate should complete");

        let remaining_source_groups = successor
            .source
            .scheduled_source_group_ids()
            .await
            .expect("successor source groups after predecessor late deactivate")
            .unwrap_or_default();
        let remaining_scan_groups = successor
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("successor scan groups after predecessor late deactivate")
            .unwrap_or_default();
        assert_eq!(
            remaining_source_groups, expected_groups,
            "late predecessor source-roots deactivate must not clear already-live successor source scheduling before successor sends another control wave"
        );
        assert_eq!(
            remaining_scan_groups, expected_groups,
            "late predecessor source-roots deactivate must not clear already-live successor scan scheduling before successor sends another control wave"
        );

        let mut followup = source_wave(3);
        followup.extend(facade_wave(3));
        successor
            .on_control_frame(&followup)
            .await
            .expect(
                "successor exact-shaped source wave must survive late predecessor source-roots deactivate on the shared worker",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_second_source_control_survives_late_predecessor_query_peer_sink_route_deactivates()
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
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
            ])
            .await
            .expect("activate predecessor query-peer sink routes");

        successor
            .on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect("initial successor source-only control should succeed");

        predecessor
            .on_control_frame(&[
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    2,
                ),
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    2,
                ),
            ])
            .await
            .expect("late predecessor query-peer sink route deactivates should complete");

        successor
            .on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect(
                "successor second source-only control must survive late predecessor query-peer sink route deactivates after the first batch already succeeded",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_second_source_control_survives_late_predecessor_all_query_peer_route_deactivates()
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

        predecessor
            .on_control_frame(&[
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
                activate_envelope_with_route_key_and_scope_rows(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    &[("test-root", &["listener-a"])],
                    1,
                ),
            ])
            .await
            .expect("activate predecessor all query-peer routes");

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
            ])
            .await
            .expect("initial successor source/source-scan control should succeed");

        predecessor
            .on_control_frame(&[
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                    2,
                ),
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                    2,
                ),
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                    2,
                ),
                deactivate_envelope_with_route_key(
                    execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                    2,
                ),
            ])
            .await
            .expect("late predecessor all query-peer route deactivates should complete");

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
            ])
            .await
            .expect(
                "successor second source-only control batch must survive late predecessor all query-peer route deactivates after the first batch already succeeded",
            );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

