    #[tokio::test]
    async fn peer_sink_status_fails_closed_while_successor_second_exact_shaped_wave_sink_apply_is_pending()
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

        let _err_reset = SinkControlErrorHookReset;
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

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_wave = tokio::spawn({
            let successor = successor.clone();
            async move {
                let mut followup = source_wave(3);
                followup.extend(sink_wave(3));
                followup.extend(facade_wave(3));
                successor.on_control_frame(&followup).await
            }
        });

        entered.notified().await;

        let request_result = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-d".into()),
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .await;
        assert!(
            request_result.is_err(),
            "peer-facing sink-status must fail closed while the successor exact-shaped second wave is still paused in sink.apply"
        );

        release.notify_waiters();

        successor_wave
            .await
            .expect("join successor exact-shaped second wave")
            .expect("successor exact-shaped second wave after paused sink.apply");

        let sink_status_events = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-d".into()),
            Duration::from_secs(2),
            Duration::from_millis(100),
        )
        .await
        .expect("peer-facing sink-status after successor exact-shaped second wave");
        let sink_status_snapshots = sink_status_events
            .into_iter()
            .map(|event| {
                rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                    .expect("decode peer sink-status snapshot")
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
            "peer-facing sink-status must republish ready groups after the successor exact-shaped second wave completes: {sink_status_snapshots:?}"
        );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn peer_sink_payload_remains_materialized_after_successor_second_exact_shaped_wave_sink_apply_recovers()
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

        successor
            .sink
            .send(&[
                mk_source_event("node-a::nfs1", b"/ready-a.txt", b"ready-a.txt", 10),
                mk_source_event("node-a::nfs2", b"/ready-b.txt", b"ready-b.txt", 11),
                mk_control_event(
                    "node-a::nfs1",
                    ControlEvent::EpochEnd {
                        epoch_id: 0,
                        epoch_type: crate::EpochType::Audit,
                    },
                    12,
                ),
                mk_control_event(
                    "node-a::nfs2",
                    ControlEvent::EpochEnd {
                        epoch_id: 0,
                        epoch_type: crate::EpochType::Audit,
                    },
                    13,
                ),
            ])
            .await
            .expect("seed successor sink state before second wave");

        for (group_id, path) in [
            ("nfs1", b"/ready-a.txt".as_slice()),
            ("nfs2", b"/ready-b.txt".as_slice()),
        ] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let trusted_request = query::InternalQueryRequest::materialized(
                    query::QueryOp::Tree,
                    query::QueryScope {
                        path: b"/".to_vec(),
                        recursive: true,
                        max_depth: None,
                        selected_group: Some(group_id.to_string()),
                    },
                    Some(query::TreeQueryOptions {
                        read_class: query::ReadClass::TrustedMaterialized,
                    }),
                );
                if let Ok(grouped) = selected_group_proxy_tree(
                    boundary.clone(),
                    NodeId("node-a".into()),
                    &trusted_request,
                )
                .await
                    && grouped.get(group_id).is_some_and(|payload| {
                        payload.entries.iter().any(|entry| entry.path == path)
                    })
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for seeded trusted payload before second wave for {group_id}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        let _err_reset = SinkControlErrorHookReset;
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

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SinkWorkerControlFramePauseHookReset;
        crate::workers::sink::install_sink_worker_control_frame_pause_hook(
            crate::workers::sink::SinkWorkerControlFramePauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let successor_wave = tokio::spawn({
            let successor = successor.clone();
            async move {
                let mut followup = source_wave(3);
                followup.extend(sink_wave(3));
                followup.extend(facade_wave(3));
                successor.on_control_frame(&followup).await
            }
        });

        entered.notified().await;
        let _ = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-d".into()),
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .await;
        release.notify_waiters();

        successor_wave
            .await
            .expect("join successor exact-shaped second wave")
            .expect("successor exact-shaped second wave after paused sink.apply");

        for (group_id, path) in [
            ("nfs1", b"/ready-a.txt".as_slice()),
            ("nfs2", b"/ready-b.txt".as_slice()),
        ] {
            let trusted_request = query::InternalQueryRequest::materialized(
                query::QueryOp::Tree,
                query::QueryScope {
                    path: b"/".to_vec(),
                    recursive: true,
                    max_depth: None,
                    selected_group: Some(group_id.to_string()),
                },
                Some(query::TreeQueryOptions {
                    read_class: query::ReadClass::TrustedMaterialized,
                }),
            );
            let grouped = selected_group_proxy_tree(
                boundary.clone(),
                NodeId("node-a".into()),
                &trusted_request,
            )
            .await
            .expect(
                "owner-local trusted selected-group tree after successor exact-shaped second wave",
            );
            let payload = grouped
                .get(group_id)
                .expect("selected-group payload after successor exact-shaped second wave");
            assert!(
                payload.entries.iter().any(|entry| entry.path == path),
                "successor exact-shaped second wave must keep owner-local trusted selected-group payload for {group_id}: {payload:?}"
            );
        }

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

