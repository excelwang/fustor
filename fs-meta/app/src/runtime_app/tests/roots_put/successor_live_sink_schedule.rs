    #[tokio::test]
    async fn successor_live_sink_schedule_survives_late_predecessor_sink_events_deactivate_reset() {
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

        let expected_groups = std::collections::BTreeSet::from(["test-root".to_string()]);
        let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let groups = successor
                .sink
                .scheduled_group_ids()
                .await
                .expect("initial successor sink groups")
                .unwrap_or_default();
            if groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < initial_deadline,
                "timed out waiting for successor sink schedule before predecessor late deactivate: groups={groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

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

        let remaining_groups = successor
            .sink
            .scheduled_group_ids()
            .await
            .expect("successor sink groups after predecessor late deactivate")
            .unwrap_or_default();
        assert_eq!(
            remaining_groups, expected_groups,
            "late predecessor sink events deactivate must not clear already-live successor sink scheduling before successor sends another control wave"
        );

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }
