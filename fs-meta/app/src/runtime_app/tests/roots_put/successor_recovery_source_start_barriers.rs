    #[tokio::test]
    async fn successor_recovery_waits_for_predecessor_source_start_across_runtime_instances_with_distinct_worker_bindings()
     {
        struct SourceControlErrorHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
            }
        }

        struct SourceWorkerStartPauseHookReset;

        impl Drop for SourceWorkerStartPauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_start_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();

        let make_app = |source_socket_dir: &Path, sink_socket_dir: &Path| {
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
                    external_runtime_worker_binding("source", source_socket_dir),
                    external_runtime_worker_binding("sink", sink_socket_dir),
                    NodeId("single-app-node".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor_socket_root = tempdir().expect("create predecessor worker socket dir");
        let predecessor_source_socket_dir =
            worker_role_socket_dir(predecessor_socket_root.path(), "source");
        let predecessor_sink_socket_dir =
            worker_role_socket_dir(predecessor_socket_root.path(), "sink");
        fs::create_dir_all(&predecessor_source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&predecessor_sink_socket_dir).expect("create sink socket dir");
        let predecessor = make_app(&predecessor_source_socket_dir, &predecessor_sink_socket_dir);

        predecessor
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
            .expect("predecessor initial source control should succeed");

        let _err_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated predecessor source-only failure".to_string(),
                ),
            },
        );
        let err = predecessor
            .on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect_err("predecessor follow-up source-only control should fail");
        assert!(
            err.to_string()
                .contains("simulated predecessor source-only failure"),
            "unexpected predecessor source-only failure: {err}"
        );
        assert!(
            !predecessor.control_initialized(),
            "predecessor failure should leave the runtime uninitialized before recovery"
        );

        let successor_socket_root = tempdir().expect("create successor worker socket dir");
        let successor_source_socket_dir =
            worker_role_socket_dir(successor_socket_root.path(), "source");
        let successor_sink_socket_dir =
            worker_role_socket_dir(successor_socket_root.path(), "sink");
        fs::create_dir_all(&successor_source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&successor_sink_socket_dir).expect("create sink socket dir");
        let successor = make_app(&successor_source_socket_dir, &successor_sink_socket_dir);

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let _pause_reset = SourceWorkerStartPauseHookReset;
        crate::workers::source::install_source_worker_start_pause_hook(
            crate::workers::source::SourceWorkerStartPauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let predecessor_recovery = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                    ])
                    .await
            }
        });

        entered.notified().await;

        let successor_recovery = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                    ])
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !successor_recovery.is_finished(),
            "successor recovery on the same node must wait while predecessor recovery is still paused in source.start, even when the two runtime instances use distinct worker bindings"
        );

        release.notify_waiters();

        predecessor_recovery
            .await
            .expect("join predecessor recovery task")
            .expect("predecessor recovery should succeed");
        successor_recovery
            .await
            .expect("join successor recovery task")
            .expect("successor recovery should succeed");

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

    #[tokio::test]
    async fn successor_recovery_waits_for_predecessor_source_start_across_runtime_instances_with_distinct_worker_module_paths()
     {
        struct SourceControlErrorHookReset;

        impl Drop for SourceControlErrorHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_control_frame_error_hook();
            }
        }

        struct SourceWorkerStartPauseHookReset;

        impl Drop for SourceWorkerStartPauseHookReset {
            fn drop(&mut self) {
                crate::workers::source::clear_source_worker_start_pause_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let fs_source = tmp.path().display().to_string();
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_module_path = runtime_test_worker_module_path();
        let worker_module_link = tmp.path().join("worker-module-link");
        #[cfg(target_family = "unix")]
        std::os::unix::fs::symlink(&worker_module_path, &worker_module_link)
            .expect("symlink worker module path");

        let make_app = |source_socket_dir: &Path,
                        sink_socket_dir: &Path,
                        source_module_path: &Path,
                        sink_module_path: &Path| {
            let mut source_binding = external_runtime_worker_binding("source", source_socket_dir);
            source_binding.module_path = Some(source_module_path.to_path_buf());
            let mut sink_binding = external_runtime_worker_binding("sink", sink_socket_dir);
            sink_binding.module_path = Some(sink_module_path.to_path_buf());
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
                    source_binding,
                    sink_binding,
                    NodeId("single-app-node".into()),
                    Some(boundary.clone()),
                    Some(boundary.clone()),
                    state_boundary.clone(),
                )
                .expect("init external-worker app"),
            )
        };

        let predecessor_socket_root = tempdir().expect("create predecessor worker socket dir");
        let predecessor_source_socket_dir =
            worker_role_socket_dir(predecessor_socket_root.path(), "source");
        let predecessor_sink_socket_dir =
            worker_role_socket_dir(predecessor_socket_root.path(), "sink");
        fs::create_dir_all(&predecessor_source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&predecessor_sink_socket_dir).expect("create sink socket dir");
        let predecessor = make_app(
            &predecessor_source_socket_dir,
            &predecessor_sink_socket_dir,
            &worker_module_path,
            &worker_module_path,
        );

        predecessor
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
            .expect("predecessor initial source control should succeed");

        let _err_reset = SourceControlErrorHookReset;
        crate::workers::source::install_source_worker_control_frame_error_hook(
            crate::workers::source::SourceWorkerControlFrameErrorHook {
                err: CnxError::ProtocolViolation(
                    "simulated predecessor source-only failure".to_string(),
                ),
            },
        );
        predecessor
            .on_control_frame(&[activate_envelope("runtime.exec.source")])
            .await
            .expect_err("predecessor follow-up source-only control should fail");

        let successor_socket_root = tempdir().expect("create successor worker socket dir");
        let successor_source_socket_dir =
            worker_role_socket_dir(successor_socket_root.path(), "source");
        let successor_sink_socket_dir =
            worker_role_socket_dir(successor_socket_root.path(), "sink");
        fs::create_dir_all(&successor_source_socket_dir).expect("create source socket dir");
        fs::create_dir_all(&successor_sink_socket_dir).expect("create sink socket dir");
        let successor = make_app(
            &successor_source_socket_dir,
            &successor_sink_socket_dir,
            &worker_module_link,
            &worker_module_link,
        );

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let _pause_reset = SourceWorkerStartPauseHookReset;
        crate::workers::source::install_source_worker_start_pause_hook(
            crate::workers::source::SourceWorkerStartPauseHook {
                entered: entered.clone(),
                release: release.clone(),
            },
        );

        let predecessor_recovery = tokio::spawn({
            let predecessor = predecessor.clone();
            async move {
                predecessor
                    .on_control_frame(&[
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                    ])
                    .await
            }
        });

        entered.notified().await;

        let successor_recovery = tokio::spawn({
            let successor = successor.clone();
            async move {
                successor
                    .on_control_frame(&[
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                        activate_envelope_with_scope_rows(
                            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                            &[("test-root", &["single-app-node::root-1"])],
                            4,
                        ),
                    ])
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !successor_recovery.is_finished(),
            "successor recovery on the same node must wait while predecessor recovery is still paused in source.start, even when the two runtime instances use distinct worker module paths"
        );

        release.notify_waiters();

        predecessor_recovery
            .await
            .expect("join predecessor recovery task")
            .expect("predecessor recovery should succeed");
        successor_recovery
            .await
            .expect("join successor recovery task")
            .expect("successor recovery should succeed");

        successor.close().await.expect("close successor app");
        predecessor.close().await.expect("close predecessor app");
    }

