macro_rules! define_generation_one_local_apply_recovery_tests {
    () => {

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_one_second_local_apply_wave_bridge_stopped_settles_within_on_control_frame_total_timeout()
    {
        use std::collections::VecDeque;

        struct SourceWorkerControlFrameErrorQueueHookReset;

        impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_control_frame_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a-local-second-wave-bridge-stop-budget".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                    worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(1))
        .collect::<Vec<_>>();

        client
            .on_control_frame(initial_control)
            .await
            .expect("first generation-one source control wave should succeed");

        let _reset = SourceWorkerControlFrameErrorQueueHookReset;
        install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
            errs: std::iter::repeat_with(|| {
                CnxError::PeerError("transport closed: sidecar control bridge stopped".into())
            })
            .take(64)
            .collect::<VecDeque<_>>(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        });

        let started = std::time::Instant::now();
        let err = client
            .on_control_frame_with_timeouts_for_tests(
                source_wave(1),
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "second generation-one local apply wave should return control to the caller once repeated bridge-stopped resets prove this lane is not making progress",
            );

        assert!(
            !matches!(err, CnxError::Timeout),
            "second generation-one local apply wave should return the bridge reset error instead of exhausting the whole local timeout budget: {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "second generation-one local apply wave should fail fast instead of retrying bridge-stopped through the full local timeout budget: elapsed={:?} err={err:?}",
            started.elapsed()
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_one_local_source_route_post_ack_schedule_refresh_unexpected_correlation_then_bridge_stopped_respects_on_control_frame_total_timeout()
    {
        use std::collections::VecDeque;

        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a-local-post-ack-refresh-budget".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                    worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(1))
        .collect::<Vec<_>>();

        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::once(CnxError::ProtocolViolation(
                    "bound route protocol violation: unexpected correlation_id in reply batch (5)"
                        .to_string(),
                ))
                .chain(std::iter::repeat_with(|| {
                    CnxError::PeerError("transport closed: sidecar control bridge stopped".into())
                }))
                .take(513)
                .collect::<VecDeque<_>>(),
                sticky_worker_instance_id: None,
                sticky_peer_err: None,
            },
        );

        let started = std::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(1),
            client.on_control_frame_with_timeouts_for_tests(
                initial_control,
                Duration::from_millis(250),
                Duration::from_millis(50),
            ),
        )
        .await
        .expect(
            "generation-one local source route must settle within the caller-owned on_control_frame total timeout after post-ack refresh poisons the worker and later bridge-stopped starts keep resetting",
        )
        .expect(
            "generation-one local source route should fail-close its post-ack refresh to the primed schedule and return control to the caller once the caller-owned timeout is exhausted",
        );

        assert!(
            started.elapsed() < Duration::from_millis(800),
            "generation-one local source route should return within the bounded on_control_frame total timeout instead of letting post-ack refresh own a longer nested budget: elapsed={:?}",
            started.elapsed()
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_one_local_source_route_replay_required_refresh_timeout_after_recovered_worker_settles_within_on_control_frame_total_timeout()
    {
        use std::collections::VecDeque;

        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: vec![
                worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a-local-replay-required-refresh-budget".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let source_wave = |generation| {
            vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source roots activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan-control activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source rescan activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        let initial_control = std::iter::once(
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                    worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
                ],
            })
            .expect("encode runtime host grants changed"),
        )
        .chain(source_wave(1))
        .collect::<Vec<_>>();

        client
            .on_control_frame(initial_control)
            .await
            .expect("baseline generation-one local source control wave should succeed before exercising replay-required recovery");

        let control = source_wave(2);

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(513)
                    .collect::<VecDeque<_>>(),
                sticky_worker_instance_id: None,
                sticky_peer_err: None,
            },
        );

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let started = std::time::Instant::now();
        let control_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .on_control_frame_with_timeouts_for_tests(
                        control,
                        Duration::from_millis(600),
                        Duration::from_millis(150),
                    )
                    .await
            }
        });

        entered.notified().await;
        client
            .reconnect_after_retryable_control_reset()
            .await
            .expect("reconnect source worker during caller-owned control path");
        let recovered_instance_id = client.worker_instance_id_for_tests().await;
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(2), control_task)
        .await
        .expect(
            "generation-one local source route must settle within the caller-owned on_control_frame total timeout after a replay-required refresh keeps timing out",
        )
        .expect("join replay-required local apply task")
        .expect(
            "generation-one local source route should fail-close the replay-required refresh to the primed schedule once the caller-owned path has already recovered a live worker",
        );

        assert_ne!(
            recovered_instance_id, previous_instance_id,
            "generation-one local source route should reach a new live worker before the replay-required refresh exhausts",
        );
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "generation-one local source route should return within the bounded on_control_frame total timeout instead of surfacing replay-required refresh exhaustion: elapsed={:?}",
            started.elapsed()
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_one_empty_scope_source_activate_after_recovered_worker_skips_post_ack_schedule_refresh()
    {
        use std::collections::VecDeque;

        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![worker_source_export(
                "node-a::nfs1",
                "node-a",
                "10.0.0.11",
                nfs1.clone(),
            )],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-a-empty-scope-refresh-skip".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );

        tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker start timed out")
            .expect("start source worker");

        let control = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode empty-scope source activate"),
        ];

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let _pause_reset = SourceWorkerControlFramePauseHookReset;
        install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        });

        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect::<VecDeque<_>>(),
                sticky_worker_instance_id: None,
                sticky_peer_err: None,
            },
        );

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let started = std::time::Instant::now();
        let control_task = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .on_control_frame_with_timeouts_for_tests(
                        control,
                        Duration::from_millis(600),
                        Duration::from_millis(150),
                    )
                    .await
            }
        });

        entered.notified().await;
        client
            .reconnect_after_retryable_control_reset()
            .await
            .expect("reconnect source worker during empty-scope caller-owned control path");
        let recovered_instance_id = client.worker_instance_id_for_tests().await;
        release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(2), control_task)
            .await
            .expect(
                "empty-scope source activate must settle within the caller-owned on_control_frame total timeout after a retryable reset",
            )
            .expect("join empty-scope local apply task")
            .expect(
                "empty-scope source activate should skip post-ack schedule refresh after the caller-owned path has already recovered a live worker",
            );

        assert_ne!(
            recovered_instance_id, previous_instance_id,
            "empty-scope source activate should recover a new worker inside the caller-owned control path",
        );
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "empty-scope source activate should return within the caller-owned budget instead of waiting on scheduled-groups refresh: elapsed={:?}",
            started.elapsed()
        );

        client.close().await.expect("close source worker");
    }

    };
}

pub(crate) use define_generation_one_local_apply_recovery_tests;
