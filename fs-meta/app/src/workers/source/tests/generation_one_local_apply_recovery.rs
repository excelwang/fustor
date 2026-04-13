macro_rules! define_generation_one_local_apply_recovery_tests {
    () => {

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

    };
}

pub(crate) use define_generation_one_local_apply_recovery_tests;
