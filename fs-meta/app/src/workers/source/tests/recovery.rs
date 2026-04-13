macro_rules! define_recovery_tests {
    () => {
    async fn assert_on_control_frame_retries_bridge_reset_peer_error(err: CnxError, label: &str) {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

        let cfg = SourceConfig {
            roots: vec![worker_source_root("nfs1", &nfs1)],
            host_object_grants: vec![
                worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
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
                NodeId("node-d".to_string()),
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

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect("first source control wave should succeed");

        let _reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook { err });

        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode second-wave source activate"),
            ])
            .await
            .unwrap_or_else(|err| panic!("{label}: {err}"));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            if source_groups
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: scheduled source groups should remain converged after retry: source={source_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_connection_reset_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                    .to_string(),
            ),
            "on_control_frame should retry a peer connection-reset bridge error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_early_eof_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
            "on_control_frame should retry a peer early-eof bridge error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_peer_bridge_stopped_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
            "on_control_frame should retry a peer bridge-stopped error and reach the live worker",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn on_control_frame_retries_timeout_errors_after_first_wave_succeeded() {
        assert_on_control_frame_retries_bridge_reset_peer_error(
            CnxError::Timeout,
            "on_control_frame should retry a timeout error and reach the live worker",
        )
        .await;
    }
    };
}

pub(crate) use define_recovery_tests;
