macro_rules! define_trigger_rescan_republish_tests {
    () => {
    async fn generation_two_real_source_route_trigger_rescan_when_ready_republishes_baseline_after_worker_not_initialized_post_ack_schedule_refresh()
    {
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

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
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

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
                .expect("encode source roots-control activate"),
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
            .expect("initial real source route wave should succeed");

        let baseline_target = b"/data";
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }
        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
            "baseline /data should publish for both roots before generation-two recovery: {baseline_counts:?}"
        );
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some("worker not initialized".to_string()),
            },
        );

        tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh still sees worker not initialized",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after worker-not-initialized",
            );

        let next_instance_id = client.worker_instance_id_for_tests().await;
        assert_ne!(
            next_instance_id, previous_instance_id,
            "worker-not-initialized post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
        );

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "scheduled groups should remain converged after worker-not-initialized refresh recovery: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("trigger_rescan_when_ready after worker-not-initialized recovery");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut republish_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < republish_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_path_data_counts(&mut republish_counts, &batch, baseline_target)
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("post-recovery trigger_rescan publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| republish_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }

        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| republish_counts.get(*origin).copied().unwrap_or(0) > 0),
            "trigger_rescan_when_ready after worker-not-initialized recovery must republish baseline /data for both roots instead of leaving post-recovery publishes at zero: {republish_counts:?}"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_real_source_route_trigger_rescan_when_ready_republishes_baseline_after_unexpected_correlation_post_ack_schedule_refresh()
    {
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

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
        let client = SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

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
                .expect("encode source roots-control activate"),
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
            .expect("initial real source route wave should succeed");

        let baseline_target = b"/data";
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }
        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
            "baseline /data should publish for both roots before generation-two recovery: {baseline_counts:?}"
        );
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "bound route protocol violation: unexpected correlation_id in reply batch (5)"
                        .to_string(),
                ),
            },
        );

        tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh hits a transient bound-route correlation mismatch",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after a correlation mismatch",
            );

        let next_instance_id = client.worker_instance_id_for_tests().await;
        assert_ne!(
            next_instance_id, previous_instance_id,
            "unexpected-correlation post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
        );

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "scheduled groups should remain converged after unexpected-correlation refresh recovery: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("trigger_rescan_when_ready after unexpected-correlation recovery");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut republish_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < republish_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => {
                    record_path_data_counts(&mut republish_counts, &batch, baseline_target)
                }
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("post-recovery trigger_rescan publish recv failed: {err}"),
            }
            if ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| republish_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
        }

        assert!(
            ["node-a::nfs1", "node-a::nfs2"]
                .iter()
                .all(|origin| republish_counts.get(*origin).copied().unwrap_or(0) > 0),
            "trigger_rescan_when_ready after unexpected-correlation recovery must republish baseline /data for both roots instead of leaving post-recovery publishes at zero: {republish_counts:?}"
        );

        client.close().await.expect("close source worker");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_grant_watch_scan_external_worker_trigger_rescan_publishes_baseline_before_cleanup_tail()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-zero-grant-watch-scan-baseline".to_string()),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after initial zero-grant wave")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after initial zero-grant wave")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "zero-grant watch-scan wave should converge scheduled groups before trigger_rescan_when_ready: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("initial trigger_rescan_when_ready should succeed");

        let baseline_target = b"/data";
        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before baseline publish wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during baseline publish wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("baseline watch-scan publish recv failed: {err}"),
            }
            let published_batches = latest_snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_data_events = latest_snapshot
                .published_data_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            if baseline_counts.len() >= 2 && published_batches > 0 && published_data_events > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "zero-grant external watch-scan trigger_rescan_when_ready must publish baseline /data for both roots before any cleanup tail: baseline_counts={baseline_counts:?} scheduled_source={:?} scheduled_scan={:?} published_batches={:?} published_events={:?} published_data={:?} last_published={:?} lifecycle={} concrete_roots={:?}",
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot.last_published_at_us_by_node,
                latest_snapshot.lifecycle_state,
                latest_snapshot.status.concrete_roots,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_grant_watch_scan_nonblocking_observability_bypasses_recent_zero_publication_cache_after_trigger_rescan_republishes_baseline()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId("node-c-zero-grant-watch-scan-nonblocking".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after initial zero-grant wave")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after initial zero-grant wave")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "zero-grant watch-scan wave should converge scheduled groups before recent-live nonblocking publication probe: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let primed = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("prime recent live observability cache before trigger_rescan");
        let node_key = "node-c-zero-grant-watch-scan-nonblocking".to_string();
        let mut primed_zero = primed.clone();
        primed_zero.published_batches_by_node =
            std::collections::BTreeMap::from([(node_key.clone(), 0)]);
        primed_zero.published_events_by_node =
            std::collections::BTreeMap::from([(node_key.clone(), 0)]);
        primed_zero.published_control_events_by_node =
            std::collections::BTreeMap::from([(node_key.clone(), 0)]);
        primed_zero.published_data_events_by_node =
            std::collections::BTreeMap::from([(node_key, 0)]);
        primed_zero.last_published_at_us_by_node.clear();
        primed_zero.last_published_origins_by_node.clear();
        primed_zero.published_origin_counts_by_node.clear();
        client.update_cached_observability_snapshot(&primed_zero);
        client.with_cache_mut(|cache| {
            cache.last_live_observability_snapshot_at = Some(Instant::now());
        });

        client
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("trigger_rescan_when_ready should succeed");

        let baseline_target = b"/data";
        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < republish_deadline {
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("baseline watch-scan publish recv failed: {err}"),
            }
            if ["node-c-zero-grant-watch-scan-nonblocking::nfs1", "node-c-zero-grant-watch-scan-nonblocking::nfs2"]
                .iter()
                .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            baseline_counts.len() >= 2,
            "trigger_rescan_when_ready must publish baseline /data for both roots before recent-live nonblocking observability is checked: {baseline_counts:?}"
        );

        let nonblocking = client.observability_snapshot_nonblocking_for_status_route().await.0;
        let live = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("fetch live observability after baseline publication");
        let nonblocking_batches = nonblocking
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let nonblocking_events = nonblocking
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let nonblocking_data = nonblocking
            .published_data_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let live_batches = live.published_batches_by_node.values().copied().sum::<u64>();
        let live_events = live.published_events_by_node.values().copied().sum::<u64>();
        let live_data = live
            .published_data_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        assert!(
            live_batches > 0 && live_events > 0 && live_data > 0,
            "live observability must show baseline publication progressed before checking recent-live nonblocking fallback: live_batches={live_batches} live_events={live_events} live_data={live_data} live={live:?} baseline_counts={baseline_counts:?}"
        );
        assert!(
            nonblocking_batches > 0 && nonblocking_events > 0 && nonblocking_data > 0,
            "nonblocking observability must not keep serving a recent zero-publication cache once trigger_rescan_when_ready has already republished baseline: primed_zero={primed_zero:?} nonblocking={nonblocking:?} live={live:?} baseline_counts={baseline_counts:?}"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_grant_watch_scan_external_worker_populates_concrete_roots_before_trigger_rescan()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-zero-grant-watch-scan-status".to_string()),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let initial_worker_instance_id = client.worker_instance_id_for_tests().await;
        let mut latest_worker_instance_id = initial_worker_instance_id;
        let mut latest_status = client
            .status_snapshot_with_failure()
            .await
            .expect("status snapshot before concrete-root readiness wait");
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after initial zero-grant wave")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after initial zero-grant wave")
                .unwrap_or_default();
            latest_status = client
                .status_snapshot_with_failure()
                .await
                .expect("status snapshot during concrete-root readiness wait");
            latest_worker_instance_id = client.worker_instance_id_for_tests().await;
            let concrete_logical_roots = latest_status
                .concrete_roots
                .iter()
                .map(|root| root.logical_root_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let concrete_object_refs = latest_status
                .concrete_roots
                .iter()
                .map(|root| root.object_ref.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if source_groups == expected_groups
                && scan_groups == expected_groups
                && concrete_logical_roots.contains("nfs1")
                && concrete_logical_roots.contains("nfs2")
                && concrete_object_refs.contains("nfs1")
                && concrete_object_refs.contains("nfs2")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "zero-grant external watch-scan wave must populate concrete roots before trigger_rescan_when_ready: source={source_groups:?} scan={scan_groups:?} concrete_roots={:?} worker_instance_id={} initial_worker_instance_id={}",
                latest_status.concrete_roots,
                latest_worker_instance_id,
                initial_worker_instance_id,
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn zero_grant_watch_scan_raw_worker_client_trigger_rescan_publishes_baseline_before_cleanup_tail()
    {
        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-zero-grant-watch-scan-raw-client".to_string()),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after initial zero-grant wave")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after initial zero-grant wave")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "zero-grant watch-scan wave should converge scheduled groups before raw trigger_rescan_when_ready RPC: source={source_groups:?} scan={scan_groups:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let raw_client = client.client().await.expect("typed source worker client");
        match raw_client
            .call_with_timeout(
                SourceWorkerRequest::TriggerRescanWhenReadyEpoch,
                SOURCE_WORKER_CONTROL_TOTAL_TIMEOUT,
            )
        .await
        .expect("raw trigger_rescan_when_ready RPC should succeed")
        {
            SourceWorkerResponse::RescanRequestEpoch(_) => {}
            other => panic!("unexpected raw trigger_rescan_when_ready response: {other:?}"),
        }

        let baseline_target = b"/data";
        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before raw baseline publish wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during raw baseline publish wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("raw baseline watch-scan publish recv failed: {err}"),
            }
            let published_batches = latest_snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_data_events = latest_snapshot
                .published_data_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            if baseline_counts.len() >= 2 && published_batches > 0 && published_data_events > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "zero-grant raw worker-client trigger_rescan_when_ready must publish baseline /data for both roots before any cleanup tail: baseline_counts={baseline_counts:?} scheduled_source={:?} scheduled_scan={:?} published_batches={:?} published_events={:?} published_data={:?} last_published={:?} lifecycle={} concrete_roots={:?}",
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot.last_published_at_us_by_node,
                latest_snapshot.lifecycle_state,
                latest_snapshot.status.concrete_roots,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_watch_scan_route_trigger_rescan_republishes_baseline_after_cleanup_only_tail_without_runtime_host_grant_change()
    {
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
            fn drop(&mut self) {
                clear_source_worker_trigger_rescan_when_ready_call_count_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-local-sink-status-helper".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );
        let route = SourceFacade::Worker(client.clone().into());

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let cleanup_tail = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source roots deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan-control deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source scan deactivate"),
        ];
        assert!(
            is_restart_deferred_retire_pending_deactivate_batch(&cleanup_tail),
            "test precondition: cleanup tail must stay cleanup-only and skip post-ack refresh"
        );

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("initial trigger_rescan_when_ready should succeed");

        let baseline_target = b"/data";
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial watch-scan publish recv failed: {err}"),
            }
            if baseline_counts.len() >= 2 {
                break;
            }
        }
        assert!(
            baseline_counts.len() >= 2,
            "initial trigger_rescan_when_ready should publish /data for both watch-scan roots before cleanup tail: {baseline_counts:?}"
        );
        let baseline_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("baseline observability snapshot after initial trigger_rescan");
        let baseline_batches = baseline_snapshot
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_events = baseline_snapshot
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_last_published = baseline_snapshot
            .last_published_at_us_by_node
            .values()
            .copied()
            .max()
            .unwrap_or_default();
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        client
            .on_control_frame(cleanup_tail)
            .await
            .expect("cleanup-only watch-scan tail should settle without a fresh host grant envelope");

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: None,
            },
        );
        let trigger_count = Arc::new(AtomicUsize::new(0));
        let _trigger_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
        install_source_worker_trigger_rescan_when_ready_call_count_hook(
            SourceWorkerTriggerRescanWhenReadyCallCountHook {
                count: trigger_count.clone(),
            },
        );

        client
            .on_control_frame(source_wave(4))
            .await
            .expect("later watch-scan recovery should settle after cleanup-only tail");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later recovery")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later recovery")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "later watch-scan recovery should restore scheduled groups from accepted activate scopes even without a grant-change envelope: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("post-cleanup trigger_rescan_when_ready should succeed");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut republish_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before post-cleanup republish wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during post-cleanup republish wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut republish_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("post-cleanup watch-scan publish recv failed: {err}"),
            }
            let published_batches = latest_snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_events = latest_snapshot
                .published_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            let last_published = latest_snapshot
                .last_published_at_us_by_node
                .values()
                .copied()
                .max()
                .unwrap_or_default();
            if republish_counts.len() >= 2
                && (published_batches > baseline_batches
                    || published_events > baseline_events
                    || last_published > baseline_last_published)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "later watch-scan recovery should republish baseline after trigger_rescan_when_ready instead of leaving cleanup-tail publication empty: trigger_count={} baseline_counts={baseline_counts:?} republish_counts={republish_counts:?} baseline_batches={baseline_batches} baseline_events={baseline_events} baseline_last_published={baseline_last_published} latest_source={:?} latest_scan={:?} latest_published_batches={:?} latest_published_events={:?} latest_published_control={:?} latest_published_data={:?} latest_last_published={:?}",
                trigger_count.load(Ordering::SeqCst),
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_control_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot.last_published_at_us_by_node,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            trigger_count.load(Ordering::SeqCst) >= 1,
            "post-cleanup republish test must prove trigger_rescan_when_ready ran at least once"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_watch_scan_route_republishes_baseline_after_fail_closed_roots_deactivate_then_cleanup_only_tail_without_runtime_host_grant_change()
    {
        struct SourceWorkerControlFrameErrorHookReset;
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

        impl Drop for SourceWorkerControlFrameErrorHookReset {
            fn drop(&mut self) {
                clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
            fn drop(&mut self) {
                clear_source_worker_trigger_rescan_when_ready_call_count_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-failclosed-cleanup-tail-republish".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );
        let route = SourceFacade::Worker(client.clone().into());

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let cleanup_tail = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan-control deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source scan deactivate"),
        ];

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("initial trigger_rescan_when_ready should succeed");

        let baseline_target = b"/data";
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial watch-scan publish recv failed: {err}"),
            }
            if baseline_counts.len() >= 2 {
                break;
            }
        }
        assert!(
            baseline_counts.len() >= 2,
            "initial trigger_rescan_when_ready should publish /data for both watch-scan roots before the fail-closed roots deactivate: {baseline_counts:?}"
        );
        let baseline_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("baseline observability snapshot after initial trigger_rescan");
        let baseline_batches = baseline_snapshot
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_events = baseline_snapshot
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_last_published = baseline_snapshot
            .last_published_at_us_by_node
            .values()
            .copied()
            .max()
            .unwrap_or_default();
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        let _roots_error_reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated cleanup-tail source-roots failure".to_string(),
            ),
        });
        let fail_closed_err = client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                    capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                        route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                        unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                        lease: None,
                        generation: 3,
                        reason: "restart_deferred_retire_pending".to_string(),
                    },
                ))
                .expect("encode source roots deactivate"),
            ])
            .await
            .expect_err("single roots-control deactivate should fail-close before the cleanup-only tail");
        assert!(
            fail_closed_err
                .to_string()
                .contains("simulated cleanup-tail source-roots failure"),
            "unexpected fail-closed roots deactivate error: {fail_closed_err}"
        );
        clear_source_worker_control_frame_error_hook();

        client
            .on_control_frame(cleanup_tail)
            .await
            .expect("cleanup-only tail should still settle after the fail-closed roots deactivate");

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: None,
            },
        );
        let trigger_count = Arc::new(AtomicUsize::new(0));
        let _trigger_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
        install_source_worker_trigger_rescan_when_ready_call_count_hook(
            SourceWorkerTriggerRescanWhenReadyCallCountHook {
                count: trigger_count.clone(),
            },
        );

        client
            .on_control_frame(source_wave(4))
            .await
            .expect("later watch-scan recovery should settle after the fail-closed roots deactivate and cleanup-only tail");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later recovery")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later recovery")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "later watch-scan recovery should restore scheduled groups from accepted activate scopes even after the fail-closed roots deactivate: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("post-cleanup trigger_rescan_when_ready should succeed after the fail-closed roots deactivate");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut republish_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before post-fail-close republish wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during post-fail-close republish wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut republish_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("post-fail-close watch-scan publish recv failed: {err}"),
            }
            let published_batches = latest_snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_events = latest_snapshot
                .published_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            let last_published = latest_snapshot
                .last_published_at_us_by_node
                .values()
                .copied()
                .max()
                .unwrap_or_default();
            if republish_counts.len() >= 2
                && (published_batches > baseline_batches
                    || published_events > baseline_events
                    || last_published > baseline_last_published)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "later watch-scan recovery should republish baseline after a fail-closed roots deactivate plus cleanup-only tail instead of leaving publication empty: trigger_count={} baseline_counts={baseline_counts:?} republish_counts={republish_counts:?} baseline_batches={baseline_batches} baseline_events={baseline_events} baseline_last_published={baseline_last_published} latest_source={:?} latest_scan={:?} latest_published_batches={:?} latest_published_events={:?} latest_published_control={:?} latest_published_data={:?} latest_last_published={:?}",
                trigger_count.load(Ordering::SeqCst),
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_control_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot.last_published_at_us_by_node,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            trigger_count.load(Ordering::SeqCst) >= 1,
            "post-fail-close republish test must prove trigger_rescan_when_ready ran at least once"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_watch_scan_route_manual_rescan_republishes_baseline_after_fail_closed_roots_deactivate_then_cleanup_only_tail_without_runtime_host_grant_change()
    {
        struct SourceWorkerControlFrameErrorHookReset;
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

        impl Drop for SourceWorkerControlFrameErrorHookReset {
            fn drop(&mut self) {
                clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-failclosed-cleanup-tail-manual-rescan".to_string()),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let cleanup_tail = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan-control deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source scan deactivate"),
        ];

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave without host grant change should succeed");
        client
            .publish_manual_rescan_signal_with_failure()
            .await
            .expect("initial manual rescan should succeed");

        let baseline_target = b"/data";
        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        while tokio::time::Instant::now() < baseline_deadline {
            match recv_loopback_events(&boundary, 250).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => continue,
                Err(err) => panic!("initial manual rescan publish recv failed: {err}"),
            }
            if baseline_counts.len() >= 2 {
                break;
            }
        }
        assert!(
            baseline_counts.len() >= 2,
            "initial manual rescan should publish /data for both watch-scan roots before the fail-closed roots deactivate: {baseline_counts:?}"
        );
        let baseline_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("baseline observability snapshot after initial manual rescan");
        let baseline_batches = baseline_snapshot
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_events = baseline_snapshot
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_last_published = baseline_snapshot
            .last_published_at_us_by_node
            .values()
            .copied()
            .max()
            .unwrap_or_default();
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        let _roots_error_reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated cleanup-tail source-roots failure".to_string(),
            ),
        });
        let fail_closed_err = client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                    capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                        route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                        unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                        lease: None,
                        generation: 3,
                        reason: "restart_deferred_retire_pending".to_string(),
                    },
                ))
                .expect("encode source roots deactivate"),
            ])
            .await
            .expect_err("single roots-control deactivate should fail-close before the cleanup-only tail");
        assert!(
            fail_closed_err
                .to_string()
                .contains("simulated cleanup-tail source-roots failure"),
            "unexpected fail-closed roots deactivate error: {fail_closed_err}"
        );
        clear_source_worker_control_frame_error_hook();

        client
            .on_control_frame(cleanup_tail)
            .await
            .expect("cleanup-only tail should still settle after the fail-closed roots deactivate");

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: None,
            },
        );

        client
            .on_control_frame(source_wave(4))
            .await
            .expect("later watch-scan recovery should settle after the fail-closed roots deactivate and cleanup-only tail");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later recovery")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later recovery")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "later watch-scan recovery should restore scheduled groups from accepted activate scopes even after the fail-closed roots deactivate: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        client
            .publish_manual_rescan_signal_with_failure()
            .await
            .expect("post-cleanup manual rescan should succeed after the fail-closed roots deactivate");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut republish_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before post-fail-close manual-rescan wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during post-fail-close manual-rescan wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut republish_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("post-fail-close manual rescan publish recv failed: {err}"),
            }
            let published_batches = latest_snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_events = latest_snapshot
                .published_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            let last_published = latest_snapshot
                .last_published_at_us_by_node
                .values()
                .copied()
                .max()
                .unwrap_or_default();
            if republish_counts.len() >= 2
                && (published_batches > baseline_batches
                    || published_events > baseline_events
                    || last_published > baseline_last_published)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "later watch-scan recovery should republish baseline after a fail-closed roots deactivate plus cleanup-only tail when manual rescan is requested instead of leaving publication empty: baseline_counts={baseline_counts:?} republish_counts={republish_counts:?} baseline_batches={baseline_batches} baseline_events={baseline_events} baseline_last_published={baseline_last_published} latest_source={:?} latest_scan={:?} latest_published_batches={:?} latest_published_events={:?} latest_published_control={:?} latest_published_data={:?} latest_last_published={:?}",
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_control_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot.last_published_at_us_by_node,
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_real_source_route_manual_rescan_publishes_baseline_for_each_split_primary_under_mixed_cluster_grants()
    {
        let tmp = tempdir().expect("create temp dir");
        let node_a_nfs1 = tmp.path().join("node-a-nfs1");
        let node_a_nfs2 = tmp.path().join("node-a-nfs2");
        let node_b_nfs1 = tmp.path().join("node-b-nfs1");
        let node_c_nfs1 = tmp.path().join("node-c-nfs1");
        let node_c_nfs2 = tmp.path().join("node-c-nfs2");
        let node_d_nfs2 = tmp.path().join("node-d-nfs2");
        let node_b_nfs3 = tmp.path().join("node-b-nfs3");
        let node_d_nfs3 = tmp.path().join("node-d-nfs3");
        let node_e_nfs3 = tmp.path().join("node-e-nfs3");
        for path in [
            &node_a_nfs1,
            &node_a_nfs2,
            &node_b_nfs1,
            &node_c_nfs1,
            &node_c_nfs2,
            &node_d_nfs2,
            &node_b_nfs3,
            &node_d_nfs3,
            &node_e_nfs3,
        ] {
            std::fs::create_dir_all(path.join("data")).expect("create mount data dir");
        }
        std::fs::write(node_a_nfs1.join("data").join("a.txt"), b"a")
            .expect("seed node-a nfs1");
        std::fs::write(node_a_nfs2.join("data").join("b.txt"), b"b")
            .expect("seed node-a nfs2");
        std::fs::write(node_b_nfs3.join("data").join("c.txt"), b"c")
            .expect("seed node-b nfs3");

        let node_a_id = "node-a-29799407896396737569357825";
        let node_b_id = "node-b-29799407896396737569357825";
        let node_c_id = "node-c-29799407896396737569357825";
        let node_d_id = "node-d-29799407896396737569357825";
        let node_e_id = "node-e-29799407896396737569357825";
        let nfs1_source = "127.0.0.1:/exports/nfs1";
        let nfs2_source = "127.0.0.1:/exports/nfs2";
        let nfs3_source = "127.0.0.1:/exports/nfs3";

        let cfg = SourceConfig {
            roots: vec![
                worker_fs_source_watch_scan_root("nfs1", nfs1_source),
                worker_fs_source_watch_scan_root("nfs2", nfs2_source),
                worker_fs_source_watch_scan_root("nfs3", nfs3_source),
            ],
            host_object_grants: vec![
                worker_source_export_with_fs_source(
                    &format!("{node_a_id}::nfs1"),
                    node_a_id,
                    "10.0.0.11",
                    node_a_nfs1.clone(),
                    nfs1_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_a_id}::nfs2"),
                    node_a_id,
                    "10.0.0.12",
                    node_a_nfs2.clone(),
                    nfs2_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_b_id}::nfs1"),
                    node_b_id,
                    "10.0.0.21",
                    node_b_nfs1.clone(),
                    nfs1_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_c_id}::nfs1"),
                    node_c_id,
                    "10.0.0.31",
                    node_c_nfs1.clone(),
                    nfs1_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_c_id}::nfs2"),
                    node_c_id,
                    "10.0.0.32",
                    node_c_nfs2.clone(),
                    nfs2_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_d_id}::nfs2"),
                    node_d_id,
                    "10.0.0.41",
                    node_d_nfs2.clone(),
                    nfs2_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_b_id}::nfs3"),
                    node_b_id,
                    "10.0.0.23",
                    node_b_nfs3.clone(),
                    nfs3_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_d_id}::nfs3"),
                    node_d_id,
                    "10.0.0.43",
                    node_d_nfs3.clone(),
                    nfs3_source,
                ),
                worker_source_export_with_fs_source(
                    &format!("{node_e_id}::nfs3"),
                    node_e_id,
                    "10.0.0.53",
                    node_e_nfs3.clone(),
                    nfs3_source,
                ),
            ],
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = SourceWorkerClientHandle::new(
            NodeId(node_a_id.to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client");

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                        bound_scope_with_resources("nfs3", &["nfs3"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                        bound_scope_with_resources("nfs3", &["nfs3"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                        bound_scope_with_resources("nfs3", &["nfs3"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                        bound_scope_with_resources("nfs3", &["nfs3"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("mixed-cluster real source route wave should succeed");

        let expected_primaries = std::collections::BTreeMap::from([
            ("nfs1".to_string(), format!("{node_a_id}::nfs1")),
            ("nfs2".to_string(), format!("{node_a_id}::nfs2")),
            ("nfs3".to_string(), format!("{node_b_id}::nfs3")),
        ]);
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let primaries = client
                .source_primary_by_group_snapshot_with_failure()
                .await
                .expect("source primary snapshot");
            if primaries == expected_primaries {
                break;
            }
            assert!(
                tokio::time::Instant::now() < ready_deadline,
                "mixed-cluster real source route should elect split primaries before manual rescan: primaries={primaries:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        while recv_loopback_events(&boundary, 50).await.is_ok() {}

        client
            .publish_manual_rescan_signal_with_failure()
            .await
            .expect("manual rescan publish should succeed");

        let baseline_target = b"/data";
        let expected_origins = [
            format!("{node_a_id}::nfs1"),
            format!("{node_a_id}::nfs2"),
        ];
        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut latest_snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot before mixed-cluster manual-rescan wait");
        loop {
            latest_snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during mixed-cluster manual-rescan wait");
            match recv_loopback_events(&boundary, 50).await {
                Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
                Err(CnxError::Timeout) => {}
                Err(err) => panic!("mixed-cluster manual rescan publish recv failed: {err}"),
            }
            if expected_origins
                .iter()
                .all(|origin| baseline_counts.get(origin).copied().unwrap_or(0) > 0)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "manual rescan should publish baseline /data for node-a split-primary roots under mixed cluster grants instead of leaving them unpublished: baseline_counts={baseline_counts:?} primaries={:?} scheduled_source={:?} scheduled_scan={:?} published_batches={:?} published_events={:?} published_data={:?} concrete_roots={:?}",
                latest_snapshot.source_primary_by_group,
                latest_snapshot.scheduled_source_groups_by_node,
                latest_snapshot.scheduled_scan_groups_by_node,
                latest_snapshot.published_batches_by_node,
                latest_snapshot.published_events_by_node,
                latest_snapshot.published_data_events_by_node,
                latest_snapshot
                    .status
                    .concrete_roots
                    .iter()
                    .map(|root| (
                        root.logical_root_id.clone(),
                        root.object_ref.clone(),
                        root.is_group_primary,
                        root.emitted_batch_count,
                        root.emitted_data_event_count,
                    ))
                    .collect::<Vec<_>>(),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        client.close().await.expect("close source worker");
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_watch_scan_route_republishes_observability_after_fail_closed_roots_deactivate_then_cleanup_only_tail_without_runtime_host_grant_change()
    {
        struct SourceWorkerControlFrameErrorHookReset;
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

        impl Drop for SourceWorkerControlFrameErrorHookReset {
            fn drop(&mut self) {
                clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
            fn drop(&mut self) {
                clear_source_worker_trigger_rescan_when_ready_call_count_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
        std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
        std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
        std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-failclosed-cleanup-tail-observability".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );
        let route = SourceFacade::Worker(client.clone().into());

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let cleanup_tail = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan-control deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source scan deactivate"),
        ];

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("initial trigger_rescan_when_ready should succeed");

        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let baseline_snapshot = loop {
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("baseline observability snapshot");
            let published_batches = snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            if published_batches > 0 {
                break snapshot;
            }
            assert!(
                tokio::time::Instant::now() < baseline_deadline,
                "initial trigger_rescan_when_ready should publish baseline before cleanup tail: {snapshot:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let baseline_batches = baseline_snapshot
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_events = baseline_snapshot
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_last_published = baseline_snapshot
            .last_published_at_us_by_node
            .values()
            .copied()
            .max()
            .unwrap_or_default();

        let _roots_error_reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated cleanup-tail source-roots failure".to_string(),
            ),
        });
        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                    capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                        route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                        unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                        lease: None,
                        generation: 3,
                        reason: "restart_deferred_retire_pending".to_string(),
                    },
                ))
                .expect("encode source roots deactivate"),
            ])
            .await
            .expect_err("single roots-control deactivate should fail-close before cleanup tail");
        clear_source_worker_control_frame_error_hook();

        client
            .on_control_frame(cleanup_tail)
            .await
            .expect("cleanup-only tail should settle after fail-closed roots deactivate");

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: None,
            },
        );
        let trigger_count = Arc::new(AtomicUsize::new(0));
        let _trigger_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
        install_source_worker_trigger_rescan_when_ready_call_count_hook(
            SourceWorkerTriggerRescanWhenReadyCallCountHook {
                count: trigger_count.clone(),
            },
        );

        client
            .on_control_frame(source_wave(4))
            .await
            .expect("later watch-scan recovery should settle after fail-closed cleanup tail");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later recovery")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later recovery")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "later watch-scan recovery should restore scheduled groups before retriggering republish: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("first post-cleanup trigger_rescan_when_ready should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("second post-cleanup trigger_rescan_when_ready should succeed");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during post-fail-close republish wait");
            let published_batches = snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_events = snapshot
                .published_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            let last_published = snapshot
                .last_published_at_us_by_node
                .values()
                .copied()
                .max()
                .unwrap_or_default();
            if published_batches > baseline_batches
                || published_events > baseline_events
                || last_published > baseline_last_published
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "later watch-scan recovery should republish observability after two direct post-cleanup retriggers instead of leaving publication flat: trigger_count={} baseline_batches={baseline_batches} baseline_events={baseline_events} baseline_last_published={baseline_last_published} snapshot={snapshot:?}",
                trigger_count.load(Ordering::SeqCst),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            trigger_count.load(Ordering::SeqCst) >= 2,
            "post-cleanup observability republish probe must prove two explicit trigger_rescan_when_ready calls ran"
        );

        client.close().await.expect("close source worker");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn generation_two_watch_scan_route_republishes_root_file_observability_after_fail_closed_roots_deactivate_then_cleanup_only_tail_without_runtime_host_grant_change()
    {
        struct SourceWorkerControlFrameErrorHookReset;
        struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

        impl Drop for SourceWorkerControlFrameErrorHookReset {
            fn drop(&mut self) {
                clear_source_worker_control_frame_error_hook();
            }
        }

        impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
            fn drop(&mut self) {
                clear_source_worker_scheduled_groups_refresh_error_queue_hook();
            }
        }

        impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
            fn drop(&mut self) {
                clear_source_worker_trigger_rescan_when_ready_call_count_hook();
            }
        }

        let tmp = tempdir().expect("create temp dir");
        let nfs1 = tmp.path().join("nfs1");
        let nfs2 = tmp.path().join("nfs2");
        std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
        std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
        std::fs::write(nfs1.join("ready-a.txt"), b"a").expect("seed nfs1 root file");
        std::fs::write(nfs2.join("ready-b.txt"), b"b").expect("seed nfs2 root file");

        let cfg = SourceConfig {
            roots: vec![
                worker_watch_scan_root("nfs1", &nfs1),
                worker_watch_scan_root("nfs2", &nfs2),
            ],
            host_object_grants: Vec::new(),
            ..SourceConfig::default()
        };
        let boundary = Arc::new(LoopbackWorkerBoundary::default());
        let state_boundary = in_memory_state_boundary();
        let worker_socket_dir = worker_socket_tempdir();
        let factory =
            RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
        let client = Arc::new(
            SourceWorkerClientHandle::new(
                NodeId("node-c-failclosed-cleanup-tail-root-observability".to_string()),
                cfg,
                external_source_worker_binding(worker_socket_dir.path()),
                factory,
            )
            .expect("construct source worker client"),
        );
        let route = SourceFacade::Worker(client.clone().into());

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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
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
                        bound_scope_with_resources("nfs1", &["nfs1"]),
                        bound_scope_with_resources("nfs2", &["nfs2"]),
                    ],
                }))
                .expect("encode source scan activate"),
            ]
        };
        let cleanup_tail = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan-control deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source rescan deactivate"),
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                    route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode source scan deactivate"),
        ];

        client
            .on_control_frame(source_wave(2))
            .await
            .expect("initial watch-scan source wave should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("initial trigger_rescan_when_ready should succeed");

        let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let baseline_snapshot = loop {
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("baseline observability snapshot");
            let published_batches = snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            if published_batches > 0 {
                break snapshot;
            }
            assert!(
                tokio::time::Instant::now() < baseline_deadline,
                "initial trigger_rescan_when_ready should publish root-file baseline before cleanup tail: {snapshot:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let baseline_batches = baseline_snapshot
            .published_batches_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_events = baseline_snapshot
            .published_events_by_node
            .values()
            .copied()
            .sum::<u64>();
        let baseline_last_published = baseline_snapshot
            .last_published_at_us_by_node
            .values()
            .copied()
            .max()
            .unwrap_or_default();

        let _roots_error_reset = SourceWorkerControlFrameErrorHookReset;
        install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated cleanup-tail source-roots failure".to_string(),
            ),
        });
        client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
                    capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                        route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                        unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                        lease: None,
                        generation: 3,
                        reason: "restart_deferred_retire_pending".to_string(),
                    },
                ))
                .expect("encode source roots deactivate"),
            ])
            .await
            .expect_err("single roots-control deactivate should fail-close before cleanup tail");
        clear_source_worker_control_frame_error_hook();

        client
            .on_control_frame(cleanup_tail)
            .await
            .expect("cleanup-only tail should settle after fail-closed roots deactivate");

        let previous_instance_id = client.worker_instance_id_for_tests().await;
        let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
        install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::iter::repeat_with(|| CnxError::Timeout)
                    .take(64)
                    .collect(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: None,
            },
        );
        let trigger_count = Arc::new(AtomicUsize::new(0));
        let _trigger_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
        install_source_worker_trigger_rescan_when_ready_call_count_hook(
            SourceWorkerTriggerRescanWhenReadyCallCountHook {
                count: trigger_count.clone(),
            },
        );

        client
            .on_control_frame(source_wave(4))
            .await
            .expect("later watch-scan recovery should settle after fail-closed cleanup tail");

        let expected_groups =
            std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
        let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later recovery")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later recovery")
                .unwrap_or_default();
            if source_groups == expected_groups && scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < scheduled_deadline,
                "later watch-scan recovery should restore scheduled groups before retriggering root-file republish: source={source_groups:?} scan={scan_groups:?}",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("first post-cleanup trigger_rescan_when_ready should succeed");
        route
            .trigger_rescan_when_ready_epoch_with_failure()
            .await
            .expect("second post-cleanup trigger_rescan_when_ready should succeed");

        let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot during root-file post-fail-close republish wait");
            let published_batches = snapshot
                .published_batches_by_node
                .values()
                .copied()
                .sum::<u64>();
            let published_events = snapshot
                .published_events_by_node
                .values()
                .copied()
                .sum::<u64>();
            let last_published = snapshot
                .last_published_at_us_by_node
                .values()
                .copied()
                .max()
                .unwrap_or_default();
            if published_batches > baseline_batches
                || published_events > baseline_events
                || last_published > baseline_last_published
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < republish_deadline,
                "later watch-scan recovery should republish root-file observability after two direct post-cleanup retriggers instead of leaving publication flat: trigger_count={} baseline_batches={baseline_batches} baseline_events={baseline_events} baseline_last_published={baseline_last_published} snapshot={snapshot:?}",
                trigger_count.load(Ordering::SeqCst),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            trigger_count.load(Ordering::SeqCst) >= 2,
            "post-cleanup root-file observability probe must prove two explicit trigger_rescan_when_ready calls ran"
        );

        client.close().await.expect("close source worker");
    }
    };
}

pub(crate) use define_trigger_rescan_republish_tests;
