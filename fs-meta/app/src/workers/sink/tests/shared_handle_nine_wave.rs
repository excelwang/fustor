#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_exact_shaped_sink_nine_wave_reacquires_worker_client_after_first_wave_succeeded_without_external_ensure_started()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    client
        .on_control_frame(sink_wave(2))
        .await
        .expect("first exact-shaped sink nine-wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => client.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before worker restart"
    );
    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before worker restart"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_wave = tokio::spawn({
        let client = client.clone();
        async move { client.on_control_frame(sink_wave(3)).await }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge after first exact-shaped sink nine-wave");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second exact-shaped sink nine-wave should restart the sink worker client without external ensure_started")
            .expect("join second exact-shaped sink nine-wave")
            .expect("second exact-shaped sink nine-wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after automatic second nine-wave restart: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after second wave restart"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after second wave restart")
        .is_some(),
        "second exact-shaped sink nine-wave restart must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after second wave restart"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after second wave restart")
        .is_some(),
        "second exact-shaped sink nine-wave restart must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_exact_shaped_sink_nine_wave_reacquires_worker_client_after_first_wave_succeeded_and_repeated_internal_peer_early_eof_errors()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    client
        .on_control_frame(sink_wave(2))
        .await
        .expect("first exact-shaped sink nine-wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => client.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before repeated early-eof recovery"
    );
    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before repeated early-eof recovery"
    );

    let previous_worker_identity = client.shared_worker_identity_for_tests().await;
    let previous_worker_instance_id = client.worker_instance_id_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    struct SinkWorkerControlFrameErrorQueueHookReset;
    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }
    let _error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                        .to_string(),
                ),
            ]),
            sticky_worker_instance_id: Some(previous_worker_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(sink_wave(3))
            .await
            .expect(
                "second exact-shaped sink nine-wave should reacquire a fresh worker client after repeated internal early-eof/reset errors on the stale instance",
            );

    let next_worker_identity = client.shared_worker_identity_for_tests().await;
    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "repeated internal early-eof/reset recovery must reset the shared sink worker client"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "repeated internal early-eof/reset recovery must replace the stale shared sink worker client"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after repeated internal early-eof/reset recovery: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after repeated early-eof recovery"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after repeated early-eof recovery")
        .is_some(),
        "repeated internal early-eof/reset recovery must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after repeated early-eof recovery"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after repeated early-eof recovery")
        .is_some(),
        "repeated internal early-eof/reset recovery must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_exact_shaped_sink_nine_wave_first_post_replay_status_snapshot_nonblocking_does_not_publish_scheduled_zero_uninitialized_groups_after_repeated_internal_peer_early_eof_errors()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    client
        .on_control_frame(sink_wave(2))
        .await
        .expect("first exact-shaped sink nine-wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => client.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before repeated early-eof recovery"
    );
    assert!(
        decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before repeated early-eof recovery"
    );

    let previous_worker_instance_id = client.worker_instance_id_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                        .to_string(),
                ),
            ]),
            sticky_worker_instance_id: Some(previous_worker_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(sink_wave(3))
            .await
            .expect(
                "second exact-shaped sink nine-wave should reacquire a fresh worker client after repeated internal early-eof/reset errors on the stale instance",
            );
    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "same-handle second exact-shaped sink nine-wave must reset the shared worker client after repeated internal early-eof/reset"
    );

    client
        .control_state_replay_required
        .store(1, Ordering::Release);
    client
        .update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before first post-replay status snapshot");

    match client.status_snapshot_nonblocking().await {
        Ok(snapshot) => {
            assert!(
                snapshot
                    .groups
                    .iter()
                    .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                    .all(|group| {
                        group.is_ready()
                            && group.live_nodes > 0
                            && group.total_nodes > 0
                            && group.materialized_revision > 1
                    }),
                "first post-replay status_snapshot_nonblocking after same-handle reset must restore ready state before publishing scheduled groups: {snapshot:?}"
            );
        }
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "first post-replay status_snapshot_nonblocking after same-handle reset must restore ready state or fail-close, got {err}"
        ),
    }

    source.close().await.expect("close source");
    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_exact_shaped_sink_nine_wave_first_post_replay_status_snapshot_restores_ready_state_after_repeated_internal_peer_early_eof_errors()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    client
        .on_control_frame(sink_wave(2))
        .await
        .expect("first exact-shaped sink nine-wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => client.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            client
                .materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    let previous_worker_instance_id = client.worker_instance_id_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
                CnxError::Internal(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                        .to_string(),
                ),
            ]),
            sticky_worker_instance_id: Some(previous_worker_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(sink_wave(3))
            .await
            .expect(
                "second exact-shaped sink nine-wave should reacquire a fresh worker client after repeated internal early-eof/reset errors on the stale instance",
            );
    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "same-handle second exact-shaped sink nine-wave must reset the shared worker client after repeated internal early-eof/reset"
    );

    client
        .control_state_replay_required
        .store(1, Ordering::Release);
    client
        .update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before first post-replay blocking status snapshot");

    match client.status_snapshot().await {
        Ok(snapshot) => {
            assert!(
                snapshot
                    .groups
                    .iter()
                    .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                    .all(|group| {
                        group.is_ready()
                            && group.live_nodes > 0
                            && group.total_nodes > 0
                            && group.materialized_revision > 1
                    }),
                "first post-replay blocking status_snapshot after same-handle reset must restore ready state instead of reopening with replay-only/not-ready groups: {snapshot:?}"
            );
        }
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "first post-replay blocking status_snapshot after same-handle reset must restore ready state or fail-close, got {err}"
        ),
    }

    source.close().await.expect("close source");
    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_sink_handle_later_nine_wave_survives_predecessor_fail_closed_cleanup_and_intermediate_success()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_sink_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor sink worker client"),
    );
    let successor = Arc::new(
        SinkWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start shared sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    predecessor
        .on_control_frame(sink_wave(2))
        .await
        .expect("predecessor initial exact-shaped sink nine-wave should succeed");

    let _deactivate_pause_reset = SinkWorkerControlFramePauseHookReset;
    let deactivate_entered = Arc::new(Notify::new());
    let deactivate_release = Arc::new(Notify::new());
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: deactivate_entered.clone(),
        release: deactivate_release.clone(),
    });

    let fail_closed_deactivate = tokio::spawn({
        let predecessor = predecessor.clone();
        async move {
            predecessor
                .on_control_frame(vec![
                    encode_runtime_exec_control(
                        &capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                            RuntimeExecDeactivate {
                                route_key: format!(
                                    "{}.stream",
                                    crate::runtime::routes::ROUTE_KEY_EVENTS
                                ),
                                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                    .to_string(),
                                lease: None,
                                generation: 3,
                                reason: "restart_deferred_retire_pending".to_string(),
                            },
                        ),
                    )
                    .expect("encode sink events deactivate"),
                ])
                .await
        }
    });

    deactivate_entered.notified().await;
    predecessor
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown shared sink worker during predecessor fail-closed cleanup");
    deactivate_release.notify_waiters();
    let _ = fail_closed_deactivate
        .await
        .expect("join predecessor fail-closed deactivate");

    tokio::time::timeout(
        Duration::from_secs(8),
        successor.on_control_frame(sink_wave(4)),
    )
    .await
    .expect("intermediate successor exact-shaped sink nine-wave should settle")
    .expect("intermediate successor exact-shaped sink nine-wave should recover");

    predecessor
        .close()
        .await
        .expect("close predecessor shared sink handle after intermediate success");

    tokio::time::timeout(Duration::from_secs(8), successor.on_control_frame(sink_wave(5)))
            .await
            .expect("later successor exact-shaped sink nine-wave should settle promptly after predecessor fail-closed cleanup plus intermediate success")
            .expect("later successor exact-shaped sink nine-wave should still succeed after predecessor fail-closed cleanup plus intermediate success");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = successor
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after later shared-handle nine-wave recovery: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    successor
        .close()
        .await
        .expect("close successor sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_sink_handle_later_nine_wave_reacquires_worker_after_predecessor_fail_closed_cleanup_and_intermediate_success_then_repeated_early_eof()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_sink_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor sink worker client"),
    );
    let successor = Arc::new(
        SinkWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start shared sink worker");

    let sink_wave = |generation| {
        let mut signals = vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!(
                    "{}.stream",
                    crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
                ),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", crate::runtime::routes::ROUTE_KEY_FORCE_FIND),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink force-find activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: "materialized-find:v1.req".to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode materialized-find activate"),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: crate::runtime::routes::sink_query_request_route_for(node_id).0,
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode per-node materialized-find activate"),
            );
        }
        signals
    };

    predecessor
        .on_control_frame(sink_wave(2))
        .await
        .expect("predecessor initial exact-shaped sink nine-wave should succeed");

    let _deactivate_pause_reset = SinkWorkerControlFramePauseHookReset;
    let deactivate_entered = Arc::new(Notify::new());
    let deactivate_release = Arc::new(Notify::new());
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: deactivate_entered.clone(),
        release: deactivate_release.clone(),
    });

    let fail_closed_deactivate = tokio::spawn({
        let predecessor = predecessor.clone();
        async move {
            predecessor
                .on_control_frame(vec![
                    encode_runtime_exec_control(
                        &capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                            RuntimeExecDeactivate {
                                route_key: format!(
                                    "{}.stream",
                                    crate::runtime::routes::ROUTE_KEY_EVENTS
                                ),
                                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                    .to_string(),
                                lease: None,
                                generation: 3,
                                reason: "restart_deferred_retire_pending".to_string(),
                            },
                        ),
                    )
                    .expect("encode sink events deactivate"),
                ])
                .await
        }
    });

    deactivate_entered.notified().await;
    predecessor
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown shared sink worker during predecessor fail-closed cleanup");
    deactivate_release.notify_waiters();
    let _ = fail_closed_deactivate
        .await
        .expect("join predecessor fail-closed deactivate");

    tokio::time::timeout(
        Duration::from_secs(8),
        successor.on_control_frame(sink_wave(4)),
    )
    .await
    .expect("intermediate successor exact-shaped sink nine-wave should settle")
    .expect("intermediate successor exact-shaped sink nine-wave should recover");

    predecessor
        .close()
        .await
        .expect("close predecessor shared sink handle after intermediate success");

    let previous_worker_identity = successor.shared_worker_identity_for_tests().await;
    let previous_worker_instance_id = successor.worker_instance_id_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: Some(previous_worker_instance_id),
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(8), successor.on_control_frame(sink_wave(5)))
            .await
            .expect(
                "later successor exact-shaped sink nine-wave should settle promptly after predecessor fail-closed cleanup plus repeated internal early-eof/reset",
            )
            .expect(
                "later successor exact-shaped sink nine-wave should still succeed after predecessor fail-closed cleanup plus repeated internal early-eof/reset",
            );

    let next_worker_identity = successor.shared_worker_identity_for_tests().await;
    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "later nine-wave recovery must reset the shared sink worker client after repeated internal early-eof/reset"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "later nine-wave recovery must replace the stale shared sink worker client after repeated internal early-eof/reset"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = successor
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after later shared-handle nine-wave recovery with repeated internal early-eof/reset: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    successor
        .close()
        .await
        .expect("close successor sink worker");
}
