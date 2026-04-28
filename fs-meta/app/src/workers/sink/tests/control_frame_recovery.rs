#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error and reach the live sink worker",
        );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors_after_first_wave_succeeded() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send_with_failure(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
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
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before stale drained/fenced retry"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before stale drained/fenced retry"
    );

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_EVENTS.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                reason: "test deactivate".to_string(),
            }))
            .expect("encode sink deactivate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error after the first sink wave already succeeded",
        );

    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after drained/fenced retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after drained/fenced retry")
        .is_some(),
        "stale drained/fenced retry must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after drained/fenced retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after drained/fenced retry")
        .is_some(),
        "stale drained/fenced retry must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_invalid_or_revoked_grant_attachment_tokens_after_first_wave_succeeded()
 {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::AccessDenied("invalid or revoked grant attachment token".to_string()),
            "on_control_frame should retry an invalid or revoked grant attachment token after the first sink wave already succeeded",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_channel_closed_after_first_wave_succeeded() {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::ChannelClosed,
            "on_control_frame should retry channel-closed continuity gaps after the first sink wave already succeeded",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_channel_closed_followup_resets_shared_client_before_retry() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let previous_worker_identity = sink.shared_worker_identity_for_tests().await;

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
        err: CnxError::ChannelClosed,
    });

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("follow-up sink control wave should recover after channel-closed error");

    let next_worker_identity = sink.shared_worker_identity_for_tests().await;

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "follow-up channel-closed recovery must reset the shared sink worker client before retry"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "follow-up channel-closed recovery must replace the shared sink worker handle before retry so later waves cannot inherit the stale bridge session"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_missing_channel_buffer_state_resets_shared_client_before_retry() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let previous_worker_identity = sink.shared_worker_identity_for_tests().await;

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
        err: CnxError::Internal(
            "sink worker unavailable: missing route state for channel_buffer ChannelSlotId(4585)"
                .to_string(),
        ),
    });

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("follow-up sink control wave should recover after missing channel_buffer route state");

    let next_worker_identity = sink.shared_worker_identity_for_tests().await;

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "missing channel_buffer route-state recovery must reset the shared sink worker client before retry"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "missing channel_buffer route-state recovery must replace the shared sink worker handle before retry"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_enforces_total_timeout_when_worker_call_stalls_after_first_wave() {
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
    let sink = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode first-wave sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ],
                Duration::from_millis(150),
                Duration::from_millis(150),
            )
            .await
        }
    });

    entered.notified().await;
    let err = tokio::time::timeout(Duration::from_secs(1), control_task)
        .await
        .expect(
            "stalled sink on_control_frame should resolve within the local total timeout budget",
        )
        .expect("join stalled sink on_control_frame task")
        .expect_err(
            "stalled sink on_control_frame should fail once its local timeout budget is exhausted",
        );
    assert!(matches!(err, CnxError::Timeout), "err={err:?}");

    release.notify_waiters();
    sink.close().await.expect("close sink worker");
}

async fn assert_on_control_frame_retries_bridge_reset_error(err: CnxError, label: &str) {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook { err });

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode second-wave sink activate"),
    ])
    .await
    .unwrap_or_else(|err| panic!("{label}: {err}"));

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "{label}: scheduled groups should remain converged after retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_internal_peer_early_eof_errors_after_first_wave_succeeded() {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::Internal(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
            "on_control_frame should retry an internal early-eof bridge error and reach the live sink worker",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_generation_two_missing_route_state_then_bridge_resets_fail_fast_before_local_apply_timeout()
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let activate = encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
        route_key: ROUTE_KEY_QUERY.to_string(),
        unit_id: "runtime.exec.sink".to_string(),
        lease: None,
        generation: 2,
        expires_at_ms: 1,
        bound_scopes: vec![
            bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
            bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
        ],
    }))
    .expect("encode sink activate");

    sink.on_control_frame(vec![activate.clone()])
        .await
        .expect("first sink control wave should succeed");

    let previous_worker_identity = sink.shared_worker_identity_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![
            CnxError::Internal(
                "sink worker unavailable: missing route state for channel_buffer ChannelSlotId(24123)"
                    .to_string(),
            ),
            CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
            CnxError::Internal(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        ]),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let started = std::time::Instant::now();
    let err = sink
        .on_control_frame_with_timeouts_for_tests(
            vec![activate],
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two local apply continuity should fail fast once missing route state is followed by bridge resets instead of hanging until the outer timeout",
        );

    let next_worker_identity = sink.shared_worker_identity_for_tests().await;
    assert!(
        reset_count.load(Ordering::SeqCst) >= 2,
        "missing route state plus bridge resets must reset the shared sink worker client before fail-closing"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "generation-two continuity fail-close must replace the stale shared sink worker client"
    );
    assert!(
        !matches!(err, CnxError::Timeout),
        "generation-two continuity fail-close should surface the bridge break instead of exhausting the full local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "generation-two continuity fail-close should not spend the bounded local budget on blind retry sleep: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_events_deactivate_fails_fast_after_repeated_bridge_reset_errors()
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode initial sink events activate"),
    ])
    .await
    .expect("initial sink events wave should succeed");

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let started = std::time::Instant::now();
    let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "single restart_deferred_retire_pending events deactivate should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset fail-close lane should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "bridge-reset fail-close lane should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_events_deactivate_retries_stale_drained_fenced_pid_error_after_first_wave_succeeded()
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send_with_failure(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
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
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before restart_deferred_retire_pending retry"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before restart_deferred_retire_pending retry"
    );

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            )]),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                RuntimeExecDeactivate {
                    route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode sink events deactivate"),
        ])
        .await
        .expect(
            "single restart_deferred_retire_pending events deactivate should retry a stale drained/fenced pid error and reach the live sink worker",
        );

    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after restart_deferred_retire_pending retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after restart_deferred_retire_pending retry")
        .is_some(),
        "restart_deferred_retire_pending retry must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query_with_failure(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after restart_deferred_retire_pending retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after restart_deferred_retire_pending retry")
        .is_some(),
        "restart_deferred_retire_pending retry must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = sink
            .status_snapshot_with_failure()
            .await
            .expect("status_snapshot should retry a stale drained/fenced pid error and reach the live sink worker");

    assert_eq!(
        snapshot.groups.len(),
        1,
        "fresh live sink worker snapshot should still decode after stale-pid retry"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_group_ids_retry_stale_drained_fenced_pid_errors() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("initial scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups before stale-pid retry test: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SinkWorkerScheduledGroupsErrorHookReset;
    install_sink_worker_scheduled_groups_error_hook(SinkWorkerScheduledGroupsErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled_group_ids should retry stale drained/fenced pid errors and reach the live sink worker");

    assert_eq!(scheduled, Some(expected_groups));

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_restart_deferred_retire_pending_cleanup_batch_fails_fast_after_repeated_bridge_reset_errors()
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode initial sink events activate"),
    ])
    .await
    .expect("initial sink events wave should succeed");

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let started = std::time::Instant::now();
    let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_c:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-c query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_d:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-d query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode aggregate query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "on-demand-force-find:v1.on-demand-force-find.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode force-find query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "sink-logical-roots-control:v1.stream".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink roots control deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "multi-envelope restart_deferred_retire_pending cleanup batch should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset retained cleanup batch should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "bridge-reset retained cleanup batch should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let first_snapshot = sink
        .status_snapshot_with_failure()
        .await
        .expect("prime cached status snapshot");
    assert!(
        first_snapshot.scheduled_groups_by_node.is_empty(),
        "primed cached sink status should start without scheduled groups: {:?}",
        first_snapshot.scheduled_groups_by_node
    );

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking should still return a snapshot");

    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-d"),
        Some(&vec!["nfs1".to_string()]),
        "status_snapshot_nonblocking should retry a peer bridge-stopped error and reach the live sink worker instead of returning the stale cached pre-activate snapshot: {:?}",
        snapshot.scheduled_groups_by_node
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_group_ids_channel_closed_resets_shared_client_before_retry() {
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
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("initial scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups before channel-closed retry test: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let previous_worker_identity = sink.shared_worker_identity_for_tests().await;
    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerScheduledGroupsErrorHookReset;
    install_sink_worker_scheduled_groups_error_hook(SinkWorkerScheduledGroupsErrorHook {
        err: CnxError::ChannelClosed,
    });

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled_group_ids should recover after channel-closed error")
        .expect("scheduled_group_ids should remain available after channel-closed retry");

    let next_worker_identity = sink.shared_worker_identity_for_tests().await;

    assert_eq!(
        scheduled, expected_groups,
        "scheduled_group_ids retry should still reach the live sink worker and preserve scheduled groups"
    );
    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "scheduled_group_ids channel-closed recovery must reset the shared sink worker client before retry"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "scheduled_group_ids channel-closed recovery must replace the shared sink worker client before retry"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
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

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _reset = SinkWorkerUpdateRootsHookReset;
    install_sink_worker_update_roots_hook(SinkWorkerUpdateRootsHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let update_task = tokio::spawn({
        let client = client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            client
                .update_logical_roots_with_failure(
                    vec![
                        sink_worker_root("nfs1", &nfs1),
                        sink_worker_root("nfs2", &nfs2),
                    ],
                    vec![
                        sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                        sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                    ],
                )
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale worker bridge");
    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(4), update_task)
        .await
        .expect("update_logical_roots should reacquire a live sink worker client after handoff")
        .expect("join update_logical_roots task")
        .expect("update_logical_roots after worker restart");

    let roots = client
        .logical_roots_snapshot_with_failure()
        .await
        .expect("logical roots snapshot after update");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-handoff sink update"
    );

    client.close().await.expect("close sink worker");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_restore_preserves_pending_groups_until_runtime_reassigns_scope() {
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

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("apply initial sink control");

    client
        .update_logical_roots_with_failure(
            vec![sink_worker_root("nfs1", &nfs1), sink_worker_root("nfs2", &nfs2)],
            vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
        )
        .await
        .expect("restore nfs2 logical root");

    let scheduled = client
        .scheduled_group_ids()
        .await
        .expect("scheduled groups")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string()]),
        "restoring logical roots must not widen sink placement before runtime reassigns nfs2"
    );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 2,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink reactivate with nfs2"),
        ])
        .await
        .expect("apply runtime reassignment");

    let scheduled_after_reassign = client
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after runtime reassignment")
        .unwrap_or_default();
    assert_eq!(
        scheduled_after_reassign,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "runtime reassignment must reopen restored pending groups for materialization"
    );

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_does_not_clear_cached_ready_status_for_surviving_groups() {
    let tmp = tempdir().expect("create temp dir");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    let nfs4 = tmp.path().join("nfs4");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");
    std::fs::create_dir_all(&nfs4).expect("create nfs4 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs2", &nfs2),
            sink_worker_root("nfs3", &nfs3),
            sink_worker_root("nfs4", &nfs4),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            sink_worker_export("node-d::nfs3", "node-d", "10.0.0.43", nfs3.clone()),
            sink_worker_export("node-d::nfs4", "node-d", "10.0.0.44", nfs4.clone()),
        ],
        ..SourceConfig::default()
    };
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

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    bound_scope_with_resources("nfs3", &["node-d::nfs3"]),
                    bound_scope_with_resources("nfs4", &["node-d::nfs4"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("apply sink control");

    client
        .send_with_failure(vec![
            mk_worker_sink_source_event(
                "node-d::nfs2",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 1,
                        ctime_us: 1,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    1,
                    false,
                ),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_worker_sink_source_event(
                "node-d::nfs2",
                mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 3, EventKind::Update),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
            mk_worker_sink_source_event(
                "node-d::nfs3",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 5,
                        ctime_us: 5,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    5,
                    false,
                ),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs3",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
            mk_worker_sink_source_event(
                "node-d::nfs3",
                mk_worker_sink_record(b"/retired-c.txt", "retired-c.txt", 7, EventKind::Update),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs3",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                8,
            ),
            mk_worker_sink_source_event(
                "node-d::nfs4",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 9,
                        ctime_us: 9,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    9,
                    false,
                ),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs4",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                10,
            ),
            mk_worker_sink_source_event(
                "node-d::nfs4",
                mk_worker_sink_record(b"/ready-d.txt", "ready-d.txt", 11, EventKind::Update),
            ),
            mk_worker_sink_control_event(
                "node-d::nfs4",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                12,
            ),
        ])
        .await
        .expect("seed ready sink state before logical-root retire");

    let primed_snapshot = client
        .status_snapshot_with_failure()
        .await
        .expect("primed live sink status snapshot");
    assert!(
        primed_snapshot
            .groups
            .iter()
            .filter(|group| {
                group.group_id == "nfs2"
                    || group.group_id == "nfs3"
                    || group.group_id == "nfs4"
            })
            .all(|group| group.is_ready() && group.live_nodes > 0),
        "precondition: nfs2/nfs3/nfs4 must be ready before retiring nfs3 from logical roots: {primed_snapshot:?}"
    );

    client
        .update_logical_roots_with_failure(
            vec![
                sink_worker_root("nfs2", &nfs2),
                sink_worker_root("nfs4", &nfs4),
            ],
            vec![
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                sink_worker_export("node-d::nfs4", "node-d", "10.0.0.44", nfs4.clone()),
            ],
        )
        .await
        .expect("retire nfs3 from logical roots");

    let cached = client
        .cached_status_snapshot_with_failure()
        .expect("cached status after update_logical_roots");
    let ready_groups = cached
        .groups
        .iter()
        .filter(|group| group.is_ready() && group.live_nodes > 0)
        .map(|group| group.group_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let scheduled_groups = cached
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !ready_groups.is_empty(),
        "retiring nfs3 must not clear all cached ready sink status for surviving groups: before={primed_snapshot:?} after={cached:?}"
    );
    assert_eq!(
        ready_groups,
        std::collections::BTreeSet::from(["nfs2".to_string(), "nfs4".to_string()]),
        "update_logical_roots must preserve only the surviving ready groups after nfs3 retire: before={primed_snapshot:?} after={cached:?}"
    );
    assert_eq!(
        scheduled_groups,
        std::collections::BTreeSet::from(["nfs2".to_string(), "nfs4".to_string()]),
        "update_logical_roots must republish only the surviving scheduled groups after nfs3 retire: before={primed_snapshot:?} after={cached:?}"
    );

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
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

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode sink activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), control_task)
        .await
        .expect("on_control_frame should reacquire a live sink worker client after handoff")
        .expect("join on_control_frame task")
        .expect("sink on_control_frame after worker restart");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after sink handoff retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_and_transport_closes_mid_handoff()
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

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_wave = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge after first wave");
    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker after first-wave success");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
        .await
        .expect("second sink control wave should reacquire a live worker client after reset")
        .expect("join second sink control wave")
        .expect("second sink control wave after worker restart");

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
            "scheduled sink groups should remain converged after second-wave retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close sink worker");
}
