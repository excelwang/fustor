#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_only_sink_status_route_republishes_after_cleanup_only_source_tail_before_later_node_c_four_envelope_source_reactivation()
 {
    struct SourceControlErrorHookReset;
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct SourceWorkerControlFramePauseHookReset;

    impl Drop for SourceControlErrorHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_control_frame_error_hook();
        }
    }

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    impl Drop for SourceWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_control_frame_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    fs::write(nfs1.join("ready-a.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("ready-b.txt"), b"b").expect("seed nfs2");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
    fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_watch_scan_root("nfs1", &nfs1),
                        worker_watch_scan_root("nfs2", &nfs2),
                    ],
                    host_object_grants: Vec::new(),
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-c-peer-cleanup-tail-sink-status".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let source_client = match &*app.source {
        SourceFacade::Worker(client) => client.clone(),
        SourceFacade::Local(_) => panic!("expected external source worker client"),
    };

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[("nfs1", &["nfs1"][..]), ("nfs2", &["nfs2"][..])];
        vec![
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
        ]
    };
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    let source_find_route = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);
    let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    initial.extend([
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_query_proxy_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
    ]);
    app.on_control_frame(&initial)
        .await
        .expect("initial peer source/status wave should succeed");

    let _source_error_reset = SourceControlErrorHookReset;
    crate::workers::source::install_source_worker_control_frame_error_hook(
        crate::workers::source::SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated peer source cleanup-tail failure".to_string(),
            ),
        },
    );

    app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        3,
        "restart_deferred_retire_pending",
        7,
        11,
        22,
    )])
    .await
    .expect(
        "peer source-only deactivate should fail-close into uninitialized replay-required recovery",
    );
    crate::workers::source::clear_source_worker_control_frame_error_hook();
    assert!(!app.control_initialized());

    let request_result = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(250),
        Duration::from_millis(50),
    )
    .await;
    assert!(
        request_result.is_err(),
        "peer-facing sink-status must fail closed while the source cleanup-tail recovery is still uninitialized"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let cleanup_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
            ])
            .await
        }
    });

    tokio::time::timeout(Duration::from_millis(600), entered.notified())
        .await
        .expect_err(
            "cleanup-only source tail must stay cleanup-only and must not re-enter source worker",
        );
    release.notify_waiters();
    cleanup_task
        .await
        .expect("join cleanup-only source tail")
        .expect("cleanup-only source tail should settle while runtime remains uninitialized");
    crate::workers::source::clear_source_worker_control_frame_pause_hook();

    app.on_control_frame(&[
        deactivate_envelope_with_route_key(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            3,
        ),
        deactivate_envelope_with_route_key(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            3,
        ),
    ])
    .await
    .expect("query-peer source cleanup routes should deactivate while runtime stays uninitialized");

    let previous_instance_id = source_client.worker_instance_id_for_tests().await;
    let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    crate::workers::source::install_source_worker_scheduled_groups_refresh_error_queue_hook(
        crate::workers::source::SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    let mut later = source_wave(4);
    later.extend([
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_query_proxy_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
    ]);
    tokio::time::timeout(Duration::from_secs(5), app.on_control_frame(&later))
            .await
            .expect(
                "later node-c four-envelope source reactivation should settle after the cleanup-only tail",
            )
            .expect(
                "later node-c four-envelope source reactivation should not exhaust runtime-app source recovery after the cleanup-only tail",
            );

    let local_sink_snapshot = tokio::time::timeout(
            Duration::from_secs(2),
            app.sink.status_snapshot_nonblocking(),
        )
        .await
        .expect("local sink status must settle promptly after later node-c four-envelope source reactivation")
        .expect("local sink status should be republished before peer sink-status route resumes");
    let local_ready_groups = local_sink_snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        local_ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "later node-c four-envelope source reactivation must restore local sink-status readiness before peer sink-status resumes: {local_sink_snapshot:?}"
    );

    let sink_status_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_secs(2),
        Duration::from_millis(100),
    )
    .await
    .expect("peer-facing sink-status after later node-c four-envelope source reactivation");
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
                .filter(|group| group.initial_audit_completed)
                .map(|group| group.group_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            ready_groups == std::collections::BTreeSet::from(["nfs1", "nfs2"])
        }),
        "peer-facing sink-status must republish ready groups after later node-c four-envelope source reactivation: {sink_status_snapshots:?}"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_only_sink_status_route_triggers_source_rescan_before_sink_readiness_check_after_cleanup_only_source_tail()
 {
    struct SourceControlErrorHookReset;
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct SourceWorkerControlFramePauseHookReset;
    struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

    impl Drop for SourceControlErrorHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_control_frame_error_hook();
        }
    }

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    impl Drop for SourceWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_control_frame_pause_hook();
        }
    }

    impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_trigger_rescan_when_ready_call_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
    fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");

    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![
                        worker_watch_scan_root("nfs1", &nfs1),
                        worker_watch_scan_root("nfs2", &nfs2),
                    ],
                    host_object_grants: Vec::new(),
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-c-peer-cleanup-tail-sink-status".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let source_client = match &*app.source {
        SourceFacade::Worker(client) => client.clone(),
        SourceFacade::Local(_) => panic!("expected external source worker client"),
    };

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[("nfs1", &["nfs1"][..]), ("nfs2", &["nfs2"][..])];
        vec![
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
        ]
    };
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    let source_find_route = format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL);
    let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    initial.extend([
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_query_proxy_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            2,
        ),
    ]);
    app.on_control_frame(&initial)
        .await
        .expect("initial peer source/status wave should succeed");

    let _source_error_reset = SourceControlErrorHookReset;
    crate::workers::source::install_source_worker_control_frame_error_hook(
        crate::workers::source::SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation(
                "simulated peer source cleanup-tail failure".to_string(),
            ),
        },
    );

    app.on_control_frame(&[deactivate_envelope_with_route_key_reason_and_lease(
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        3,
        "restart_deferred_retire_pending",
        7,
        11,
        22,
    )])
    .await
    .expect(
        "peer source-only deactivate should fail-close into uninitialized replay-required recovery",
    );
    crate::workers::source::clear_source_worker_control_frame_error_hook();
    assert!(!app.control_initialized());

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let cleanup_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
                deactivate_envelope_with_route_key_reason_and_lease(
                    execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                    format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                    3,
                    "restart_deferred_retire_pending",
                    7,
                    11,
                    22,
                ),
            ])
            .await
        }
    });

    tokio::time::timeout(Duration::from_millis(600), entered.notified())
        .await
        .expect_err(
            "cleanup-only source tail must stay cleanup-only and must not re-enter source worker",
        );
    release.notify_waiters();
    cleanup_task
        .await
        .expect("join cleanup-only source tail")
        .expect("cleanup-only source tail should settle while runtime remains uninitialized");
    crate::workers::source::clear_source_worker_control_frame_pause_hook();

    app.on_control_frame(&[
        deactivate_envelope_with_route_key(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            3,
        ),
        deactivate_envelope_with_route_key(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            3,
        ),
    ])
    .await
    .expect("query-peer source cleanup routes should deactivate while runtime stays uninitialized");

    let previous_instance_id = source_client.worker_instance_id_for_tests().await;
    let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    crate::workers::source::install_source_worker_scheduled_groups_refresh_error_queue_hook(
        crate::workers::source::SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );
    let trigger_rescan_count = Arc::new(AtomicUsize::new(0));
    let _trigger_rescan_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    crate::workers::source::install_source_worker_trigger_rescan_when_ready_call_count_hook(
        crate::workers::source::SourceWorkerTriggerRescanWhenReadyCallCountHook {
            count: trigger_rescan_count.clone(),
        },
    );
    let post_return_retrigger_entered = Arc::new(Notify::new());
    let post_return_retrigger_release = Arc::new(Notify::new());
    let _post_return_retrigger_reset = LocalSinkStatusRepublishRetriggerPauseHookReset;
    install_local_sink_status_republish_retrigger_pause_hook(
        LocalSinkStatusRepublishRetriggerPauseHook {
            entered: post_return_retrigger_entered.clone(),
            release: post_return_retrigger_release.clone(),
        },
    );
    let mut later = source_wave(4);
    later.extend([
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_find_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_status_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_query_proxy_route.clone(),
            &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
            4,
        ),
    ]);
    let later_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&later).await }
    });
    let later_result = tokio::time::timeout(Duration::from_secs(5), later_task)
            .await
            .expect(
                "later node-c four-envelope source reactivation should settle after the cleanup-only tail",
            )
            .expect("join later node-c four-envelope source reactivation");

    match later_result {
        Ok(()) => {
            assert!(
                trigger_rescan_count.load(Ordering::SeqCst) >= 1,
                "runtime-app later source-only recovery must trigger source->sink convergence before republishing sink status"
            );
        }
        Err(err) => {
            let trigger_count = trigger_rescan_count.load(Ordering::SeqCst);
            if trigger_count == 0 {
                panic!(
                    "runtime-app checked sink readiness after later source-only recovery without ever triggering source->sink convergence: {err}"
                );
            }
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups after later source-only recovery")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups after later source-only recovery")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups after later source-only recovery")
                .unwrap_or_default();
            if source_groups.is_empty()
                || source_groups != scan_groups
                || source_groups != sink_groups
            {
                assert!(
                    err.to_string()
                        .contains("runtime scope convergence not observed after retained replay"),
                    "runtime-app triggered source->sink convergence {trigger_count} time(s) but still collapsed missing runtime scope convergence into a sink-readiness failure: {err}; source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
                );
                app.close().await.expect("close app");
                return;
            }
            if err
                .to_string()
                .contains("source convergence evidence not observed after retained replay")
            {
                panic!(
                    "runtime-app triggered source->sink convergence {trigger_count} time(s) and observed runtime scope convergence source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}, but still kept waiting for nonexistent source publish evidence: {err}"
                );
            }
            if err.to_string().contains(
                    "sink status readiness not restored after retained replay once runtime scope converged"
                ) {
                    let source_status_active = app
                        .facade_gate
                        .route_active(
                            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                            &source_status_route,
                        )
                        .expect("query-peer source-status route state after later source-only recovery error");
                    let source_find_active = app
                        .facade_gate
                        .route_active(
                            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                            &source_find_route,
                        )
                        .expect("query-peer source-find route state after later source-only recovery error");
                    let sink_status_active = app
                        .facade_gate
                        .route_active(
                            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                            &sink_status_route,
                        )
                        .expect("query-peer sink-status route state after later source-only recovery error");
                    let sink_query_proxy_active = app
                        .facade_gate
                        .route_active(
                            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                            &sink_query_proxy_route,
                        )
                        .expect("query-peer sink-query-proxy route state after later source-only recovery error");
                    if !source_status_active || !source_find_active {
                        panic!(
                            "later source-only recovery may keep source-owned peer routes live while sink readiness is still restoring; source_status_active={source_status_active} source_find_active={source_find_active} err={err}"
                        );
                    }
                    assert!(
                        !sink_status_active && !sink_query_proxy_active,
                        "later source-only recovery must keep sink-owned peer routes suppressed until local sink status republish succeeds; sink_status_active={sink_status_active} sink_query_proxy_active={sink_query_proxy_active} err={err}"
                    );
                    app.close().await.expect("close app");
                    return;
                }
            if matches!(err, CnxError::Timeout) {
                if trigger_count < 2 {
                    panic!(
                        "runtime-app must retrigger source->sink convergence after a post-recovery sink timeout once runtime scope has already converged; triggers={trigger_count} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
                    );
                }
                panic!(
                    "runtime-app retriggered source->sink convergence {trigger_count} time(s) after runtime scope convergence but still exhausted sink readiness on a raw timeout: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
                );
            }
            panic!(
                "runtime-app triggered source->sink convergence {trigger_count} time(s) but returned an unexpected recovery error after runtime scope convergence source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}: {err}"
            );
        }
    }

    let trigger_count_after_return = trigger_rescan_count.load(Ordering::SeqCst);
    let expected_local_ready_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let immediate_local_sink_snapshot = tokio::time::timeout(
        Duration::from_millis(250),
        app.sink.status_snapshot_nonblocking(),
    )
    .await;
    let immediate_local_sink_ready = matches!(
        &immediate_local_sink_snapshot,
        Ok(Ok(snapshot))
            if snapshot
                .groups
                .iter()
                .filter(|group| group.initial_audit_completed)
                .map(|group| group.group_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                == expected_local_ready_groups
    );
    let (local_sink_snapshot, trigger_count_after_local_wait) = if immediate_local_sink_ready {
        (
            immediate_local_sink_snapshot
                .expect("immediate local sink snapshot timeout already excluded")
                .expect("immediate local sink status err already excluded"),
            trigger_count_after_return,
        )
    } else {
        tokio::time::timeout(Duration::from_secs(2), post_return_retrigger_entered.notified())
                .await
                .expect(
                    "runtime-app must either restore local sink readiness before return or spawn a deferred post-return local sink-status republish helper that reaches its post-return source->sink retrigger after later source-only recovery returned",
                );
        post_return_retrigger_release.notify_waiters();
        let local_sink_snapshot = tokio::time::timeout(
            Duration::from_secs(2),
            app.sink.status_snapshot_nonblocking(),
        )
        .await;
        let trigger_count_after_local_wait = trigger_rescan_count.load(Ordering::SeqCst);
        let local_sink_snapshot = match local_sink_snapshot {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(err)) => {
            let sink_groups_after_local_wait = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups while local sink status republish remains unavailable")
                .unwrap_or_default();
            let cached_sink_status_summary = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|cached_err| {
                    format!("cached_sink_status_unavailable err={cached_err}")
                });
            let blocking_sink_status_summary = match tokio::time::timeout(
                Duration::from_secs(2),
                app.sink.status_snapshot(),
            )
            .await
            {
                Ok(Ok(snapshot)) => summarize_sink_status_endpoint(&snapshot),
                Ok(Err(snapshot_err)) => format!("blocking_status_err={snapshot_err}"),
                Err(_) => "blocking_status_timeout".to_string(),
            };
            let source_observability_summary = summarize_source_observability_endpoint(
                &app.source.observability_snapshot_nonblocking().await,
            );
            let sink_status_active = app
                    .facade_gate
                    .route_active(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        &sink_status_route,
                    )
                    .expect("query-peer sink-status route state while local sink status republish remains unavailable");
            let sink_query_proxy_active = app
                    .facade_gate
                    .route_active(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        &sink_query_proxy_route,
                    )
                    .expect("query-peer sink-query-proxy route state while local sink status republish remains unavailable");
            assert!(
                !sink_status_active && !sink_query_proxy_active,
                "runtime-app must keep sink-owned peer routes suppressed while post-return local sink-status republish is still unavailable; sink_status_active={sink_status_active} sink_query_proxy_active={sink_query_proxy_active} sink_groups_after_local_wait={sink_groups_after_local_wait:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary} err={err}"
            );
            panic!(
                "runtime-app kept sink-owned peer routes suppressed after later source-only recovery returned, but local sink status still failed before republish: trigger_count_after_return={trigger_count_after_return} trigger_count_after_local_wait={trigger_count_after_local_wait} sink_groups_after_local_wait={sink_groups_after_local_wait:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary} err={err}"
            );
        }
        Err(_) => {
            let sink_groups_after_local_wait = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups while local sink status republish timed out")
                .unwrap_or_default();
            let cached_sink_status_summary = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|cached_err| {
                    format!("cached_sink_status_unavailable err={cached_err}")
                });
            let blocking_sink_status_summary = match tokio::time::timeout(
                Duration::from_secs(2),
                app.sink.status_snapshot(),
            )
            .await
            {
                Ok(Ok(snapshot)) => summarize_sink_status_endpoint(&snapshot),
                Ok(Err(snapshot_err)) => format!("blocking_status_err={snapshot_err}"),
                Err(_) => "blocking_status_timeout".to_string(),
            };
            let source_observability_summary = summarize_source_observability_endpoint(
                &app.source.observability_snapshot_nonblocking().await,
            );
            let sink_status_active = app
                    .facade_gate
                    .route_active(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        &sink_status_route,
                    )
                    .expect("query-peer sink-status route state while local sink status republish timed out");
            let sink_query_proxy_active = app
                    .facade_gate
                    .route_active(
                        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                        &sink_query_proxy_route,
                    )
                    .expect("query-peer sink-query-proxy route state while local sink status republish timed out");
            assert!(
                !sink_status_active && !sink_query_proxy_active,
                "runtime-app must keep sink-owned peer routes suppressed while post-return local sink-status republish timed out; sink_status_active={sink_status_active} sink_query_proxy_active={sink_query_proxy_active} sink_groups_after_local_wait={sink_groups_after_local_wait:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}"
            );
            panic!(
                "runtime-app kept sink-owned peer routes suppressed after later source-only recovery returned, but local sink status still did not republish within the bounded wait: trigger_count_after_return={trigger_count_after_return} trigger_count_after_local_wait={trigger_count_after_local_wait} sink_groups_after_local_wait={sink_groups_after_local_wait:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}"
            );
        }
        };
        (local_sink_snapshot, trigger_count_after_local_wait)
    };
    let local_ready_groups = local_sink_snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        local_ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "runtime-app must restore local sink-status readiness after later source-only recovery returned: {local_sink_snapshot:?}"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sink_status_route_reuses_cached_ready_snapshot_when_replay_required_probe_reports_not_ready_after_retry_reset()
{
    struct SinkWorkerControlFrameErrorQueueHookReset;
    struct SinkWorkerStatusSnapshotHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_control_frame_error_hook();
        }
    }

    impl Drop for SinkWorkerStatusSnapshotHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_status_snapshot_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    fs::write(nfs2.join("ready-b.txt"), b"b").expect("seed nfs2");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");
    fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
    let nfs2_source = nfs2.display().to_string();

    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            FSMetaConfig {
                source: SourceConfig {
                    roots: vec![worker_fs_watch_scan_root("nfs2", &nfs2_source)],
                    host_object_grants: vec![worker_export_with_fs_source(
                        "node-a::nfs2",
                        "node-a",
                        "10.0.0.12",
                        &nfs2_source,
                        nfs2.clone(),
                    )],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[("nfs2", &["node-a::nfs2"][..])];
        vec![
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
        ]
    };
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    initial.push(activate_envelope_with_route_key_and_scope_rows(
        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        sink_status_route.clone(),
        &[("nfs2", &["listener-a"])],
        2,
    ));
    app.on_control_frame(&initial)
        .await
        .expect("initial source/sink + sink-status route wave should succeed");

    app.sink
        .send(&[
            mk_source_event("node-a::nfs2", b"/ready-b.txt", b"ready-b.txt", 11),
            mk_control_event(
                "node-a::nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: crate::EpochType::Audit,
                },
                12,
            ),
        ])
        .await
        .expect("seed nfs2 sink state");

    let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let snapshot = match app.sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => {
                assert!(
                    tokio::time::Instant::now() < readiness_deadline,
                    "precondition: local sink status must become ready before forcing replay-required route fallback"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(err) => panic!("prime ready local sink status: {err}"),
        };
        if snapshot.groups.iter().any(|group| {
            group.group_id == "nfs2" && group.initial_audit_completed && group.total_nodes > 0
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < readiness_deadline,
            "precondition: nfs2 must become ready before forcing replay-required route fallback: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _sink_error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    crate::workers::sink::install_sink_worker_control_frame_error_queue_hook(
        crate::workers::sink::SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::Internal(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            )]),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let mut followup = source_wave(3);
    followup.extend(sink_wave(3));
    followup.push(activate_envelope_with_route_key_and_scope_rows(
        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        sink_status_route.clone(),
        &[("nfs2", &["listener-a"])],
        3,
    ));
    app.on_control_frame(&followup)
        .await
        .expect("follow-up sink wave should settle after one retry reset");

    let _status_snapshot_reset = SinkWorkerStatusSnapshotHookReset;
    crate::workers::sink::install_sink_worker_status_snapshot_hook(
        crate::workers::sink::SinkWorkerStatusSnapshotHook {
            snapshot: crate::sink::SinkStatusSnapshot {
                scheduled_groups_by_node: std::collections::BTreeMap::from([(
                    "node-a".to_string(),
                    vec!["nfs2".to_string()],
                )]),
                groups: vec![crate::sink::SinkGroupStatusSnapshot {
                    group_id: "nfs2".to_string(),
                    primary_object_ref: "node-a::nfs2".to_string(),
                    total_nodes: 0,
                    live_nodes: 0,
                    tombstoned_count: 0,
                    attested_count: 0,
                    suspect_count: 0,
                    blind_spot_count: 0,
                    shadow_time_us: 0,
                    shadow_lag_us: 0,
                    overflow_pending_audit: false,
                    initial_audit_completed: false,
                    materialized_revision: 1,
                    estimated_heap_bytes: 0,
                }],
                ..crate::sink::SinkStatusSnapshot::default()
            },
        },
    );

    let sink_status_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(250),
        Duration::from_millis(50),
    )
    .await
    .expect(
        "sink-status route should reuse the cached ready local sink snapshot when the replay-required live probe reports a transient not-ready snapshot after a retry reset",
    );
    let sink_status_snapshots = sink_status_events
        .into_iter()
        .map(|event| {
            rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                .expect("decode sink-status snapshot")
        })
        .collect::<Vec<_>>();
    assert!(
        sink_status_snapshots.iter().any(|snapshot| {
            snapshot.groups.iter().any(|group| {
                group.group_id == "nfs2" && group.initial_audit_completed && group.total_nodes > 0
            })
        }),
        "sink-status route must preserve the cached ready nfs2 snapshot instead of timing out on a replay-required transient not-ready probe: {sink_status_snapshots:?}"
    );

    app.close().await.expect("close app");
}
