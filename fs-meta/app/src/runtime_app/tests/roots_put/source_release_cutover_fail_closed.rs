async fn source_release_cutover_hook_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static CELL: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    CELL.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

struct SinkWorkerControlFrameErrorQueueHookReset;

impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
    fn drop(&mut self) {
        crate::workers::sink::clear_sink_worker_control_frame_error_hook();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_sink_replay_after_source_repair_is_lane_scoped_and_clears_on_recovery() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let source_roots_route = format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL);
    let source_wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            source_roots_route.clone(),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    let sink_wave = vec![
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ];
    let mut initial = source_wave.clone();
    initial.extend(sink_wave);
    app.on_control_frame(&initial)
        .await
        .expect("initial source/sink wave should succeed");
    assert!(app.control_initialized());

    app.update_runtime_control_state_for_tests(|state| {
        *state = RuntimeControlState::live(true, true);
    });

    app.on_control_frame(&[tick_envelope_with_route_key(
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        source_roots_route,
        2,
    )])
    .await
    .expect("source replay repair followed by deferred sink replay should complete");

    assert!(
        !app.source_state_replay_required(),
        "deferring retained sink replay after source repair must not re-arm already-cleared source replay",
    );
    assert!(
        app.sink_state_replay_required(),
        "sink replay must remain pending for its bounded recovery lane",
    );

    let sink_exposure_confirm = encode_runtime_unit_exposure(&RuntimeUnitExposure {
        route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
        unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
        generation: 2,
        confirmed_at_us: 1,
    })
    .expect("encode sink exposure confirmation");
    app.on_control_frame(&[sink_exposure_confirm])
        .await
        .expect("runtime control-readiness probe should run bounded retained sink replay recovery before returning not-ready");

    assert!(
        !app.sink_state_replay_required(),
        "successful bounded recovery must clear the sink replay requirement instead of leaving empty/tick frames to repeatedly defer it",
    );
    assert!(
        !app.source_state_replay_required(),
        "sink replay recovery must not re-arm already-cleared source replay",
    );
    assert!(
        app.control_initialized(),
        "bounded recovery should reopen runtime control after source and sink replay are clear",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_source_generation_cutover_arms_worker_replay_obligation() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let source_roots_route = format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL);
    let source_wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            source_roots_route.clone(),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    let source_signals = crate::runtime::orchestration::source_control_signals_from_envelopes(
        &source_wave,
    )
    .expect("decode source cutover wave");

    assert!(
        !app.source.retained_replay_required().await,
        "precondition: worker retained source replay starts clear",
    );
    app.defer_source_generation_cutover_replay_inline(&source_signals)
        .await
        .expect("defer source generation cutover replay");

    assert!(
        app.source.retained_replay_required().await,
        "deferred source generation cutover must arm worker retained replay; otherwise source repair can clear app replay without applying retained source state",
    );
    let retained = app.source.retained_control_state_for_tests().await;
    assert!(
        retained.active_by_route.contains_key(&(
            execution_units::SOURCE_RUNTIME_UNIT_ID.to_string(),
            source_roots_route,
        )),
        "deferred source generation cutover must retain source route state for recovery",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_release_cutover_source_retryable_reset_fail_closes_without_inline_replay_blocking_quorum(
) {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let mut wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    wave.extend([
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ]);

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    let entered_source_worker = Arc::new(Notify::new());
    let release_source_worker = Arc::new(Notify::new());
    let _source_worker_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered_source_worker.clone(),
            release: release_source_worker.clone(),
        },
    );

    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&wave).await }
    });
    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task)
        .await
        .expect(
            "retryable source worker reset after state retention must return without inline source worker replay",
        )
        .expect("join apply task");
    apply_result
        .expect("retryable source worker reset after state retention should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "fail-closed source cutover must not re-enter source worker control inside the process-level apply/quorum path",
    );
    release_source_worker.notify_waiters();

    assert!(
        !app.control_initialized(),
        "mixed release cutover must not keep the process-level apply/quorum path waiting for inline source replay after a retryable source reset",
    );
    assert!(
        app.source_state_replay_required(),
        "fail-closed source cutover must preserve retained source replay for the next bounded recovery entrypoint",
    );
    let retained = app.source.retained_control_state_for_tests().await;
    assert!(
        retained.active_by_route.contains_key(&(
            execution_units::SOURCE_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL)
        )),
        "fail-closed source cutover must retain source roots activate state",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_only_release_cutover_source_retryable_reset_fail_closes_without_inline_replay_blocking_quorum(
) {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    let entered_source_worker = Arc::new(Notify::new());
    let release_source_worker = Arc::new(Notify::new());
    let _source_worker_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered_source_worker.clone(),
            release: release_source_worker.clone(),
        },
    );

    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&wave).await }
    });
    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task)
        .await
        .expect(
            "source-only post-initial release cutover reset must return without inline replay",
        )
        .expect("join apply task");
    apply_result.expect("source-only post-initial release cutover reset should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "source-only post-initial release cutover must not re-enter source worker control inside the process-level apply/quorum path",
    );
    release_source_worker.notify_waiters();

    assert!(
        !app.control_initialized(),
        "source-only release cutover must fail closed instead of spending the process-level apply/quorum path on inline source replay",
    );
    assert!(
        app.source_state_replay_required(),
        "source-only release cutover must preserve retained source replay for the next bounded recovery entrypoint",
    );
    let retained = app.source.retained_control_state_for_tests().await;
    assert!(
        retained.active_by_route.contains_key(&(
            execution_units::SOURCE_RUNTIME_UNIT_ID.to_string(),
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL)
        )),
        "source-only release cutover must retain source roots activate state",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_post_initial_source_replay_retryable_reset_fail_closes_without_inline_replay() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let retained_source = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    let (retained_source_signals, _, _) =
        split_app_control_signals(&retained_source).expect("split retained source signals");
    app.source
        .record_retained_control_signals(&retained_source_signals)
        .await;
    set_control_initialized_for_tests(&app, true);
    set_source_replay_required_for_tests(&app, true);

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    let entered_source_worker = Arc::new(Notify::new());
    let release_source_worker = Arc::new(Notify::new());
    let _source_worker_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered_source_worker.clone(),
            release: release_source_worker.clone(),
        },
    );

    let mut apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&[]).await }
    });
    let apply_result = match tokio::time::timeout(Duration::from_millis(800), &mut apply_task).await
    {
        Ok(joined) => joined.expect("join retained replay apply task"),
        Err(_) => {
            release_source_worker.notify_waiters();
            apply_task.abort();
            panic!(
                "retained post-initial source replay reset must return without inline source worker replay"
            );
        }
    };
    apply_result.expect("retained post-initial source replay should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "retained post-initial source replay must not re-enter source worker control inside the process-level apply/quorum path",
    );
    release_source_worker.notify_waiters();

    assert!(
        !app.control_initialized(),
        "retained post-initial source replay must fail closed instead of spending the process-level apply/quorum path on inline source replay",
    );
    assert!(
        app.source_state_replay_required(),
        "retained post-initial source replay must remain pending for bounded recovery",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_closed_release_cutover_keeps_source_status_recovery_lane_live() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let mut wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    wave.extend([
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ]);

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });

    app.on_control_frame(&wave)
        .await
        .expect("retryable source reset should fail closed inside app");

    assert!(
        !app.control_initialized(),
        "release cutover source reset must leave trusted runtime control closed",
    );
    assert!(
        app.source_state_replay_required(),
        "release cutover source reset must keep retained source replay pending",
    );

    let sink_exposure_confirm = encode_runtime_unit_exposure(&RuntimeUnitExposure {
        route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
        unit_id: execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
        generation: 2,
        confirmed_at_us: 1,
    })
    .expect("encode sink exposure confirmation");
    app.on_control_frame(&[sink_exposure_confirm])
        .await
        .expect("runtime control-readiness probe should run bounded retained source replay recovery before returning not-ready");
    assert!(
        !app.source_state_replay_required(),
        "runtime control-readiness recovery must clear retained source replay after replaying current-generation source state",
    );
    assert!(
        app.control_initialized(),
        "runtime control-readiness recovery should reopen runtime control after retained source replay clears",
    );

    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    assert!(
        app.runtime_endpoint_routes
            .lock()
            .await
            .contains(&source_status_route),
        "fail-closed release cutover must keep the source-status recovery lane live for the assigned source owner",
    );

    let status_task = tokio::spawn({
        let boundary = boundary.clone();
        async move {
            internal_source_status_snapshots_with_timeout(
                boundary,
                NodeId("node-a".into()),
                Duration::from_secs(12),
                Duration::from_millis(100),
            )
            .await
        }
    });
    let snapshots = tokio::time::timeout(Duration::from_secs(15), status_task)
        .await
        .expect("source status recovery request should not hang")
        .expect("join source status request")
        .expect("source status recovery request should complete");
    assert!(
        snapshots
            .iter()
            .any(|snapshot| !snapshot.source_primary_by_group.is_empty()),
        "source status recovery should return source-primary evidence after retained replay",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_rescan_source_status_does_not_repair_retained_source_replay_before_source_state_current(
) {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let mut wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    wave.extend([
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ]);

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });

    app.on_control_frame(&wave)
        .await
        .expect("retryable source reset should fail closed inside app");
    assert!(
        app.source_state_replay_required(),
        "precondition: release cutover source reset must leave retained source replay pending",
    );

    let adapter = crate::runtime::seam::exchange_host_adapter(
        boundary.clone(),
        NodeId("api-node".to_string()),
        crate::runtime::routes::default_route_bindings(),
    );
    let events = capanix_host_adapter_fs::HostAdapter::call_collect(
        &adapter,
        ROUTE_TOKEN_FS_META_INTERNAL,
        METHOD_SOURCE_STATUS,
        query::api::manual_rescan_source_status_request_payload(),
        Duration::from_secs(12),
        Duration::from_millis(100),
    )
    .await
    .expect("manual-rescan source-status should return observation without retained replay repair");
    let snapshots = events
        .iter()
        .map(|event| {
            rmp_serde::from_slice::<crate::workers::source::SourceObservabilitySnapshot>(
                event.payload_bytes(),
            )
            .expect("decode source observability snapshot")
        })
        .collect::<Vec<_>>();
    assert!(
        snapshots
            .iter()
            .any(|snapshot| snapshot
                .scheduled_source_groups_by_node
                .values()
                .any(|groups| groups.iter().any(|group| group == "nfs1"))
                && snapshot
                    .scheduled_source_groups_by_node
                    .values()
                    .any(|groups| groups.iter().any(|group| group == "nfs2"))),
        "manual-rescan source-status should still report runtime-scope ownership from control/cache while source replay is pending: {snapshots:?}",
    );
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.source_primary_by_group.is_empty()),
        "manual-rescan source-status must not report live delivery target evidence while source replay is pending: {snapshots:?}",
    );
    assert!(
        app.source_state_replay_required(),
        "manual-rescan source-status must not clear retained source replay from a status read",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_cleanup_source_retire_with_sink_tick_fails_closed_without_inline_replay() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let retained_source = [
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    let (retained_source_signals, _, _) =
        split_app_control_signals(&retained_source).expect("split retained source signals");
    app.source
        .record_retained_control_signals(&retained_source_signals)
        .await;
    app.update_runtime_control_state_for_tests(|state| {
        *state = RuntimeControlState::live(false, false);
    });

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    let entered_source_worker = Arc::new(Notify::new());
    let release_source_worker = Arc::new(Notify::new());
    let _source_worker_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered_source_worker.clone(),
            release: release_source_worker.clone(),
        },
    );

    let cleanup_wakeup = [
        deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
            "restart_deferred_retire_pending",
            2,
            10,
            60_000,
        ),
        tick_envelope_with_route_key(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            2,
        ),
    ];
    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&cleanup_wakeup).await }
    });
    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task).await;
    if apply_result.is_err() {
        release_source_worker.notify_waiters();
        panic!(
            "mixed cleanup source retire plus sink tick must fail closed without inline source replay"
        );
    }
    apply_result
        .expect("mixed cleanup should return within process apply budget")
        .expect("join mixed cleanup")
        .expect("mixed cleanup retryable reset should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "mixed cleanup must not re-enter source worker inline after retryable reset"
    );
    assert!(
        !app.control_initialized(),
        "mixed cleanup retryable reset must close the runtime control gate"
    );
    assert!(
        app.source_state_replay_required(),
        "mixed cleanup retryable reset must preserve source replay for the recovery lane"
    );
    release_source_worker.notify_waiters();

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_cleanup_sink_tick_replay_reset_fails_closed_without_inline_replay() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let retained_sink = [
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ];
    let (_, retained_sink_signals, _) =
        split_app_control_signals(&retained_sink).expect("split retained sink signals");
    app.record_retained_sink_control_state(&retained_sink_signals)
        .await;
    app.update_runtime_control_state_for_tests(|state| {
        *state = RuntimeControlState::live(false, true);
    });

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    let _sink_error_reset = SinkWorkerControlFrameErrorQueueHookReset;
    crate::workers::sink::install_sink_worker_control_frame_error_queue_hook(
        crate::workers::sink::SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let cleanup_wakeup = [
        deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
            "restart_deferred_retire_pending",
            2,
            10,
            60_000,
        ),
        tick_envelope_with_route_key(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            2,
        ),
    ];
    let mut apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&cleanup_wakeup).await }
    });
    let apply_result = tokio::time::timeout(Duration::from_millis(800), &mut apply_task).await;
    if apply_result.is_err() {
        apply_task.abort();
        panic!(
            "mixed cleanup sink retryable reset must fail closed without inline sink replay retries"
        );
    }
    apply_result
        .expect("mixed cleanup sink reset should return within process apply budget")
        .expect("join mixed cleanup sink reset")
        .expect("mixed cleanup sink retryable reset should fail closed in-app");

    assert!(
        !app.control_initialized(),
        "mixed cleanup sink retryable reset must close the runtime control gate"
    );
    assert!(
        app.sink_state_replay_required(),
        "mixed cleanup sink retryable reset must preserve sink replay for the recovery lane"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_grant_wakeup_of_retained_source_cutover_replay_fails_closed_without_inline_retry() {
    let _hook_guard = source_release_cutover_hook_guard().await;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = nfs1.display().to_string();
    let nfs2_source = nfs2.display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let app = Arc::new(
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
                            "127.0.0.11",
                            &nfs1_source,
                            nfs1.clone(),
                        ),
                        worker_export_with_fs_source(
                            "node-a::nfs2",
                            "node-a",
                            "127.0.0.12",
                            &nfs2_source,
                            nfs2.clone(),
                        ),
                    ],
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
        .expect("init external-worker app"),
    );

    let source_scopes = &[
        ("nfs1", &["node-a::nfs1"][..]),
        ("nfs2", &["node-a::nfs2"][..]),
    ];
    let mut release_wave = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_scopes,
            2,
        ),
    ];
    release_wave.extend([
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, source_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            source_scopes,
            2,
        ),
    ]);

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });
    app.on_control_frame(&release_wave)
        .await
        .expect("initial source cutover reset should fail closed in-app");
    assert!(
        app.source_state_replay_required(),
        "precondition: release cutover reset must leave retained source replay pending"
    );

    let entered_source_worker = Arc::new(Notify::new());
    let release_source_worker = Arc::new(Notify::new());
    let _source_worker_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: entered_source_worker.clone(),
            release: release_source_worker.clone(),
        },
    );

    let tick_wakeup = [tick_envelope_with_route_key(
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        2,
    )];
    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&tick_wakeup).await }
    });

    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task).await;
    if apply_result.is_err() {
        release_source_worker.notify_waiters();
        panic!(
            "tick wakeup of retained source cutover replay must return without inline source worker retry"
        );
    }
    apply_result
        .expect("tick wakeup should return within process apply budget")
        .expect("join tick wakeup")
        .expect("tick wakeup retryable reset should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "tick wakeup of retained source replay must not enter source worker inline"
    );
    assert!(
        app.source_state_replay_required(),
        "tick wakeup must preserve retained source replay for the recovery lane"
    );

    let cleanup_wakeup = [
        deactivate_envelope_with_route_key_reason_and_lease(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
            "restart_deferred_retire_pending",
            2,
            10,
            60_000,
        ),
        tick_envelope_with_route_key(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            2,
        ),
    ];
    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&cleanup_wakeup).await }
    });

    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task).await;
    if apply_result.is_err() {
        release_source_worker.notify_waiters();
        panic!(
            "cleanup wakeup of retained source cutover replay must return without inline source worker retry"
        );
    }
    apply_result
        .expect("cleanup wakeup should return within process apply budget")
        .expect("join cleanup wakeup")
        .expect("cleanup wakeup retryable reset should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "cleanup wakeup of retained source replay must not enter source worker inline"
    );
    assert!(
        app.source_state_replay_required(),
        "cleanup wakeup must preserve retained source replay for the recovery lane"
    );

    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
    });

    let host_grant_wakeup = [host_object_grants_changed_rows_envelope(
        2,
        &[
            (
                "node-a::nfs1",
                "node-a",
                "127.0.0.11",
                &nfs1_source,
                &nfs1_source,
                true,
            ),
            (
                "node-a::nfs2",
                "node-a",
                "127.0.0.12",
                &nfs2_source,
                &nfs2_source,
                true,
            ),
        ],
    )];
    let apply_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&host_grant_wakeup).await }
    });

    let apply_result = tokio::time::timeout(Duration::from_millis(800), apply_task).await;
    if apply_result.is_err() {
        release_source_worker.notify_waiters();
        panic!(
            "host-grant wakeup of retained source cutover replay must return without inline source worker retry"
        );
    }
    apply_result
        .expect("host-grant wakeup should return within process apply budget")
        .expect("join host-grant wakeup")
        .expect("host-grant wakeup retryable reset should fail closed in-app");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_source_worker.notified())
            .await
            .is_err(),
        "host-grant wakeup must not re-enter source worker control inside the process-level apply/quorum path"
    );
    release_source_worker.notify_waiters();
    assert!(
        app.source_state_replay_required(),
        "host-grant wakeup must preserve retained source replay for the recovery lane"
    );

    app.close().await.expect("close app");
}
