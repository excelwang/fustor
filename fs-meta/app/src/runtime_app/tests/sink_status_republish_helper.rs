#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_sink_status_republish_after_recovery_restores_ready_groups_after_cleanup_only_source_tail()
 {
    struct SourceControlErrorHookReset;
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    struct SinkWorkerControlFramePauseHookReset;

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

    impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_trigger_rescan_when_ready_call_count_hook();
        }
    }

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
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
            NodeId("node-c-local-sink-status-helper".into()),
            Some(boundary.clone()),
            Some(boundary),
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
    let initial_trigger_rescan_count = Arc::new(AtomicUsize::new(0));
    let _trigger_rescan_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    crate::workers::source::install_source_worker_trigger_rescan_when_ready_call_count_hook(
        crate::workers::source::SourceWorkerTriggerRescanWhenReadyCallCountHook {
            count: initial_trigger_rescan_count.clone(),
        },
    );

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");
    let initial_ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout(
            Duration::from_millis(350),
            app.sink.status_snapshot_nonblocking(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.initial_audit_completed)
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups
                    == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
                {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }
        if tokio::time::Instant::now() >= initial_ready_deadline {
            app.source
                .trigger_rescan_when_ready()
                .await
                .expect("direct trigger_rescan_when_ready after failed deferred initial trigger");
            let direct_trigger_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let mut direct_trigger_restored_ready = false;
            let mut latest_source_observability =
                app.source.observability_snapshot_nonblocking().await;
            let mut latest_cached_sink_status = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
            while tokio::time::Instant::now() < direct_trigger_deadline {
                latest_source_observability = app.source.observability_snapshot_nonblocking().await;
                latest_cached_sink_status = app
                    .sink
                    .cached_status_snapshot()
                    .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                    .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
                if latest_source_observability
                    .published_batches_by_node
                    .values()
                    .copied()
                    .sum::<u64>()
                    > 0
                {
                    direct_trigger_restored_ready = true;
                    break;
                }
                match tokio::time::timeout(
                    Duration::from_millis(200),
                    app.sink.status_snapshot_nonblocking(),
                )
                .await
                {
                    Ok(Ok(snapshot)) => {
                        let ready_groups = snapshot
                            .groups
                            .iter()
                            .filter(|group| group.initial_audit_completed)
                            .map(|group| group.group_id.clone())
                            .collect::<std::collections::BTreeSet<_>>();
                        if ready_groups
                            == std::collections::BTreeSet::from([
                                "nfs1".to_string(),
                                "nfs2".to_string(),
                            ])
                        {
                            direct_trigger_restored_ready = true;
                            break;
                        }
                    }
                    Ok(Err(_)) | Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups before cleanup-tail precondition failure")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups before cleanup-tail precondition failure")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups before cleanup-tail precondition failure")
                .unwrap_or_default();
            let cached_sink_status = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
            let source_observability =
                summarize_source_observability_endpoint(&latest_source_observability);
            let initial_trigger_count = initial_trigger_rescan_count.load(Ordering::SeqCst);
            panic!(
                "precondition: cleanup-only source-tail republish seam requires the local sink to materialize both nfs1/nfs2 before the fail-closed recovery begins; initial_trigger_count={initial_trigger_count} direct_trigger_restored_ready={direct_trigger_restored_ready} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status_before_direct_trigger={cached_sink_status} cached_sink_status_after_direct_trigger={latest_cached_sink_status} source_observability={source_observability}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _source_error_reset = SourceControlErrorHookReset;
    crate::workers::source::install_source_worker_control_frame_error_hook(
        crate::workers::source::SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation("simulated source cleanup-tail failure".to_string()),
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
    .expect("source-only deactivate should fail-close into uninitialized replay-required recovery");
    crate::workers::source::clear_source_worker_control_frame_error_hook();
    assert!(!app.control_initialized());

    let previous_instance_id = source_client.worker_instance_id_for_tests().await;

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
    .expect("cleanup-only source tail should settle while runtime remains uninitialized");

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
    let trigger_rescan_count = initial_trigger_rescan_count.clone();

    let later = source_wave(4);
    tokio::time::timeout(Duration::from_secs(5), app.on_control_frame(&later))
        .await
        .expect("later source-only recovery should settle after the cleanup-only tail")
        .expect("later source-only recovery should not exhaust runtime-app source recovery");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let post_return_sink_replay_signals = app
        .current_generation_retained_sink_replay_signals_for_local_republish()
        .await;
    let post_return_replay_generations = post_return_sink_replay_signals
        .iter()
        .filter_map(|signal| match signal {
            SinkControlSignal::Activate { generation, .. }
            | SinkControlSignal::Deactivate { generation, .. }
            | SinkControlSignal::Tick { generation, .. } => Some(*generation),
            SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Passthrough(_) => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !post_return_replay_generations.is_empty(),
        "direct local sink-status republish helper must retain a current-generation sink replay wave after later source-only recovery",
    );
    assert_eq!(
        post_return_replay_generations,
        std::collections::BTreeSet::from([4_u64]),
        "direct local sink-status republish helper must rebuild its post-return retained sink replay wave from the later source-only recovery generation instead of replaying stale retained generations: {post_return_sink_replay_signals:?}",
    );
    let trigger_count_before_helper = trigger_rescan_count.load(Ordering::SeqCst);
    let gather_helper_stall_context = || async {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        let blocking_sink_status_summary =
            match tokio::time::timeout(Duration::from_secs(2), app.sink.status_snapshot()).await {
                Ok(Ok(snapshot)) => summarize_sink_status_endpoint(&snapshot),
                Ok(Err(snapshot_err)) => format!("blocking_status_err={snapshot_err}"),
                Err(_) => "blocking_status_timeout".to_string(),
            };
        let source_observability_summary = summarize_source_observability_endpoint(
            &app.source.observability_snapshot_nonblocking().await,
        );
        (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        )
    };
    let post_return_retrigger_entered = Arc::new(Notify::new());
    let post_return_retrigger_release = Arc::new(Notify::new());
    let _post_return_retrigger_reset = LocalSinkStatusRepublishRetriggerPauseHookReset;
    install_local_sink_status_republish_retrigger_pause_hook(
        LocalSinkStatusRepublishRetriggerPauseHook {
            entered: post_return_retrigger_entered.clone(),
            release: post_return_retrigger_release.clone(),
        },
    );
    let helper_probe_entered = Arc::new(Notify::new());
    let helper_probe_release = Arc::new(Notify::new());
    let _probe_pause_reset = LocalSinkStatusRepublishProbePauseHookReset;
    install_local_sink_status_republish_probe_pause_hook(LocalSinkStatusRepublishProbePauseHook {
        entered: helper_probe_entered.clone(),
        release: helper_probe_release.clone(),
    });
    let sink_replay_entered = Arc::new(Notify::new());
    let sink_replay_release = Arc::new(Notify::new());
    let _sink_pause_reset = SinkWorkerControlFramePauseHookReset;
    crate::workers::sink::install_sink_worker_control_frame_pause_hook(
        crate::workers::sink::SinkWorkerControlFramePauseHook {
            entered: sink_replay_entered.clone(),
            release: sink_replay_release.clone(),
        },
    );
    let post_return_retrigger_wait = post_return_retrigger_entered.notified();
    let helper_probe_wait = helper_probe_entered.notified();
    let sink_replay_wait = sink_replay_entered.notified();
    let helper_task = tokio::spawn({
        let source = app.source.clone();
        let sink = app.sink.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = post_return_sink_replay_signals.clone();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
        }
    });
    if tokio::time::timeout(Duration::from_secs(2), post_return_retrigger_wait)
        .await
        .is_err()
    {
        if helper_task.is_finished() {
            let helper_result = tokio::time::timeout(Duration::from_secs(1), helper_task)
                .await
                .expect("join direct local sink-status republish helper fast-path task")
                .expect("join direct local sink-status republish helper fast-path task");
            if let Err(err) = helper_result {
                panic!(
                    "direct local sink-status republish helper returned early but still failed after later source-only recovery: {err}"
                );
            }
            let local_sink_snapshot = app
                .sink
                .status_snapshot_nonblocking()
                .await
                .expect("local sink status should be ready when the direct helper fast-path returns before retrigger");
            let local_ready_groups = local_sink_snapshot
                .groups
                .iter()
                .filter(|group| group.initial_audit_completed)
                .map(|group| group.group_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                local_ready_groups,
                std::collections::BTreeSet::from(["nfs1", "nfs2"]),
                "direct local sink-status republish helper may return early once local sink readiness is already restored after later source-only recovery: {local_sink_snapshot:?}"
            );
            app.close().await.expect("close app");
            return;
        }
        let (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        ) = gather_helper_stall_context().await;
        let trigger_count_now = trigger_rescan_count.load(Ordering::SeqCst);
        panic!(
            "direct local sink-status republish helper did not reach its post-return source->sink retrigger point after later source-only recovery returned; trigger_count_before_helper={trigger_count_before_helper} trigger_count_now={trigger_count_now} helper_task_finished={} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}",
            helper_task.is_finished(),
        );
    }
    let trigger_count_when_helper_reached_retrigger = trigger_rescan_count.load(Ordering::SeqCst);
    assert!(
        trigger_count_when_helper_reached_retrigger > trigger_count_before_helper,
        "direct local sink-status republish helper must re-arm source->sink convergence before it reaches its post-return retrigger point after later source-only recovery returned; trigger_count_before_helper={trigger_count_before_helper} trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger}"
    );
    post_return_retrigger_release.notify_waiters();
    if tokio::time::timeout(Duration::from_secs(2), helper_probe_wait)
        .await
        .is_err()
    {
        let (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        ) = gather_helper_stall_context().await;
        let trigger_count_now = trigger_rescan_count.load(Ordering::SeqCst);
        panic!(
            "direct local sink-status republish helper reached its post-return retrigger point but did not reach the first sink-side probe after later source-only recovery returned; trigger_count_before_helper={trigger_count_before_helper} trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger} trigger_count_now={trigger_count_now} helper_task_finished={} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}",
            helper_task.is_finished(),
        );
    }
    let trigger_count_when_helper_reached_first_sink_probe =
        trigger_rescan_count.load(Ordering::SeqCst);
    assert!(
        trigger_count_when_helper_reached_first_sink_probe
            >= trigger_count_when_helper_reached_retrigger,
        "direct local sink-status republish helper must not lose its post-return source->sink retrigger before it reaches the first sink-side probe; trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger} trigger_count_when_helper_reached_first_sink_probe={trigger_count_when_helper_reached_first_sink_probe}"
    );
    helper_probe_release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), sink_replay_wait)
            .await
            .expect(
                "direct local sink-status republish helper must replay the retained sink control wave after post-return source->sink convergence before it can republish local sink status",
            );
    sink_replay_release.notify_waiters();
    let helper_result = tokio::time::timeout(Duration::from_secs(5), helper_task)
            .await
            .expect("direct local sink-status republish helper should settle after later source-only recovery")
            .expect("join direct local sink-status republish helper task");
    let trigger_count_after_helper = trigger_rescan_count.load(Ordering::SeqCst);

    if let Err(err) = helper_result {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups after helper failure")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups after helper failure")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups after helper failure")
            .unwrap_or_default();
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        let blocking_sink_status_summary =
            match tokio::time::timeout(Duration::from_secs(2), app.sink.status_snapshot()).await {
                Ok(Ok(snapshot)) => summarize_sink_status_endpoint(&snapshot),
                Ok(Err(snapshot_err)) => format!("blocking_status_err={snapshot_err}"),
                Err(_) => "blocking_status_timeout".to_string(),
            };
        let source_observability_summary = summarize_source_observability_endpoint(
            &app.source.observability_snapshot_nonblocking().await,
        );
        panic!(
            "direct local sink-status republish helper must restore ready groups once post-return source->sink convergence has been retriggered; err={err}; trigger_count_before_helper={trigger_count_before_helper} trigger_count_after_helper={trigger_count_after_helper} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}"
        );
    }

    assert!(
        trigger_count_after_helper <= trigger_count_before_helper + 2,
        "direct local sink-status republish helper may spend one extra direct retrigger after the manual-rescan fallback, but it must not loop forever; trigger_count_before_helper={trigger_count_before_helper} trigger_count_after_helper={trigger_count_after_helper}"
    );

    let local_sink_snapshot = app
        .sink
        .status_snapshot_nonblocking()
        .await
        .expect("local sink status should be ready after direct helper completion");
    let local_ready_groups = local_sink_snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        local_ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "direct local sink-status republish helper must restore local sink readiness after later source-only recovery: {local_sink_snapshot:?}"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_sink_status_republish_after_recovery_uses_blocking_sink_status_truth_before_reentering_retained_sink_replay()
 {
    struct SourceControlErrorHookReset;
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    struct SinkWorkerControlFramePauseHookReset;
    struct SinkWorkerStatusNonblockingCacheFallbackHookReset;
    struct SinkWorkerStatusSnapshotHookReset;

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

    impl Drop for SourceWorkerTriggerRescanWhenReadyCallCountHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_trigger_rescan_when_ready_call_count_hook();
        }
    }

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
        }
    }

    impl Drop for SinkWorkerStatusNonblockingCacheFallbackHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_status_nonblocking_cache_fallback_hook();
        }
    }

    impl Drop for SinkWorkerStatusSnapshotHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_status_snapshot_hook();
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
            NodeId("node-c-local-sink-status-helper".into()),
            Some(boundary.clone()),
            Some(boundary),
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
    let initial_trigger_rescan_count = Arc::new(AtomicUsize::new(0));
    let _trigger_rescan_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    crate::workers::source::install_source_worker_trigger_rescan_when_ready_call_count_hook(
        crate::workers::source::SourceWorkerTriggerRescanWhenReadyCallCountHook {
            count: initial_trigger_rescan_count.clone(),
        },
    );

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");
    let initial_ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout(
            Duration::from_millis(350),
            app.sink.status_snapshot_nonblocking(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.initial_audit_completed)
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups
                    == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
                {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }
        if tokio::time::Instant::now() >= initial_ready_deadline {
            app.source
                .trigger_rescan_when_ready()
                .await
                .expect("direct trigger_rescan_when_ready after failed deferred initial trigger");
            let direct_trigger_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let mut direct_trigger_restored_ready = false;
            let mut latest_source_observability =
                app.source.observability_snapshot_nonblocking().await;
            let mut latest_cached_sink_status = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
            while tokio::time::Instant::now() < direct_trigger_deadline {
                latest_source_observability = app.source.observability_snapshot_nonblocking().await;
                latest_cached_sink_status = app
                    .sink
                    .cached_status_snapshot()
                    .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                    .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
                if latest_source_observability
                    .published_batches_by_node
                    .values()
                    .copied()
                    .sum::<u64>()
                    > 0
                {
                    direct_trigger_restored_ready = true;
                    break;
                }
                match tokio::time::timeout(
                    Duration::from_millis(200),
                    app.sink.status_snapshot_nonblocking(),
                )
                .await
                {
                    Ok(Ok(snapshot)) => {
                        let ready_groups = snapshot
                            .groups
                            .iter()
                            .filter(|group| group.initial_audit_completed)
                            .map(|group| group.group_id.clone())
                            .collect::<std::collections::BTreeSet<_>>();
                        if ready_groups
                            == std::collections::BTreeSet::from([
                                "nfs1".to_string(),
                                "nfs2".to_string(),
                            ])
                        {
                            direct_trigger_restored_ready = true;
                            break;
                        }
                    }
                    Ok(Err(_)) | Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let source_groups = app
                .source
                .scheduled_source_group_ids()
                .await
                .expect("source groups before helper precondition failure")
                .unwrap_or_default();
            let scan_groups = app
                .source
                .scheduled_scan_group_ids()
                .await
                .expect("scan groups before helper precondition failure")
                .unwrap_or_default();
            let sink_groups = app
                .sink
                .scheduled_group_ids()
                .await
                .expect("sink groups before helper precondition failure")
                .unwrap_or_default();
            let cached_sink_status = app
                .sink
                .cached_status_snapshot()
                .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
                .unwrap_or_else(|err| format!("cached_sink_status_unavailable err={err}"));
            let source_observability =
                summarize_source_observability_endpoint(&latest_source_observability);
            let initial_trigger_count = initial_trigger_rescan_count.load(Ordering::SeqCst);
            panic!(
                "precondition: direct helper blocking-truth seam requires the local sink to materialize both nfs1/nfs2 before the fail-closed recovery begins; initial_trigger_count={initial_trigger_count} direct_trigger_restored_ready={direct_trigger_restored_ready} source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status_before_direct_trigger={cached_sink_status} cached_sink_status_after_direct_trigger={latest_cached_sink_status} source_observability={source_observability}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _source_error_reset = SourceControlErrorHookReset;
    crate::workers::source::install_source_worker_control_frame_error_hook(
        crate::workers::source::SourceWorkerControlFrameErrorHook {
            err: CnxError::ProtocolViolation("simulated source cleanup-tail failure".to_string()),
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
        "source-only deactivate should fail-close into uninitialized replay-required recovery",
    );
    crate::workers::source::clear_source_worker_control_frame_error_hook();
    assert!(!app.control_initialized());

    let previous_instance_id = source_client.worker_instance_id_for_tests().await;

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
    .expect("cleanup-only source tail should settle while runtime remains uninitialized");

    let _refresh_error_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    crate::workers::source::install_source_worker_scheduled_groups_refresh_error_queue_hook(
        crate::workers::source::SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::Timeout]),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    let mut later_source_only = source_wave(4);
    later_source_only[0] = activate_envelope_with_route_key_and_scope_rows(
        execution_units::SOURCE_RUNTIME_UNIT_ID,
        format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
        4,
    );
    app.on_control_frame(&later_source_only)
        .await
        .expect("later source-only recovery should settle after the cleanup-only tail");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let post_return_sink_replay_signals = app
        .current_generation_retained_sink_replay_signals_for_local_republish()
        .await;
    let post_return_replay_generations = post_return_sink_replay_signals
        .iter()
        .filter_map(|signal| match signal {
            SinkControlSignal::Activate { generation, .. }
            | SinkControlSignal::Deactivate { generation, .. }
            | SinkControlSignal::Tick { generation, .. } => Some(*generation),
            SinkControlSignal::RuntimeHostGrantChange { .. }
            | SinkControlSignal::Passthrough(_) => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        !post_return_replay_generations.is_empty(),
        "precondition: helper blocking-truth seam requires a current-generation retained sink replay wave after later source-only recovery",
    );
    assert_eq!(
        post_return_replay_generations,
        std::collections::BTreeSet::from([4_u64]),
        "precondition: helper blocking-truth seam requires the post-return retained sink replay wave to be rebuilt at generation 4: {post_return_sink_replay_signals:?}",
    );

    let cached_not_ready_snapshot = crate::sink::SinkStatusSnapshot {
        live_nodes: 2,
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "nfs1".to_string(),
                total_nodes: 1,
                live_nodes: 1,
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
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "nfs2".to_string(),
                total_nodes: 1,
                live_nodes: 1,
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
            },
        ],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-c-local-sink-status-helper".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };
    crate::workers::sink::install_sink_worker_status_snapshot_hook(
        crate::workers::sink::SinkWorkerStatusSnapshotHook {
            snapshot: cached_not_ready_snapshot,
        },
    );
    let cached_snapshot_before_helper = app
        .sink
        .status_snapshot()
        .await
        .expect("poison local cached sink status with a current but non-ready snapshot before helper runs");
    assert!(
        !sink_status_snapshot_ready_for_expected_groups(
            &cached_snapshot_before_helper,
            &expected_groups,
        ),
        "precondition: helper blocking-truth seam requires the local cached sink status to stay non-ready before helper execution: {cached_snapshot_before_helper:?}"
    );

    let trigger_count_before_helper = initial_trigger_rescan_count.load(Ordering::SeqCst);
    let gather_helper_stall_context = || async {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups while local sink-status republish helper is stalled")
            .unwrap_or_default();
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        let blocking_sink_status_summary =
            match tokio::time::timeout(Duration::from_secs(2), app.sink.status_snapshot()).await {
                Ok(Ok(snapshot)) => summarize_sink_status_endpoint(&snapshot),
                Ok(Err(snapshot_err)) => format!("blocking_status_err={snapshot_err}"),
                Err(_) => "blocking_status_timeout".to_string(),
            };
        let source_observability_summary = summarize_source_observability_endpoint(
            &app.source.observability_snapshot_nonblocking().await,
        );
        (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        )
    };
    let post_return_retrigger_entered = Arc::new(Notify::new());
    let post_return_retrigger_release = Arc::new(Notify::new());
    let _post_return_retrigger_reset = LocalSinkStatusRepublishRetriggerPauseHookReset;
    install_local_sink_status_republish_retrigger_pause_hook(
        LocalSinkStatusRepublishRetriggerPauseHook {
            entered: post_return_retrigger_entered.clone(),
            release: post_return_retrigger_release.clone(),
        },
    );
    let helper_probe_entered = Arc::new(Notify::new());
    let helper_probe_release = Arc::new(Notify::new());
    let _probe_pause_reset = LocalSinkStatusRepublishProbePauseHookReset;
    install_local_sink_status_republish_probe_pause_hook(LocalSinkStatusRepublishProbePauseHook {
        entered: helper_probe_entered.clone(),
        release: helper_probe_release.clone(),
    });
    let sink_replay_entered = Arc::new(Notify::new());
    let sink_replay_release = Arc::new(Notify::new());
    let _sink_pause_reset = SinkWorkerControlFramePauseHookReset;
    crate::workers::sink::install_sink_worker_control_frame_pause_hook(
        crate::workers::sink::SinkWorkerControlFramePauseHook {
            entered: sink_replay_entered.clone(),
            release: sink_replay_release.clone(),
        },
    );
    let post_return_retrigger_wait = post_return_retrigger_entered.notified();
    let helper_probe_wait = helper_probe_entered.notified();
    let sink_replay_wait = sink_replay_entered.notified();
    let helper_task = tokio::spawn({
        let source = app.source.clone();
        let sink = app.sink.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = post_return_sink_replay_signals.clone();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), post_return_retrigger_wait)
        .await
        .expect(
            "blocking-truth helper seam requires the post-return source->sink retrigger point to be reached",
        );
    let trigger_count_when_helper_reached_retrigger = initial_trigger_rescan_count.load(Ordering::SeqCst);
    assert!(
        trigger_count_when_helper_reached_retrigger > trigger_count_before_helper,
        "helper must re-arm source->sink convergence before the first sink-side probe; trigger_count_before_helper={trigger_count_before_helper} trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger}"
    );
    post_return_retrigger_release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), helper_probe_wait)
        .await
        .expect(
            "blocking-truth helper seam requires the first sink-side probe to be reached",
        );
    let trigger_count_when_helper_reached_first_sink_probe =
        initial_trigger_rescan_count.load(Ordering::SeqCst);
    assert!(
        trigger_count_when_helper_reached_first_sink_probe
            >= trigger_count_when_helper_reached_retrigger,
        "helper must not lose its post-return source->sink retrigger before the first sink-side probe; trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger} trigger_count_when_helper_reached_first_sink_probe={trigger_count_when_helper_reached_first_sink_probe}"
    );

    let cached_snapshot_before_probe = app
        .sink
        .cached_status_snapshot()
        .expect("cached sink status before blocking-truth probe");
    assert!(
        !sink_status_snapshot_ready_for_expected_groups(
            &cached_snapshot_before_probe,
            &expected_groups
        ),
        "precondition: helper must still see local cached sink status as not ready before the first sink-side probe so the blocking-truth seam is meaningful: {cached_snapshot_before_probe:?}"
    );

    let _sink_cache_fallback_reset = SinkWorkerStatusNonblockingCacheFallbackHookReset;
    crate::workers::sink::install_sink_worker_status_nonblocking_cache_fallback_hook(
        crate::workers::sink::SinkWorkerStatusNonblockingCacheFallbackHook,
    );
    let ready_snapshot = crate::sink::SinkStatusSnapshot {
        live_nodes: 2,
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "nfs1".to_string(),
                total_nodes: 1,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "nfs2".to_string(),
                total_nodes: 1,
                live_nodes: 1,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-c-local-sink-status-helper".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };
    let _sink_status_snapshot_reset = SinkWorkerStatusSnapshotHookReset;
    crate::workers::sink::install_sink_worker_status_snapshot_hook(
        crate::workers::sink::SinkWorkerStatusSnapshotHook {
            snapshot: ready_snapshot,
        },
    );
    helper_probe_release.notify_waiters();

    if tokio::time::timeout(Duration::from_millis(600), sink_replay_wait)
        .await
        .is_ok()
    {
        sink_replay_release.notify_waiters();
        let _ = tokio::time::timeout(Duration::from_secs(1), helper_task).await;
        let (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        ) = gather_helper_stall_context().await;
        panic!(
            "local sink-status republish helper must accept a bounded blocking sink-status truth before it re-enters retained sink replay once runtime scope already converged; source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}"
        );
    }

    let helper_result = tokio::time::timeout(Duration::from_secs(5), helper_task)
        .await
        .expect("blocking-truth local sink-status republish helper should settle")
        .expect("join blocking-truth local sink-status republish helper task");
    if let Err(err) = helper_result {
        let (
            source_groups,
            scan_groups,
            sink_groups,
            cached_sink_status_summary,
            blocking_sink_status_summary,
            source_observability_summary,
        ) = gather_helper_stall_context().await;
        panic!(
            "local sink-status republish helper must accept a bounded blocking sink-status truth once runtime scope already converged instead of failing closed on a cached zeroish nonblocking view; err={err}; source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?} cached_sink_status={cached_sink_status_summary} blocking_sink_status={blocking_sink_status_summary} source_observability={source_observability_summary}"
        );
    }

    let local_sink_snapshot = app
        .sink
        .status_snapshot_nonblocking()
        .await
        .expect("local sink status should be ready after helper accepts blocking sink-status truth");
    let local_ready_groups = local_sink_snapshot
        .groups
        .iter()
        .filter(|group| group.initial_audit_completed)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        local_ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "helper must refresh local sink readiness once it accepts the bounded blocking sink-status truth: {local_sink_snapshot:?}"
    );

    app.close().await.expect("close app");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_one_initial_mixed_source_and_sink_activate_does_not_enter_local_sink_status_republish_helper()
{
    struct LocalSinkStatusRepublishHelperEntryCountHookReset;

    impl Drop for LocalSinkStatusRepublishHelperEntryCountHookReset {
        fn drop(&mut self) {
            clear_local_sink_status_republish_helper_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("initial-wave")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("initial-wave")).expect("create nfs2 dir");
    fs::write(nfs1.join("initial-wave").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("initial-wave").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[("nfs1", &["node-a::nfs1"][..]), ("nfs2", &["node-a::nfs2"][..])];
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

    let helper_entries = Arc::new(AtomicUsize::new(0));
    let _helper_entry_reset = LocalSinkStatusRepublishHelperEntryCountHookReset;
    install_local_sink_status_republish_helper_entry_count_hook(helper_entries.clone());

    let mut initial = source_wave(1);
    initial.extend(sink_wave(1));
    app.on_control_frame(&initial)
        .await
        .expect("generation-one initial mixed source/sink activate should succeed without post-recovery local sink-status waiting");

    assert_eq!(
        helper_entries.load(Ordering::Acquire),
        0,
        "generation-one initial mixed source/sink activate must not enter the post-recovery local sink-status republish helper"
    );

    app.close().await.expect("close app");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_sink_tick_does_not_reenter_sink_worker_when_replay_not_required()
{
    struct SinkWorkerControlFramePauseHookReset;

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            crate::workers::sink::clear_sink_worker_control_frame_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("steady-tick")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("steady-tick")).expect("create nfs2 dir");
    fs::write(nfs1.join("steady-tick").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("steady-tick").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
        ];
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("generation-two source/sink activate should succeed before steady tick");

    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups")
            .unwrap_or_default();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before steady tick: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "ordinary steady sink tick seam requires runtime control to remain initialized before the tick followup"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady sink tick seam requires retained sink replay to be disarmed before the tick followup"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = SinkWorkerControlFramePauseHookReset;
    crate::workers::sink::install_sink_worker_control_frame_pause_hook(
        crate::workers::sink::SinkWorkerControlFramePauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let tick_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[tick_envelope_with_route_key(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                2,
            )])
            .await
        }
    });

    if tokio::time::timeout(Duration::from_millis(600), entered.notified())
        .await
        .is_ok()
    {
        let _ = tick_task.await;
        panic!(
            "ordinary current-generation sink tick while runtime stays initialized and retained replay is disarmed must not re-enter sink worker or piggyback retained sink replay"
        );
    }

    tokio::time::timeout(Duration::from_secs(5), tick_task)
        .await
        .expect("ordinary steady sink tick should settle without sink worker re-entry")
        .expect("join steady sink tick task")
        .expect("ordinary steady sink tick should not exhaust runtime_app sink recovery");

    assert!(
        app.control_initialized(),
        "ordinary steady sink tick must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady sink tick must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_tick_does_not_reenter_source_worker_when_replay_not_required()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("steady-tick")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("steady-tick")).expect("create nfs2 dir");
    fs::write(nfs1.join("steady-tick").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("steady-tick").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
        ];
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("generation-two source/sink activate should succeed before steady source tick");

    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups")
            .unwrap_or_default();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before steady source tick: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "ordinary steady source tick seam requires runtime control to remain initialized before the tick followup"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source tick seam requires retained source replay to be disarmed before the tick followup"
    );

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::ProtocolViolation(
            "must not re-enter source worker or piggyback retained source replay".to_string(),
        )]),
    });

    app.on_control_frame(&[
        tick_envelope_with_route_key(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            2,
        ),
    ])
    .await
    .expect("ordinary steady source tick should not exhaust runtime_app source recovery");

    assert!(
        app.control_initialized(),
        "ordinary steady source tick must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source tick must leave retained source replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_and_sink_ticks_do_not_reenter_runtime_app_source_apply_when_replay_not_required()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("steady-tick")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("steady-tick")).expect("create nfs2 dir");
    fs::write(nfs1.join("steady-tick").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("steady-tick").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
        ];
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("generation-two source/sink activate should succeed before steady mixed ticks");

    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups")
            .unwrap_or_default();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before steady mixed ticks: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "ordinary steady mixed source/sink/facade ticks require runtime control to remain initialized before the followup"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks require retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks require retained sink replay to be disarmed before the followup"
    );

    let _source_apply_error_reset = SourceApplyErrorQueueHookReset;
    install_source_apply_error_queue_hook(SourceApplyErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::ProtocolViolation(
            "must not re-enter runtime_app source.apply during ordinary mixed steady ticks"
                .to_string(),
        )]),
    });

    app.on_control_frame(&[
        tick_envelope_with_route_key(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            2,
        ),
    ])
    .await
    .expect("ordinary steady mixed source/sink ticks should not exhaust runtime_app source recovery");

    assert!(
        app.control_initialized(),
        "ordinary steady mixed source/sink/facade ticks must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_and_facade_ticks_reenter_runtime_app_source_apply_when_facade_publication_still_needs_current_generation_source_truth()
{
    struct SourceApplyEntryCountHookReset;

    impl Drop for SourceApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_source_apply_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("steady-tick")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("steady-tick")).expect("create nfs2 dir");
    fs::write(nfs1.join("steady-tick").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("steady-tick").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
        ];
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("generation-two source/sink activate should succeed before steady source+facade ticks");

    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups")
            .unwrap_or_default();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before steady source+facade ticks: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "ordinary steady source+facade tick seam requires runtime control to remain initialized before the followup"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source+facade tick seam requires retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source+facade tick seam requires retained sink replay to be disarmed before the followup"
    );

    let source_apply_entries = Arc::new(AtomicUsize::new(0));
    let _source_apply_entry_reset = SourceApplyEntryCountHookReset;
    install_source_apply_entry_count_hook(source_apply_entries.clone());

    app.on_control_frame(&[
        tick_envelope_with_route_key(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            2,
        ),
        tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 2),
    ])
    .await
    .expect(
        "ordinary steady source+facade ticks should settle without exhausting runtime_app control recovery",
    );

    assert!(
        source_apply_entries.load(Ordering::Acquire) > 0,
        "ordinary current-generation source+facade ticks while runtime stays initialized and retained replay is disarmed must still re-enter runtime_app source.apply so facade publication/status followups do not lose current-generation source truth",
    );

    assert!(
        app.control_initialized(),
        "ordinary steady source+facade ticks must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source+facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady source+facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_sink_and_facade_ticks_do_not_reenter_runtime_app_apply_paths_when_replay_not_required()
{
    struct SourceApplyEntryCountHookReset;
    struct SinkApplyEntryCountHookReset;

    impl Drop for SourceApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_source_apply_entry_count_hook();
        }
    }

    impl Drop for SinkApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_sink_apply_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("steady-tick")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("steady-tick")).expect("create nfs2 dir");
    fs::write(nfs1.join("steady-tick").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("steady-tick").join("seed.txt"), b"b").expect("seed nfs2");

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
                    roots: vec![worker_root("nfs1", &nfs1), worker_root("nfs2", &nfs2)],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
                        worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".to_string()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker runtime app"),
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[("nfs1", &["node-a::nfs1"]), ("nfs2", &["node-a::nfs2"])],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
        ];
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("generation-two source/sink activate should succeed before steady mixed ticks");

    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let source_groups = app
            .source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = app
            .source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = app
            .sink
            .scheduled_group_ids()
            .await
            .expect("sink groups")
            .unwrap_or_default();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before steady mixed ticks: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "ordinary steady mixed source/sink/facade ticks require runtime control to remain initialized before the followup"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks require retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks require retained sink replay to be disarmed before the followup"
    );

    let source_apply_entries = Arc::new(AtomicUsize::new(0));
    let sink_apply_entries = Arc::new(AtomicUsize::new(0));
    let _source_apply_entry_reset = SourceApplyEntryCountHookReset;
    let _sink_apply_entry_reset = SinkApplyEntryCountHookReset;
    install_source_apply_entry_count_hook(source_apply_entries.clone());
    install_sink_apply_entry_count_hook(sink_apply_entries.clone());

    app.on_control_frame(&[
        tick_envelope_with_route_key(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            2,
        ),
        tick_envelope_with_route_key(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            2,
        ),
        tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 2),
    ])
    .await
    .expect(
        "ordinary steady mixed source/sink/facade ticks should settle without exhausting runtime_app control recovery",
    );

    assert_eq!(
        source_apply_entries.load(Ordering::Acquire),
        0,
        "ordinary current-generation mixed source/sink/facade ticks while runtime stays initialized and retained replay is disarmed must not re-enter runtime_app source.apply",
    );
    assert_eq!(
        sink_apply_entries.load(Ordering::Acquire),
        0,
        "ordinary current-generation mixed source/sink/facade ticks while runtime stays initialized and retained replay is disarmed must not re-enter runtime_app sink.apply",
    );

    assert!(
        app.control_initialized(),
        "ordinary steady mixed source/sink/facade ticks must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.source_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required.load(Ordering::Acquire),
        "ordinary steady mixed source/sink/facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}
