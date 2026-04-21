fn local_sink_status_republish_helper_test_serial() -> &'static tokio::sync::Mutex<()> {
    static CELL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    CELL.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[test]
fn sink_status_snapshot_ready_for_expected_groups_trusts_exported_readiness_over_stale_legacy_bit()
{
    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    let snapshot = crate::sink::SinkStatusSnapshot {
        live_nodes: 1,
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
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
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["nfs1".to_string()],
        )]),
        ..crate::sink::SinkStatusSnapshot::default()
    };

    assert!(
        sink_status_snapshot_ready_for_expected_groups(&snapshot, &expected_groups),
        "runtime_app local sink-status helper must trust exported readiness over stale initial_audit_completed=false: {snapshot:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_sink_status_republish_after_recovery_restores_ready_groups_after_cleanup_only_source_tail()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
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
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
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
                    .filter(|group| group.is_ready())
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
                .trigger_rescan_when_ready_epoch()
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
                            .filter(|group| group.is_ready())
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
        let runtime_state_changed = app.runtime_state_changed.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = post_return_sink_replay_signals.clone();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                runtime_state_changed,
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
                .filter(|group| group.is_ready())
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
        .filter(|group| group.is_ready())
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
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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
                    .filter(|group| group.is_ready())
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
                .trigger_rescan_when_ready_epoch()
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
                            .filter(|group| group.is_ready())
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
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::Ready,
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
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::PendingMaterialization,
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
    let cached_snapshot_before_helper = app.sink.status_snapshot().await.expect(
        "poison local cached sink status with a current but non-ready snapshot before helper runs",
    );
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
        let runtime_state_changed = app.runtime_state_changed.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = post_return_sink_replay_signals.clone();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_from_parts(
                source,
                sink,
                runtime_state_changed,
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
    let trigger_count_when_helper_reached_retrigger =
        initial_trigger_rescan_count.load(Ordering::SeqCst);
    assert!(
        trigger_count_when_helper_reached_retrigger > trigger_count_before_helper,
        "helper must re-arm source->sink convergence before the first sink-side probe; trigger_count_before_helper={trigger_count_before_helper} trigger_count_when_helper_reached_retrigger={trigger_count_when_helper_reached_retrigger}"
    );
    post_return_retrigger_release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), helper_probe_wait)
        .await
        .expect("blocking-truth helper seam requires the first sink-side probe to be reached");
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
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::Ready,
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
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::Ready,
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

    let local_sink_snapshot = app.sink.status_snapshot_nonblocking().await.expect(
        "local sink status should be ready after helper accepts blocking sink-status truth",
    );
    let local_ready_groups = local_sink_snapshot
        .groups
        .iter()
        .filter(|group| group.is_ready())
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
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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
async fn wait_for_sink_status_republish_readiness_after_recovery_accepts_expected_ready_groups_when_snapshot_contains_extra_ready_group()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SinkWorkerStatusSnapshotHookReset;

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
            NodeId("node-c-extra-ready-group".into()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init app"),
    );
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
            match tokio::time::timeout(
                Duration::from_millis(350),
                app.sink.status_snapshot_nonblocking(),
            )
            .await
            {
                Ok(Ok(snapshot))
                    if sink_status_snapshot_ready_for_expected_groups(
                        &snapshot,
                        &expected_groups,
                    ) =>
                {
                    break;
                }
                Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
            }
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for converged ready sink status before extra-ready-group readiness exact"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _sink_status_snapshot_reset = SinkWorkerStatusSnapshotHookReset;
    crate::workers::sink::install_sink_worker_status_snapshot_hook(
        crate::workers::sink::SinkWorkerStatusSnapshotHook {
            snapshot: crate::sink::SinkStatusSnapshot {
                live_nodes: 3,
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
                        overflow_pending_materialization: false,

                        readiness: crate::sink::GroupReadinessState::Ready,
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
                        overflow_pending_materialization: false,

                        readiness: crate::sink::GroupReadinessState::Ready,
                        materialized_revision: 1,
                        estimated_heap_bytes: 0,
                    },
                    crate::sink::SinkGroupStatusSnapshot {
                        group_id: "nfs3".to_string(),
                        primary_object_ref: "nfs3".to_string(),
                        total_nodes: 1,
                        live_nodes: 1,
                        tombstoned_count: 0,
                        attested_count: 0,
                        suspect_count: 0,
                        blind_spot_count: 0,
                        shadow_time_us: 0,
                        shadow_lag_us: 0,
                        overflow_pending_materialization: false,

                        readiness: crate::sink::GroupReadinessState::Ready,
                        materialized_revision: 1,
                        estimated_heap_bytes: 0,
                    },
                ],
                scheduled_groups_by_node: std::collections::BTreeMap::from([(
                    "node-c-extra-ready-group".to_string(),
                    vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
                )]),
                ..crate::sink::SinkStatusSnapshot::default()
            },
        },
    );

    let readiness_result = app
        .wait_for_sink_status_republish_readiness_after_recovery(None)
        .await;
    assert_eq!(
        readiness_result.expect(
            "runtime_app retained-replay readiness wait must accept expected ready groups even when the snapshot still carries an extra unrelated ready group"
        ),
        None,
        "runtime_app retained-replay readiness wait should settle once the expected groups are ready instead of demanding an exact ready-group set match"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_sink_status_republish_requiring_probe_checks_first_probe_before_post_return_retrigger_when_cached_ready()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;

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
            NodeId("node-c-require-probe-helper".into()),
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

    let trigger_rescan_count = Arc::new(AtomicUsize::new(0));
    let _trigger_rescan_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    crate::workers::source::install_source_worker_trigger_rescan_when_ready_call_count_hook(
        crate::workers::source::SourceWorkerTriggerRescanWhenReadyCallCountHook {
            count: trigger_rescan_count.clone(),
        },
    );

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let expected_source_groups = expected_groups.clone();
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
        if source_groups == expected_source_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            match tokio::time::timeout(
                Duration::from_millis(350),
                app.sink.status_snapshot_nonblocking(),
            )
            .await
            {
                Ok(Ok(snapshot))
                    if sink_status_snapshot_ready_for_expected_groups(
                        &snapshot,
                        &expected_groups,
                    ) =>
                {
                    break;
                }
                Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
            }
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for cached-ready local sink status before requiring-probe helper test"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let cached_snapshot_before_helper = app
        .sink
        .cached_status_snapshot()
        .expect("cached sink status before requiring-probe helper");
    assert!(
        sink_status_snapshot_ready_for_expected_groups(
            &cached_snapshot_before_helper,
            &expected_groups,
        ),
        "precondition: requiring-probe helper seam requires cached sink status to already be ready: {cached_snapshot_before_helper:?}"
    );

    let trigger_count_before_helper = trigger_rescan_count.load(Ordering::SeqCst);
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

    let helper_task = tokio::spawn({
        let source = app.source.clone();
        let sink = app.sink.clone();
        let runtime_state_changed = app.runtime_state_changed.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = Vec::<SinkControlSignal>::new();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
                source,
                sink,
                runtime_state_changed,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
        }
    });

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(2)) => {
            panic!(
                "requiring-probe helper stalled before reaching its first sink-side probe or retrigger point"
            );
        }
        _ = post_return_retrigger_entered.notified() => {
            post_return_retrigger_release.notify_waiters();
            helper_probe_release.notify_waiters();
            let _ = tokio::time::timeout(Duration::from_secs(1), helper_task).await;
            panic!(
                "requiring-probe helper must check the first sink-side probe before re-arming source->sink convergence when cached sink status is already ready"
            );
        }
        _ = helper_probe_entered.notified() => {}
    }

    assert_eq!(
        trigger_rescan_count.load(Ordering::SeqCst),
        trigger_count_before_helper,
        "requiring-probe helper must not trigger source->sink convergence before its first sink-side probe when cached sink status is already ready"
    );

    helper_probe_release.notify_waiters();
    let helper_result = tokio::time::timeout(Duration::from_secs(5), helper_task)
        .await
        .expect("requiring-probe helper should settle after its first sink-side probe unblocks")
        .expect("join requiring-probe helper task");
    if let Err(err) = helper_result {
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        panic!(
            "requiring-probe helper should accept an already-ready first sink-side probe without falling into post-return retrigger logic: err={err} cached_sink_status={cached_sink_status_summary}"
        );
    }

    assert_eq!(
        trigger_rescan_count.load(Ordering::SeqCst),
        trigger_count_before_helper,
        "requiring-probe helper must settle without triggering source->sink convergence when the first sink-side probe already proves readiness"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_sink_status_republish_requiring_probe_replays_retained_sink_wave_before_returning_when_cached_ready()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    struct SinkWorkerControlFramePauseHookReset;

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
            NodeId("node-c-require-probe-replay".into()),
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

    let trigger_rescan_count = Arc::new(AtomicUsize::new(0));
    let _trigger_rescan_reset = SourceWorkerTriggerRescanWhenReadyCallCountHookReset;
    crate::workers::source::install_source_worker_trigger_rescan_when_ready_call_count_hook(
        crate::workers::source::SourceWorkerTriggerRescanWhenReadyCallCountHook {
            count: trigger_rescan_count.clone(),
        },
    );

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let expected_source_groups = expected_groups.clone();
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
        if source_groups == expected_source_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            match tokio::time::timeout(
                Duration::from_millis(350),
                app.sink.status_snapshot_nonblocking(),
            )
            .await
            {
                Ok(Ok(snapshot))
                    if sink_status_snapshot_ready_for_expected_groups(
                        &snapshot,
                        &expected_groups,
                    ) =>
                {
                    break;
                }
                Ok(Ok(snapshot)) => {
                    eprintln!(
                        "require-probe replay precondition not ready snapshot={:?} cached={:?}",
                        snapshot,
                        app.sink.cached_status_snapshot().ok()
                    );
                }
                Ok(Err(err)) => {
                    eprintln!(
                        "require-probe replay precondition status_snapshot_nonblocking err={} cached={:?}",
                        err,
                        app.sink.cached_status_snapshot().ok()
                    );
                }
                Err(_) => {
                    eprintln!(
                        "require-probe replay precondition status_snapshot_nonblocking timeout cached={:?}",
                        app.sink.cached_status_snapshot().ok()
                    );
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for cached-ready local sink status before requiring-probe replay test"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let cached_snapshot_before_helper = app
        .sink
        .cached_status_snapshot()
        .expect("cached sink status before requiring-probe replay helper");
    assert!(
        sink_status_snapshot_ready_for_expected_groups(
            &cached_snapshot_before_helper,
            &expected_groups,
        ),
        "precondition: requiring-probe replay seam requires cached sink status to already be ready: {cached_snapshot_before_helper:?}"
    );

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
        "precondition: requiring-probe replay seam requires a retained sink replay wave when cached sink status is already ready",
    );
    assert_eq!(
        post_return_replay_generations,
        std::collections::BTreeSet::from([2_u64]),
        "precondition: requiring-probe replay seam should retain the current sink generation before helper execution: {post_return_sink_replay_signals:?}",
    );

    let trigger_count_before_helper = trigger_rescan_count.load(Ordering::SeqCst);
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

    let helper_task = tokio::spawn({
        let source = app.source.clone();
        let sink = app.sink.clone();
        let runtime_state_changed = app.runtime_state_changed.clone();
        let expected_groups = expected_groups.clone();
        let post_return_sink_replay_signals = post_return_sink_replay_signals.clone();
        async move {
            FSMetaApp::wait_for_local_sink_status_republish_after_recovery_requiring_probe_from_parts(
                source,
                sink,
                runtime_state_changed,
                &expected_groups,
                &post_return_sink_replay_signals,
            )
            .await
        }
    });

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(2)) => {
            panic!(
                "requiring-probe replay helper stalled before reaching its first sink-side probe or retrigger point"
            );
        }
        _ = post_return_retrigger_entered.notified() => {
            post_return_retrigger_release.notify_waiters();
            helper_probe_release.notify_waiters();
            sink_replay_release.notify_waiters();
            let _ = tokio::time::timeout(Duration::from_secs(1), helper_task).await;
            panic!(
                "requiring-probe replay helper must check the first sink-side probe before re-arming source->sink convergence when cached sink status is already ready"
            );
        }
        _ = helper_probe_entered.notified() => {}
    }

    assert_eq!(
        trigger_rescan_count.load(Ordering::SeqCst),
        trigger_count_before_helper,
        "requiring-probe replay helper must not trigger source->sink convergence before its first sink-side probe when cached sink status is already ready"
    );

    helper_probe_release.notify_waiters();
    if tokio::time::timeout(Duration::from_secs(2), sink_replay_entered.notified())
        .await
        .is_err()
    {
        if helper_task.is_finished() {
            let helper_result = tokio::time::timeout(Duration::from_secs(1), helper_task)
                .await
                .expect("join requiring-probe replay helper task")
                .expect("join requiring-probe replay helper task");
            panic!(
                "requiring-probe replay helper returned without replaying the retained sink control wave once the first sink-side probe confirmed cached readiness: {helper_result:?}"
            );
        }
        panic!(
            "requiring-probe replay helper reached the first sink-side probe but never re-entered the retained sink control wave even though cached readiness alone is insufficient in this lane"
        );
    }

    sink_replay_release.notify_waiters();
    let helper_result = tokio::time::timeout(Duration::from_secs(5), helper_task)
        .await
        .expect("requiring-probe replay helper should settle after retained sink replay unblocks")
        .expect("join requiring-probe replay helper task");
    if let Err(err) = helper_result {
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        panic!(
            "requiring-probe replay helper should settle after replaying the retained sink control wave on top of a cached-ready first sink-side probe: err={err} cached_sink_status={cached_sink_status_summary}"
        );
    }

    assert_eq!(
        trigger_rescan_count.load(Ordering::SeqCst),
        trigger_count_before_helper,
        "requiring-probe replay helper must settle without triggering source->sink convergence when the cached-ready first probe only needs a retained sink replay wave"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_sink_tick_does_not_reenter_sink_worker_when_replay_not_required()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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

    let expected_source_groups = std::collections::BTreeSet::new();
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
        if source_groups == expected_source_groups
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
        !app.sink_state_replay_required(),
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
        !app.sink_state_replay_required(),
        "ordinary steady sink tick must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_tick_does_not_reenter_source_worker_when_replay_not_required()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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

    let expected_source_groups = std::collections::BTreeSet::new();
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
        if source_groups == expected_source_groups
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
        !app.source_state_replay_required(),
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
        !app.source_state_replay_required(),
        "ordinary steady source tick must leave retained source replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_and_sink_ticks_do_not_reenter_runtime_app_source_apply_when_replay_not_required()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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

    let expected_source_groups = std::collections::BTreeSet::new();
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
        if source_groups == expected_source_groups
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
        !app.source_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks require retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required(),
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
    .expect(
        "ordinary steady mixed source/sink ticks should not exhaust runtime_app source recovery",
    );

    assert!(
        app.control_initialized(),
        "ordinary steady mixed source/sink/facade ticks must keep runtime control initialized when replay is not required"
    );
    assert!(
        !app.source_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_and_facade_ticks_reenter_runtime_app_source_apply_when_facade_publication_still_needs_current_generation_source_truth()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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
    let expected_source_groups = std::collections::BTreeSet::new();

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
    app.on_control_frame(&initial).await.expect(
        "generation-two source/sink activate should succeed before steady source+facade ticks",
    );

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
        if source_groups == expected_source_groups
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
        !app.source_state_replay_required(),
        "ordinary steady source+facade tick seam requires retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required(),
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
        !app.source_state_replay_required(),
        "ordinary steady source+facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required(),
        "ordinary steady source+facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_current_generation_source_sink_and_facade_ticks_do_not_reenter_runtime_app_apply_paths_when_replay_not_required()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

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

    let expected_source_groups = std::collections::BTreeSet::new();
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
        if source_groups == expected_source_groups
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
        !app.source_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks require retained source replay to be disarmed before the followup"
    );
    assert!(
        !app.sink_state_replay_required(),
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
        !app.source_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks must leave retained source replay disarmed when replay is not required"
    );
    assert!(
        !app.sink_state_replay_required(),
        "ordinary steady mixed source/sink/facade ticks must leave retained sink replay disarmed when replay is not required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_only_source_followup_reenters_source_apply_once_when_source_replay_required() {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SourceApplyEntryCountHookReset;

    impl Drop for SourceApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_source_apply_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("replay-source")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("replay-source")).expect("create nfs2 dir");
    fs::write(nfs1.join("replay-source").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("replay-source").join("seed.txt"), b"b").expect("seed nfs2");

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
    let expected_source_groups = std::collections::BTreeSet::new();

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
    app.on_control_frame(&initial).await.expect(
        "generation-two source/sink activate should succeed before replay-only source followup",
    );

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
        if source_groups == expected_source_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before replay-only source followup: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "replay-only source followup requires runtime control to remain initialized before the followup"
    );
    set_source_replay_required_for_tests(&app, true);
    set_sink_replay_required_for_tests(&app, false);

    let source_apply_entries = Arc::new(AtomicUsize::new(0));
    let _source_apply_entry_reset = SourceApplyEntryCountHookReset;
    install_source_apply_entry_count_hook(source_apply_entries.clone());

    app.on_control_frame(&[tick_envelope(
        execution_units::FACADE_RUNTIME_UNIT_ID,
        2,
    )])
    .await
    .expect(
        "replay-only source followup should settle without exhausting runtime_app control recovery",
    );

    assert_eq!(
        source_apply_entries.load(Ordering::Acquire),
        1,
        "source replay-only followup must re-enter runtime_app source.apply exactly once when source replay is required",
    );
    assert!(
        !app.source_state_replay_required(),
        "source replay-only followup must clear retained source replay after replaying current-generation state"
    );
    assert!(
        !app.sink_state_replay_required(),
        "source replay-only followup must not arm retained sink replay when no sink replay is required"
    );
    assert!(
        app.control_initialized(),
        "source replay-only followup must leave runtime control initialized after replay completes"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_only_sink_followup_reenters_sink_apply_once_and_local_sink_status_republish_helper_once_when_sink_replay_required()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SinkApplyEntryCountHookReset;
    struct LocalSinkStatusRepublishHelperEntryCountHookReset;

    impl Drop for SinkApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_sink_apply_entry_count_hook();
        }
    }

    impl Drop for LocalSinkStatusRepublishHelperEntryCountHookReset {
        fn drop(&mut self) {
            clear_local_sink_status_republish_helper_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    fs::create_dir_all(nfs1.join("replay-sink")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2.join("replay-sink")).expect("create nfs2 dir");
    fs::write(nfs1.join("replay-sink").join("seed.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2.join("replay-sink").join("seed.txt"), b"b").expect("seed nfs2");

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
    let expected_source_groups = std::collections::BTreeSet::new();

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
    initial.push(activate_envelope_with_route_key_and_scope_rows(
        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
        &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
        2,
    ));
    app.on_control_frame(&initial).await.expect(
        "generation-two source/sink activate should succeed before replay-only sink followup",
    );

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
        if source_groups == expected_source_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for generation-two scope convergence before replay-only sink followup: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        app.control_initialized(),
        "replay-only sink followup requires runtime control to remain initialized before the followup"
    );
    set_source_replay_required_for_tests(&app, false);
    set_sink_replay_required_for_tests(&app, true);

    let sink_apply_entries = Arc::new(AtomicUsize::new(0));
    let helper_entries = Arc::new(AtomicUsize::new(0));
    let _sink_apply_entry_reset = SinkApplyEntryCountHookReset;
    let _helper_entry_reset = LocalSinkStatusRepublishHelperEntryCountHookReset;
    install_sink_apply_entry_count_hook(sink_apply_entries.clone());
    install_local_sink_status_republish_helper_entry_count_hook(helper_entries.clone());

    app.on_control_frame(&[activate_envelope_with_route_key_and_scope_rows(
        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
        &[("nfs1", &["nfs1"]), ("nfs2", &["nfs2"])],
        2,
    )])
    .await
    .expect(
        "replay-only sink followup should settle without exhausting runtime_app control recovery",
    );

    assert_eq!(
        sink_apply_entries.load(Ordering::Acquire),
        1,
        "sink replay-only followup must re-enter runtime_app sink.apply exactly once when sink replay is required",
    );
    assert_eq!(
        helper_entries.load(Ordering::Acquire),
        1,
        "sink replay-only followup must enter the local sink-status republish helper exactly once when sink replay is required",
    );
    assert!(
        !app.sink_state_replay_required(),
        "sink replay-only followup must clear retained sink replay after replaying current-generation state"
    );
    assert!(
        !app.source_state_replay_required(),
        "sink replay-only followup must not arm retained source replay when only sink replay is required"
    );
    assert!(
        app.control_initialized(),
        "sink replay-only followup must leave runtime control initialized after replay completes"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_led_uninitialized_mixed_recovery_keeps_control_gate_closed_until_local_sink_status_republish_completes()
 {
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct SourceControlErrorHookReset;
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    struct LocalSinkStatusRepublishHelperEntryCountHookReset;
    struct LocalSinkStatusRepublishProbePauseHookReset;

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

    impl Drop for LocalSinkStatusRepublishHelperEntryCountHookReset {
        fn drop(&mut self) {
            clear_local_sink_status_republish_helper_entry_count_hook();
        }
    }

    impl Drop for LocalSinkStatusRepublishProbePauseHookReset {
        fn drop(&mut self) {
            clear_local_sink_status_republish_probe_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
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
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-c-mixed-recovery-gate".into()),
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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let active_facade = match api::spawn(
        app.config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve facade config"),
        app.node_id.clone(),
        app.runtime_boundary.clone(),
        app.source.clone(),
        app.sink.clone(),
        app.query_sink.clone(),
        app.runtime_boundary.clone(),
        app.facade_pending_status.clone(),
        app.facade_service_state.clone(),
        app.api_request_tracker.clone(),
        app.api_control_gate.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 2,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
                    .filter(|group| group.is_ready())
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for local sink readiness before source-led mixed-recovery red"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let initial_gate_ready =
        FacadePublicationMachine::from_input(app.collect_facade_publication_input().await)
        .snapshot()
        .publication_ready;
    assert!(
        initial_gate_ready,
        "precondition: mixed-recovery gate red requires the active facade control stream to start publication-ready"
    );
    app.api_control_gate.set_ready(initial_gate_ready);

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
    assert!(
        !app.control_initialized(),
        "precondition: mixed-recovery red requires runtime to enter uninitialized recovery before the later source-only wave"
    );
    assert!(
        !app.api_control_gate.is_ready(),
        "precondition: request-sensitive fail-closed recovery must close the API control gate before the later source-only wave"
    );

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

    let helper_entries = Arc::new(AtomicUsize::new(0));
    let _helper_entry_reset = LocalSinkStatusRepublishHelperEntryCountHookReset;
    install_local_sink_status_republish_helper_entry_count_hook(helper_entries.clone());
    let helper_probe_entered = Arc::new(Notify::new());
    let helper_probe_release = Arc::new(Notify::new());
    let _probe_pause_reset = LocalSinkStatusRepublishProbePauseHookReset;
    install_local_sink_status_republish_probe_pause_hook(LocalSinkStatusRepublishProbePauseHook {
        entered: helper_probe_entered.clone(),
        release: helper_probe_release.clone(),
    });

    let mut later = source_wave(4);
    later.push(activate_envelope_with_route_key_and_scope_rows(
        execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
        format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
        &[("nfs1", &["listener-a"]), ("nfs2", &["listener-a"])],
        4,
    ));

    let later_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&later).await }
    });

    if tokio::time::timeout(Duration::from_secs(2), helper_probe_entered.notified())
        .await
        .is_err()
    {
        let helper_entries_now = helper_entries.load(Ordering::Acquire);
        let control_gate_ready = app.api_control_gate.is_ready();
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        let source_observability_summary = summarize_source_observability_endpoint(
            &app.source.observability_snapshot_nonblocking().await,
        );
        if later_task.is_finished() {
            let later_result = tokio::time::timeout(Duration::from_secs(1), later_task)
                .await
                .expect("join later source-led mixed-recovery task")
                .expect("join later source-led mixed-recovery task");
            panic!(
                "source-led uninitialized mixed recovery must enter the local sink-status republish helper before reopening the API control gate when retained sink replay and sink-status publication coincide; helper_entries={helper_entries_now} control_gate_ready={control_gate_ready} cached_sink_status={cached_sink_status_summary} source_observability={source_observability_summary} later_result={later_result:?}"
            );
        }
        panic!(
            "source-led uninitialized mixed recovery never reached the local sink-status republish helper probe before stalling; helper_entries={helper_entries_now} control_gate_ready={control_gate_ready} cached_sink_status={cached_sink_status_summary} source_observability={source_observability_summary}"
        );
    }

    assert_eq!(
        helper_entries.load(Ordering::Acquire),
        1,
        "source-led uninitialized mixed recovery must enter the local sink-status republish helper exactly once before reopening the API control gate"
    );
    assert!(
        !app.api_control_gate.is_ready(),
        "source-led uninitialized mixed recovery must keep the API control gate closed while local sink-status republish is still in flight"
    );
    assert!(
        !later_task.is_finished(),
        "source-led uninitialized mixed recovery must stay blocked in the local sink-status republish helper while the control gate remains closed"
    );

    helper_probe_release.notify_waiters();
    let later_result = tokio::time::timeout(Duration::from_secs(5), later_task)
        .await
        .expect("later source-led mixed recovery should complete after local sink-status republish unblocks")
        .expect("join later source-led mixed recovery task");
    if let Err(err) = later_result {
        let cached_sink_status_summary = app
            .sink
            .cached_status_snapshot()
            .map(|snapshot| summarize_sink_status_endpoint(&snapshot))
            .unwrap_or_else(|cached_err| {
                format!("cached_sink_status_unavailable err={cached_err}")
            });
        let source_observability_summary = summarize_source_observability_endpoint(
            &app.source.observability_snapshot_nonblocking().await,
        );
        panic!(
            "source-led uninitialized mixed recovery failed after local sink-status republish unblocked: err={err} cached_sink_status={cached_sink_status_summary} source_observability={source_observability_summary}"
        );
    }

    assert!(
        app.control_initialized(),
        "source-led uninitialized mixed recovery must reinitialize runtime control after local sink-status republish completes"
    );
    assert!(
        app.api_control_gate.is_ready(),
        "source-led uninitialized mixed recovery must reopen the API control gate after local sink-status republish completes"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_sink_owned_query_peer_publication_keeps_control_gate_closed_when_source_replay_regresses_before_gate_reopen()
 {
    let _family_serial = deferred_sink_owned_query_peer_publication_test_serial()
        .lock()
        .await;
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset;
    struct DeferredSinkOwnedQueryPeerPublicationCompletionHookReset;

    impl Drop for DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset {
        fn drop(&mut self) {
            clear_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook();
        }
    }

    impl Drop for DeferredSinkOwnedQueryPeerPublicationCompletionHookReset {
        fn drop(&mut self) {
            clear_deferred_sink_owned_query_peer_publication_completion_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
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
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-c-deferred-query-peer-gate-reopen".into()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init app"),
    );

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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let active_facade = match api::spawn(
        app.config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve facade config"),
        app.node_id.clone(),
        app.runtime_boundary.clone(),
        app.source.clone(),
        app.sink.clone(),
        app.query_sink.clone(),
        app.runtime_boundary.clone(),
        app.facade_pending_status.clone(),
        app.facade_service_state.clone(),
        app.api_request_tracker.clone(),
        app.api_control_gate.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 2,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
                    .filter(|group| group.is_ready())
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for local sink readiness before deferred query-peer publication tail"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let deferred_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    assert!(
        !app.facade_gate
            .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, &deferred_route)
            .expect("query-peer deferred route state before helper"),
        "precondition: deferred query-peer route must start inactive before helper publication"
    );

    let control_ready_after_republish =
        FacadePublicationMachine::from_input(app.collect_facade_publication_input().await)
        .snapshot()
        .publication_ready;
    assert!(
        control_ready_after_republish,
        "precondition: helper tail red requires a stale ready bool captured before source replay regresses"
    );
    app.api_control_gate
        .set_ready(control_ready_after_republish);
    assert!(
        app.api_control_gate.is_ready(),
        "precondition: helper tail red requires the API control gate to start open"
    );

    let gate_reopen_entered = Arc::new(Notify::new());
    let gate_reopen_release = Arc::new(Notify::new());
    let helper_completed = Arc::new(Notify::new());
    let _gate_reopen_reset = DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset;
    let _helper_completed_reset = DeferredSinkOwnedQueryPeerPublicationCompletionHookReset;
    install_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook(
        DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook {
            entered: gate_reopen_entered.clone(),
            release: gate_reopen_release.clone(),
        },
    );
    install_deferred_sink_owned_query_peer_publication_completion_hook(
        DeferredSinkOwnedQueryPeerPublicationCompletionHook {
            entered: helper_completed.clone(),
        },
    );

    set_source_replay_required_for_tests(&app, true);
    app.api_control_gate.set_ready(false);
    assert_eq!(
        app.current_facade_service_state().await,
        FacadeServiceState::Unavailable,
        "source replay regression must first push facade state back to unavailable before the deferred query-peer helper tail completes"
    );

    FSMetaApp::spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
        app.instance_id,
        app.source.clone(),
        app.sink.clone(),
        expected_groups,
        None,
        Vec::new(),
        vec![FacadeControlSignal::Activate {
            unit: FacadeRuntimeUnit::QueryPeer,
            route_key: deferred_route.clone(),
            generation: 2,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["nfs1".to_string()],
            }],
        }],
        app.facade_gate.clone(),
        app.mirrored_query_peer_routes.clone(),
        app.pending_facade.clone(),
        app.facade_pending_status.clone(),
        app.api_task.clone(),
        app.api_control_gate.clone(),
        app.facade_service_state.clone(),
        app.runtime_gate_state.clone(),
        app.runtime_state_changed.clone(),
        app.pending_fixed_bind_has_suppressed_dependent_routes
            .clone(),
    );

    tokio::time::timeout(Duration::from_secs(2), gate_reopen_entered.notified())
        .await
        .expect("deferred query-peer helper must reach the tail gate-reopen point");

    assert!(
        app.facade_gate
            .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, &deferred_route)
            .expect("query-peer deferred route state at gate-reopen pause"),
        "helper must already have published the deferred sink-owned query-peer route before it considers reopening the API control gate"
    );

    gate_reopen_release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), helper_completed.notified())
        .await
        .expect("deferred query-peer helper must complete after the gate-reopen pause releases");

    assert!(
        !app.api_control_gate.is_ready(),
        "deferred sink-owned query-peer publication must recompute the API control gate from current runtime facts instead of reopening it from a stale pre-regression ready bool"
    );
    assert_eq!(
        app.current_facade_service_state().await,
        FacadeServiceState::Unavailable,
        "deferred sink-owned query-peer publication must leave facade state unavailable while source replay is still required"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_sink_owned_query_peer_publication_does_not_overwrite_pending_facade_state_with_serving_when_pending_reappears_before_gate_reopen()
 {
    let _family_serial = deferred_sink_owned_query_peer_publication_test_serial()
        .lock()
        .await;
    let _serial = local_sink_status_republish_helper_test_serial()
        .lock()
        .await;

    struct DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset;
    struct DeferredSinkOwnedQueryPeerPublicationCompletionHookReset;

    impl Drop for DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset {
        fn drop(&mut self) {
            clear_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook();
        }
    }

    impl Drop for DeferredSinkOwnedQueryPeerPublicationCompletionHookReset {
        fn drop(&mut self) {
            clear_deferred_sink_owned_query_peer_publication_completion_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
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
                api: api::ApiConfig {
                    enabled: true,
                    facade_resource_id: "listener-a".to_string(),
                    local_listener_resources: vec![api::config::ApiListenerResource {
                        resource_id: "listener-a".to_string(),
                        bind_addr: bind_addr.clone(),
                    }],
                    auth: api::ApiAuthConfig {
                        passwd_path,
                        shadow_path,
                        ..api::ApiAuthConfig::default()
                    },
                },
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-c-deferred-query-peer-pending-overwrite".into()),
            Some(boundary.clone()),
            Some(boundary),
            state_boundary,
        )
        .expect("init app"),
    );

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

    let mut initial = source_wave(2);
    initial.extend(sink_wave(2));
    app.on_control_frame(&initial)
        .await
        .expect("initial local source/sink wave should succeed");

    let active_facade = match api::spawn(
        app.config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve facade config"),
        app.node_id.clone(),
        app.runtime_boundary.clone(),
        app.source.clone(),
        app.sink.clone(),
        app.query_sink.clone(),
        app.runtime_boundary.clone(),
        app.facade_pending_status.clone(),
        app.facade_service_state.clone(),
        app.api_request_tracker.clone(),
        app.api_control_gate.clone(),
    )
    .await
    {
        Ok(handle) => handle,
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 2,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
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
                    .filter(|group| group.is_ready())
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "timed out waiting for local sink readiness before deferred query-peer publication pending-overwrite red"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let deferred_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    let control_ready_after_republish =
        FacadePublicationMachine::from_input(app.collect_facade_publication_input().await)
        .snapshot()
        .publication_ready;
    assert!(
        control_ready_after_republish,
        "precondition: helper pending-overwrite red requires facade publication to start ready"
    );
    app.api_control_gate
        .set_ready(control_ready_after_republish);
    assert_eq!(
        app.current_facade_service_state().await,
        FacadeServiceState::Serving,
        "precondition: helper pending-overwrite red requires facade to start serving"
    );

    let gate_reopen_entered = Arc::new(Notify::new());
    let gate_reopen_release = Arc::new(Notify::new());
    let helper_completed = Arc::new(Notify::new());
    let _gate_reopen_reset = DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHookReset;
    let _helper_completed_reset = DeferredSinkOwnedQueryPeerPublicationCompletionHookReset;
    install_deferred_sink_owned_query_peer_publication_gate_reopen_pause_hook(
        DeferredSinkOwnedQueryPeerPublicationGateReopenPauseHook {
            entered: gate_reopen_entered.clone(),
            release: gate_reopen_release.clone(),
        },
    );
    install_deferred_sink_owned_query_peer_publication_completion_hook(
        DeferredSinkOwnedQueryPeerPublicationCompletionHook {
            entered: helper_completed.clone(),
        },
    );

    FSMetaApp::spawn_deferred_sink_owned_query_peer_publication_after_local_sink_status_ready(
        app.instance_id,
        app.source.clone(),
        app.sink.clone(),
        expected_groups,
        None,
        Vec::new(),
        vec![FacadeControlSignal::Activate {
            unit: FacadeRuntimeUnit::QueryPeer,
            route_key: deferred_route.clone(),
            generation: 2,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: "nfs1".to_string(),
                resource_ids: vec!["nfs1".to_string()],
            }],
        }],
        app.facade_gate.clone(),
        app.mirrored_query_peer_routes.clone(),
        app.pending_facade.clone(),
        app.facade_pending_status.clone(),
        app.api_task.clone(),
        app.api_control_gate.clone(),
        app.facade_service_state.clone(),
        app.runtime_gate_state.clone(),
        app.runtime_state_changed.clone(),
        app.pending_fixed_bind_has_suppressed_dependent_routes
            .clone(),
    );

    tokio::time::timeout(Duration::from_secs(2), gate_reopen_entered.notified())
        .await
        .expect("deferred query-peer helper must reach the tail gate-reopen point for pending-overwrite red");

    assert!(
        app.facade_gate
            .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, &deferred_route)
            .expect("query-peer deferred route state at pending-overwrite pause"),
        "helper must already have published the deferred sink-owned query-peer route before pending facade state is reintroduced"
    );

    let pending = PendingFacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 3,
        resource_ids: vec!["listener-a".to_string()],
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: "listener-a".to_string(),
            resource_ids: vec!["listener-a".to_string()],
        }],
        group_ids: Vec::new(),
        runtime_managed: true,
        runtime_exposure_confirmed: true,
        resolved: app
            .config
            .api
            .resolve_for_candidate_ids(&["listener-a".to_string()])
            .expect("resolve pending facade config"),
    };
    *app.pending_facade.lock().await = Some(pending.clone());
    FSMetaApp::set_pending_facade_status_waiting(
        &app.facade_pending_status,
        &pending,
        FacadePendingReason::AwaitingObservationEligibility,
    );
    assert_eq!(
        app.current_facade_service_state().await,
        FacadeServiceState::Pending,
        "pending facade reintroduction must first republish pending state before the deferred helper tail completes"
    );
    assert_eq!(
        *app.facade_service_state
            .read()
            .expect("read published facade state before helper completion"),
        FacadeServiceState::Pending,
        "precondition: pending facade reintroduction must mark published facade state pending before helper completion"
    );

    gate_reopen_release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), helper_completed.notified())
        .await
        .expect("deferred query-peer helper must complete after pending-overwrite pause releases");

    assert_eq!(
        *app.facade_service_state
            .read()
            .expect("read published facade state after helper completion"),
        FacadeServiceState::Pending,
        "deferred sink-owned query-peer publication must not overwrite a newly pending facade state with serving when pending reappears before the tail gate reopen"
    );
    assert!(
        !app.api_control_gate.is_ready(),
        "deferred sink-owned query-peer publication must not reopen the API control gate while a new pending control facade is still waiting before the tail gate reopen completes"
    );

    app.close().await.expect("close app");
}
