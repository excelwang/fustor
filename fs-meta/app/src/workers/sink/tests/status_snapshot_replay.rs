#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_scheduled_zero_uninitialized_cache_when_worker_unavailable()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

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
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let live_snapshot = sink
        .status_snapshot()
        .await
        .expect("snapshot after materialization");
    assert!(
        live_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before seeding a scheduled zero cache: {live_snapshot:?}"
    );

    let mut zero_snapshot = live_snapshot.clone();
    zero_snapshot.scheduled_groups_by_node = std::collections::BTreeMap::from([(
        "node-d".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    for group in &mut zero_snapshot.groups {
        if group.group_id == "nfs1" || group.group_id == "nfs2" {
            group.primary_object_ref = "unassigned".to_string();
            group.initial_audit_completed = false;
            group.live_nodes = 0;
            group.total_nodes = 0;
            group.tombstoned_count = 0;
            group.attested_count = 0;
            group.suspect_count = 0;
            group.blind_spot_count = 0;
            group.shadow_time_us = 0;
            group.shadow_lag_us = 0;
            group.overflow_pending_audit = false;
            group.materialized_revision = 1;
            group.estimated_heap_bytes = 0;
        }
    }
    sink.update_cached_status_snapshot(zero_snapshot.clone())
        .expect("seed zero scheduled cache");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err("status_snapshot_nonblocking must not publish a scheduled-but-fully-zero sink snapshot when the live sink worker status call failed");

    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "worker-unavailable path should propagate the live status error instead of returning a scheduled zero/uninitialized snapshot: stale={zero_snapshot:?} err={err:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_republishes_scheduled_groups_into_cached_summary_when_live_status_fails_from_stale_empty_cache_after_schedule_convergence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

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
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("snapshot after materialization");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before probing local sink-status republish: {ready_snapshot:?}"
    );

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing stale empty cache publication"
    );

    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("seed stale empty cached status after ready materialization");

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed when the cached status summary is empty and the live sink worker status call failed",
            );
    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "live status failure should be propagated instead of returning an empty cached sink-status summary: {err:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached status summary after fail-closed nonblocking status");
    let cached_scheduled = cached_snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        cached_scheduled, scheduled,
        "after schedule convergence, a fail-closed nonblocking status probe must still republish the cached scheduled groups instead of leaving the local sink-status summary completely empty: ready={ready_snapshot:?} cached={cached_snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_snapshot_nonblocking_does_not_regress_ready_cached_groups_to_live_missing_scheduled_rows_with_stream_evidence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs3", &nfs3)],
        host_object_grants: vec![sink_worker_export(
            "node-b::nfs3",
            "node-b",
            "10.0.0.43",
            nfs3.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let ready_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
            total_nodes: 6,
            live_nodes: 5,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_applied_batches_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            1,
        )]),
        stream_applied_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            15,
        )]),
        stream_applied_control_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            3,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            12,
        )]),
        stream_applied_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_last_applied_at_us_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            42,
        )]),
        ..SinkStatusSnapshot::default()
    };
    sink.update_cached_status_snapshot(ready_snapshot.clone())
        .expect("seed ready cached snapshot");

    let live_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: ready_snapshot.scheduled_groups_by_node.clone(),
        stream_ready_origin_counts_by_node: ready_snapshot
            .stream_ready_origin_counts_by_node
            .clone(),
        stream_applied_batches_by_node: ready_snapshot.stream_applied_batches_by_node.clone(),
        stream_applied_events_by_node: ready_snapshot.stream_applied_events_by_node.clone(),
        stream_applied_control_events_by_node: ready_snapshot
            .stream_applied_control_events_by_node
            .clone(),
        stream_applied_data_events_by_node: ready_snapshot
            .stream_applied_data_events_by_node
            .clone(),
        stream_applied_origin_counts_by_node: ready_snapshot
            .stream_applied_origin_counts_by_node
            .clone(),
        stream_last_applied_at_us_by_node: ready_snapshot.stream_last_applied_at_us_by_node.clone(),
        ..SinkStatusSnapshot::default()
    };

    let _reset = SinkWorkerStatusSnapshotHookReset;
    install_sink_worker_status_snapshot_hook(SinkWorkerStatusSnapshotHook {
        snapshot: live_snapshot.clone(),
    });

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking must preserve the ready cached group instead of publishing a live scheduled-with-stream-evidence snapshot that dropped the group row",
            );

    assert!(
        snapshot_has_ready_scheduled_groups(&snapshot),
        "status_snapshot_nonblocking must not regress ready cached groups to a live snapshot that keeps scheduled nfs3 stream evidence but drops the group row: ready={ready_snapshot:?} live={live_snapshot:?} returned={snapshot:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached sink status after degraded live snapshot");
    assert!(
        snapshot_has_ready_scheduled_groups(&cached_snapshot),
        "the degraded live snapshot must not overwrite the ready cached status summary: ready={ready_snapshot:?} live={live_snapshot:?} cached={cached_snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_regress_ready_cached_groups_to_live_replayed_zero_state_after_worker_restart()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

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
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before restart");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before restarting the sink worker: {ready_snapshot:?}"
    );

    sink.shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown sink worker before replay-only restart");
    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");

    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking after replay-only worker restart");

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must not regress previously ready groups to a replay-only zero-state snapshot after worker restart: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_worker_restart_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before restart");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before restarting the no-grant sink worker: {ready_snapshot:?}"
    );

    sink.shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown sink worker before replay-only restart");
    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");
    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after retained replay worker restart should return a restored snapshot",
            );

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after worker restart and retained replay instead of reopening with scheduled zero groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_retry_reset_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before retry reset");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before retry reset: {ready_snapshot:?}"
    );

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retry-reset retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after retry reset retained replay should return a restored snapshot",
            );

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "retry-reset retained replay should reset the shared sink worker client before publishing status",
    );
    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after retry reset and retained replay instead of reopening with scheduled zero groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_same_instance_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before same-instance retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after same-instance retained replay should return a restored snapshot",
            );

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after same-instance retained replay instead of leaving the local sink-status summary empty: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_settles_within_runtime_app_probe_budget_after_same_instance_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before same-instance retained replay");

    let result = tokio::time::timeout(
        Duration::from_millis(350),
        sink.status_snapshot_nonblocking(),
    )
    .await;

    let result = result.expect(
            "status_snapshot_nonblocking must settle within the runtime-app local sink-status republish probe budget after same-instance retained replay",
        );
    match result {
        Ok(snapshot) => assert!(
            snapshot
                .groups
                .iter()
                .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                .all(|group| group.initial_audit_completed && group.live_nodes > 0),
            "status_snapshot_nonblocking must either restore ready state within the probe budget or fail-close, not return a degraded snapshot: ready={ready_snapshot:?} returned={snapshot:?}"
        ),
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "status_snapshot_nonblocking within the runtime-app local sink-status republish probe budget must restore ready state or fail-close, got {err}"
        ),
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_settles_within_runtime_app_probe_budget_after_same_instance_replay_completed()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete same-instance retained replay before local republish probe");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after same-instance retained replay completed");

    let result = tokio::time::timeout(
        Duration::from_millis(350),
        sink.status_snapshot_nonblocking(),
    )
    .await;

    let result = result.expect(
            "status_snapshot_nonblocking must settle within the runtime-app local sink-status republish probe budget after same-instance retained replay already completed",
        );
    match result {
        Ok(snapshot) => assert!(
            snapshot
                .groups
                .iter()
                .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                .all(|group| group.initial_audit_completed && group.live_nodes > 0),
            "status_snapshot_nonblocking after replay completion must restore ready state within the probe budget or fail-close, not return a degraded snapshot: ready={ready_snapshot:?} returned={snapshot:?}"
        ),
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "status_snapshot_nonblocking after replay completion within the runtime-app local sink-status republish probe budget must restore ready state or fail-close, got {err}"
        ),
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_eventually_restores_ready_groups_after_second_same_instance_replay_completed_without_new_source_events()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before second same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before second same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete first same-instance retained replay before later recovery");
    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete second same-instance retained replay before local republish wait");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after second same-instance retained replay completed");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after second retained replay")
            .unwrap_or_default();
        assert_eq!(
            scheduled, expected_groups,
            "scheduled groups must stay converged while local sink-status republish waits after second same-instance retained replay"
        );

        match tokio::time::timeout(
            Duration::from_millis(350),
            sink.status_snapshot_nonblocking(),
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
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            let cached_snapshot = sink
                .cached_status_snapshot()
                .expect("cached status after second same-instance retained replay");
            let blocking_sink_status =
                match tokio::time::timeout(Duration::from_secs(2), sink.status_snapshot()).await {
                    Ok(Ok(snapshot)) => format!("{snapshot:?}"),
                    Ok(Err(err)) => format!("blocking_status_err={err}"),
                    Err(_) => "blocking_status_timeout".to_string(),
                };
            panic!(
                "second same-instance retained replay should eventually restore ready groups without new source events instead of leaving local sink-status stuck empty: cached={cached_snapshot:?} blocking={blocking_sink_status}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_ready_groups_after_explicit_same_instance_retained_wave_without_new_source_events()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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

    let root_scopes = vec![
        bound_scope_with_resources("nfs1", &["nfs1"]),
        bound_scope_with_resources("nfs2", &["nfs2"]),
    ];
    let explicit_replay_wave = || {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink query activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
                unit_id: "runtime.exec.sink".to_string(),
                generation: 2,
                at_ms: 1,
            })
            .expect("encode explicit retained sink replay tick"),
        ]
    };

    sink.on_control_frame(explicit_replay_wave())
        .await
        .expect("apply initial sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before explicit replay wave");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before explicit same-instance replay wave: {ready_snapshot:?}"
    );

    sink.on_control_frame(explicit_replay_wave())
        .await
        .expect("apply explicit same-instance retained sink replay wave");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after explicit same-instance replay wave");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after explicit same-instance replay wave")
            .unwrap_or_default();
        assert_eq!(
            scheduled, expected_groups,
            "scheduled groups must stay converged while local sink-status republish waits after the explicit same-instance retained sink replay wave"
        );

        match tokio::time::timeout(
            Duration::from_millis(350),
            sink.status_snapshot_nonblocking(),
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
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            let cached_snapshot = sink
                .cached_status_snapshot()
                .expect("cached status after explicit same-instance replay wave");
            let blocking_sink_status =
                match tokio::time::timeout(Duration::from_secs(2), sink.status_snapshot()).await {
                    Ok(Ok(snapshot)) => format!("{snapshot:?}"),
                    Ok(Err(err)) => format!("blocking_status_err={err}"),
                    Err(_) => "blocking_status_timeout".to_string(),
                };
            panic!(
                "explicit same-instance retained sink replay wave should eventually restore ready groups without new source events instead of leaving local sink-status stuck empty: cached={cached_snapshot:?} blocking={blocking_sink_status}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_republishes_scheduled_groups_into_cached_summary_before_runtime_app_probe_budget_cancels_post_replay_status_refresh()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before runtime-app probe cancellation seam");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before probing post-replay local status cancellation seam: {ready_snapshot:?}"
    );

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after ready materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing the post-replay local sink-status seam"
    );

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before post-replay local status probe");

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        sink.status_snapshot_nonblocking(),
    )
    .await;
    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached status summary after bounded post-replay local status probe");
    let cached_scheduled = cached_snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        cached_scheduled, scheduled,
        "when the runtime-app probe budget expires during post-replay local status refresh, the cached sink-status summary must still republish the converged scheduled groups instead of staying fully empty: result={result:?} cached={cached_snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_restores_persisted_root_id_ready_state_after_retry_reset_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
            "nfs1",
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
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
            "nfs2",
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
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before retry reset");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before retry reset: {ready_snapshot:?}"
    );

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retry-reset retained replay");

    let snapshot = sink
            .status_snapshot()
            .await
            .expect("blocking status_snapshot after retry reset retained replay should return a restored snapshot");

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "blocking retry-reset retained replay should reset the shared sink worker client before publishing status",
    );
    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "blocking status_snapshot must restore durably persisted root-id ready state after retry reset and retained replay instead of timing out or reopening with not-ready groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_report_replay_complete_when_live_snapshot_is_scheduled_zero_uninitialized()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &[]),
                bound_scope_with_resources("nfs2", &[]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before replay-required status");

    sink.control_state_replay_required
        .store(1, Ordering::Release);

    let err = sink
            .status_snapshot()
            .await
            .expect_err("status_snapshot must not report retained replay complete while the live sink status is still a scheduled zero/uninitialized snapshot");

    assert!(
        matches!(err, CnxError::Timeout),
        "replay-required blocking status must fail close instead of reporting a scheduled zero/uninitialized snapshot as success: {err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_publish_scheduled_zero_uninitialized_snapshot_after_replay_cleared()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &[]),
                bound_scope_with_resources("nfs2", &[]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before blocking status");

    sink.control_state_replay_required
        .store(0, Ordering::Release);

    let err = sink
            .status_snapshot()
            .await
            .expect_err("status_snapshot must fail close instead of publishing a scheduled zero/uninitialized snapshot after retained replay has already been cleared");

    assert!(
        matches!(err, CnxError::Timeout),
        "blocking status_snapshot must fail close on scheduled zero/uninitialized groups even after retained replay was already cleared: {err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_fails_closed_when_live_snapshot_is_single_scheduled_zero_uninitialized_with_bound_primary_object_ref()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs3", &nfs3)],
        host_object_grants: vec![sink_worker_export(
            "node-b::nfs3",
            "node-b",
            "10.0.0.43",
            nfs3.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b-29795712685086500907384833".to_string()),
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
            bound_scopes: vec![bound_scope_with_resources("nfs3", &["node-b::nfs3"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before zero-state status probe");

    let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after activate")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs3".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduled_deadline,
            "single-group activate should converge scheduled nfs3 before probing the live zero-state seam: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let raw_client = sink.client().await.expect("typed sink worker client");
    let raw_snapshot = match SinkWorkerClientHandle::call_worker(
        &raw_client,
        SinkWorkerRequest::StatusSnapshot,
        SINK_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect("raw status snapshot RPC should succeed before fail-close wrapper")
    {
        SinkWorkerResponse::StatusSnapshot(snapshot) => snapshot,
        other => panic!("unexpected raw sink worker status response: {other:?}"),
    };
    let raw_group = raw_snapshot
        .groups
        .iter()
        .find(|group| group.group_id == "nfs3")
        .expect("raw live sink snapshot should include scheduled nfs3");
    assert_eq!(
        raw_snapshot
            .scheduled_groups_by_node
            .values()
            .flat_map(|groups| groups.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["nfs3".to_string()]),
        "precondition: raw live sink snapshot must already carry scheduled nfs3"
    );
    assert!(
        !raw_group.initial_audit_completed
            && raw_group.live_nodes == 0
            && raw_group.total_nodes == 0
            && raw_group.materialized_revision <= 1,
        "precondition: raw live sink snapshot must still be a zero/uninitialized scheduled group: {raw_snapshot:?}"
    );
    assert_ne!(
        raw_group.primary_object_ref, "unassigned",
        "precondition: this seam needs a concrete primary object ref so the live zero-state can slip past the current scheduled-zero guard: {raw_snapshot:?}"
    );
    assert_ne!(
        raw_group.primary_object_ref, raw_group.group_id,
        "precondition: this seam needs a bound primary object ref distinct from the group id so the live zero-state can slip past the current scheduled-zero guard: {raw_snapshot:?}"
    );

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed instead of publishing a single scheduled zero/uninitialized group whose live primary_object_ref is a concrete bound grant ref",
            );

    assert!(
        matches!(err, CnxError::Timeout),
        "single scheduled zero/uninitialized group with a bound primary_object_ref must fail close with timeout instead of reaching runtime-app as an ok empty-root summary: err={err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_rearm_same_retained_replay_after_zero_uninitialized_reply() {
    struct SinkWorkerControlFramePauseHookReset;

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
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
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
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
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink roots-control activate"),
    ])
    .await
    .expect("apply retained replay control wave before blocking status");

    sink.control_state_replay_required
        .store(1, Ordering::Release);

    let first_err = sink
            .status_snapshot()
            .await
            .expect_err("first blocking status_snapshot must fail close on a zero/uninitialized retained replay snapshot");

    assert!(
        matches!(first_err, CnxError::Timeout),
        "first blocking status_snapshot must fail close on a zero/uninitialized retained replay snapshot: {first_err:?}"
    );
    assert_eq!(
        sink.control_state_replay_required.load(Ordering::Acquire),
        0,
        "blocking status_snapshot must not re-arm the same retained replay after it already replayed the retained three-envelope sink wave and still saw a zero/uninitialized snapshot"
    );

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _pause_reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_status = tokio::spawn({
        let sink = sink.clone();
        async move { sink.status_snapshot().await }
    });

    let pause_entered = tokio::time::timeout(Duration::from_millis(600), entered.notified()).await;
    if pause_entered.is_ok() {
        release.notify_waiters();
        let _ = second_status.await;
        panic!(
            "second blocking status_snapshot must not replay the same retained three-envelope sink wave again after the first zero/uninitialized fail-close"
        );
    }

    let second_err = tokio::time::timeout(Duration::from_secs(2), second_status)
            .await
            .expect("second blocking status_snapshot should settle promptly without replaying retained control")
            .expect("join second blocking status_snapshot")
            .expect_err(
                "second blocking status_snapshot should still fail close on the zero/uninitialized snapshot",
            );
    assert!(
        matches!(second_err, CnxError::Timeout),
        "second blocking status_snapshot should fail close without replaying retained control: {second_err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_query_still_reads_local_payload_while_control_inflight() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

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
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink query route");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        if ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "initial materialization should exist before sink control pauses"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let inflight_control = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 3,
                            expires_at_ms: 1,
                            bound_scopes: vec![bound_scope_with_resources(
                                "nfs1",
                                &["node-d::nfs1"],
                            )],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ],
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("sink control should reach pause point");

    let query = tokio::time::timeout(
        Duration::from_millis(800),
        sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1")),
    )
    .await;

    release.notify_waiters();
    let _ = inflight_control.await.expect("join inflight control");

    let events = query
        .expect("blocking materialized_query should still settle while sink control is in flight")
        .expect("blocking materialized_query during control inflight");
    assert!(
        decode_exact_query_node(events, b"/force-find-stress")
            .expect("decode query during inflight")
            .is_some(),
        "blocking materialized_query during sink control inflight must still return the last local materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

