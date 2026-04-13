#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_partially_stale_split_cache_when_control_is_marked_inflight()
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

    let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let live_snapshot = loop {
        match sink.status_snapshot().await {
            Ok(snapshot)
                if snapshot
                    .groups
                    .iter()
                    .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                    .all(|group| group.initial_audit_completed && group.live_nodes > 0) =>
            {
                break snapshot;
            }
            Ok(_) | Err(CnxError::Timeout) => {}
            Err(err) => panic!("snapshot after materialization: {err:?}"),
        }
        assert!(
            tokio::time::Instant::now() < readiness_deadline,
            "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(
        live_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache: {live_snapshot:?}"
    );

    let mut stale_snapshot = live_snapshot.clone();
    stale_snapshot.scheduled_groups_by_node = std::collections::BTreeMap::from([(
        "node-d".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    for group in &mut stale_snapshot.groups {
        if group.group_id == "nfs1" {
            group.initial_audit_completed = false;
            group.live_nodes = 0;
            group.total_nodes = 0;
            group.shadow_time_us = 0;
        }
    }
    sink.update_cached_status_snapshot(stale_snapshot.clone())
        .expect("seed split stale cache");

    let _inflight = sink.begin_control_op();
    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking during synthetic control inflight");

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must not return a partially stale split cache where one scheduled group regressed to zero while another stayed ready: stale={stale_snapshot:?} live={live_snapshot:?} returned={snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_partially_stale_split_cache_when_worker_unavailable()
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
        "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache: {live_snapshot:?}"
    );

    let mut stale_snapshot = live_snapshot.clone();
    stale_snapshot.scheduled_groups_by_node = std::collections::BTreeMap::from([(
        "node-d".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    for group in &mut stale_snapshot.groups {
        if group.group_id == "nfs1" {
            group.initial_audit_completed = false;
            group.live_nodes = 0;
            group.total_nodes = 0;
            group.shadow_time_us = 0;
            group.materialized_revision = 1;
        }
    }
    sink.update_cached_status_snapshot(stale_snapshot.clone())
        .expect("seed split stale cache");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err("status_snapshot_nonblocking must not return a partially stale split cache when the live worker status call failed");

    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "worker-unavailable path should propagate the live status error instead of returning a contradictory split cached snapshot: {err:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_regress_ready_cached_groups_to_live_partially_stale_split_snapshot()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
            sink_worker_root("nfs3", &nfs3),
        ],
        host_object_grants: vec![
            sink_worker_export("node-b::nfs1", "node-b", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-b::nfs2", "node-b", "10.0.0.42", nfs2.clone()),
            sink_worker_export("node-b::nfs3", "node-b", "10.0.0.43", nfs3.clone()),
        ],
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
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-b::nfs1".to_string(),
                total_nodes: 10,
                live_nodes: 9,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 11,
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-b::nfs2".to_string(),
                total_nodes: 8,
                live_nodes: 7,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_audit: false,
                initial_audit_completed: true,
                materialized_revision: 9,
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
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
            },
        ],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string(), "nfs3".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };
    sink.update_cached_status_snapshot(ready_snapshot.clone())
        .expect("seed ready cached snapshot");

    let live_snapshot = SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-b::nfs1".to_string(),
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
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-b::nfs2".to_string(),
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
                estimated_heap_bytes: 1,
            },
            crate::sink::SinkGroupStatusSnapshot {
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
            },
        ],
        scheduled_groups_by_node: ready_snapshot.scheduled_groups_by_node.clone(),
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
                "status_snapshot_nonblocking must preserve the ready cached groups instead of publishing a live partially stale split snapshot",
            );

    assert!(
        snapshot_has_ready_scheduled_groups(&snapshot),
        "status_snapshot_nonblocking must not regress ready cached groups to a live partially stale split snapshot where nfs1/nfs2 reopened as zero while nfs3 stayed ready: ready={ready_snapshot:?} live={live_snapshot:?} returned={snapshot:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached sink status after live partially stale split snapshot");
    assert!(
        snapshot_has_ready_scheduled_groups(&cached_snapshot),
        "the live partially stale split snapshot must not overwrite the ready cached status summary: ready={ready_snapshot:?} live={live_snapshot:?} cached={cached_snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_partially_stale_split_cache_when_worker_not_started()
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

    let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let live_snapshot = loop {
        match sink.status_snapshot().await {
            Ok(snapshot)
                if snapshot
                    .groups
                    .iter()
                    .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                    .all(|group| group.initial_audit_completed && group.live_nodes > 0) =>
            {
                break snapshot;
            }
            Ok(_) | Err(CnxError::Timeout) => {}
            Err(err) => panic!("snapshot after materialization: {err:?}"),
        }
        assert!(
            tokio::time::Instant::now() < readiness_deadline,
            "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(
        live_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache: {live_snapshot:?}"
    );

    let mut stale_snapshot = live_snapshot.clone();
    stale_snapshot.scheduled_groups_by_node = std::collections::BTreeMap::from([(
        "node-d".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    for group in &mut stale_snapshot.groups {
        if group.group_id == "nfs1" {
            group.initial_audit_completed = false;
            group.live_nodes = 0;
            group.total_nodes = 0;
            group.shadow_time_us = 0;
            group.materialized_revision = 1;
        }
    }
    sink.update_cached_status_snapshot(stale_snapshot.clone())
        .expect("seed split stale cache");

    sink.shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown worker to force not-started cache path");

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err("status_snapshot_nonblocking must not return a partially stale split cache when no live worker is available");

    assert!(
        matches!(
            err,
            CnxError::Timeout
                | CnxError::TransportClosed(_)
                | CnxError::ProtocolViolation(_)
                | CnxError::Internal(_)
        ),
        "not-started path should fail closed instead of returning a contradictory split cached snapshot: {err:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}
