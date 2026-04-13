#[tokio::test]
async fn successor_recovery_fails_closed_on_shared_control_frame_lease_timeout_across_runtime_instances_with_distinct_worker_bindings()
{
    struct SourceWorkerStartPauseHookReset;

    impl Drop for SourceWorkerStartPauseHookReset {
        fn drop(&mut self) {
            crate::workers::source::clear_source_worker_start_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let fs_source = tmp.path().display().to_string();
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();

    let make_app = |source_socket_dir: &Path, sink_socket_dir: &Path| {
        Arc::new(
            FSMetaApp::with_boundaries_and_state(
                FSMetaConfig {
                    source: SourceConfig {
                        roots: vec![worker_fs_source_root("test-root", &fs_source)],
                        host_object_grants: vec![worker_export_with_fs_source(
                            "single-app-node::root-1",
                            "single-app-node",
                            "127.0.0.1",
                            &fs_source,
                            tmp.path().to_path_buf(),
                        )],
                        ..SourceConfig::default()
                    },
                    ..FSMetaConfig::default()
                },
                external_runtime_worker_binding("source", source_socket_dir),
                external_runtime_worker_binding("sink", sink_socket_dir),
                NodeId("single-app-node".into()),
                Some(boundary.clone()),
                Some(boundary.clone()),
                state_boundary.clone(),
            )
            .expect("init external-worker app"),
        )
    };

    let predecessor_socket_root = worker_socket_tempdir();
    let predecessor_source_socket_dir =
        worker_role_socket_dir(predecessor_socket_root.path(), "source");
    let predecessor_sink_socket_dir =
        worker_role_socket_dir(predecessor_socket_root.path(), "sink");
    fs::create_dir_all(&predecessor_source_socket_dir).expect("create predecessor source socket");
    fs::create_dir_all(&predecessor_sink_socket_dir).expect("create predecessor sink socket");
    let predecessor = make_app(&predecessor_source_socket_dir, &predecessor_sink_socket_dir);

    let successor_socket_root = worker_socket_tempdir();
    let successor_source_socket_dir =
        worker_role_socket_dir(successor_socket_root.path(), "source");
    let successor_sink_socket_dir =
        worker_role_socket_dir(successor_socket_root.path(), "sink");
    fs::create_dir_all(&successor_source_socket_dir).expect("create successor source socket");
    fs::create_dir_all(&successor_sink_socket_dir).expect("create successor sink socket");
    let successor = make_app(&successor_source_socket_dir, &successor_sink_socket_dir);

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = SourceWorkerStartPauseHookReset;
    crate::workers::source::install_source_worker_start_pause_hook(
        crate::workers::source::SourceWorkerStartPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let predecessor_recovery = tokio::spawn({
        let predecessor = predecessor.clone();
        async move {
            predecessor
                .on_control_frame(&[activate_envelope("runtime.exec.source")])
                .await
        }
    });

    entered.notified().await;

    let mut successor_recovery = tokio::spawn({
        let successor = successor.clone();
        async move {
            successor
                .on_control_frame(&[activate_envelope("runtime.exec.source")])
                .await
        }
    });

    assert!(
        tokio::time::timeout(
            CONTROL_FRAME_LEASE_ACQUIRE_BUDGET + Duration::from_millis(350),
            &mut successor_recovery,
        )
        .await
        .is_err(),
        "same-node successor should keep waiting for predecessor source.start handoff instead of fail-closing on the shared control-frame lease budget",
    );

    release.notify_waiters();

    predecessor_recovery
        .await
        .expect("join predecessor recovery task")
        .expect("predecessor recovery should succeed");
    successor_recovery
        .await
        .expect("join successor recovery task")
        .expect("successor recovery should succeed after predecessor releases the shared lease");

    successor.close().await.expect("close successor app");
    predecessor.close().await.expect("close predecessor app");
}
