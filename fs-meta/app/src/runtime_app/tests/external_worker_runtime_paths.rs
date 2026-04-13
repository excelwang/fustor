#[test]
fn runtime_test_worker_module_path_prefers_newer_debug_deps_cdylib_over_stale_top_level_debug_cdylib()
 {
    let tmp = tempdir().expect("create temp dir");
    let lib_name = fs_meta_runtime_lib_filename();
    let stale = tmp.path().join("target/debug").join(lib_name);
    let fresh = tmp.path().join("target/debug/deps").join(lib_name);
    std::fs::create_dir_all(stale.parent().expect("stale parent")).expect("create stale dir");
    std::fs::create_dir_all(fresh.parent().expect("fresh parent")).expect("create fresh dir");
    std::fs::write(&stale, b"stale").expect("write stale module");
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(&fresh, b"fresh").expect("write fresh module");

    let resolved = resolve_runtime_test_worker_module_path_from_workspace_root(tmp.path())
        .expect("resolve worker module path");

    assert_eq!(
        resolved, fresh,
        "runtime_app external-worker tests must select the freshest built fs-meta cdylib instead of a stale top-level debug artifact"
    );
}

#[test]
fn control_frame_lease_path_is_stable_per_node_and_binding_even_across_socket_roots() {
    let worker_socket_root_a = worker_socket_tempdir();
    let source_socket_dir_a = worker_role_socket_dir(worker_socket_root_a.path(), "source");
    let sink_socket_dir_a = worker_role_socket_dir(worker_socket_root_a.path(), "sink");

    let worker_socket_root_b = worker_socket_tempdir();
    let source_socket_dir_b = worker_role_socket_dir(worker_socket_root_b.path(), "source");
    let sink_socket_dir_b = worker_role_socket_dir(worker_socket_root_b.path(), "sink");

    let binding_source_a = external_runtime_worker_binding("source", &source_socket_dir_a);
    let binding_sink_a = external_runtime_worker_binding("sink", &sink_socket_dir_a);
    let binding_source_b = external_runtime_worker_binding("source", &source_socket_dir_b);
    let binding_sink_b = external_runtime_worker_binding("sink", &sink_socket_dir_b);

    let first = shared_control_frame_lease_path_for_runtime(
        &NodeId("node-a".into()),
        &binding_source_a,
        &binding_sink_a,
    )
    .expect("external runtime bindings should derive a cross-process lease path");
    let second = shared_control_frame_lease_path_for_runtime(
        &NodeId("node-a".into()),
        &binding_source_b,
        &binding_sink_b,
    )
    .expect("external runtime bindings should derive the same lease path");

    assert_eq!(
        first, second,
        "same node and logical runtime worker binding must resolve to one stable control-frame lease path even when successor and predecessor use different socket roots"
    );
    assert!(
        first
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains("node-a")),
        "lease path should remain node-scoped so distinct nodes do not contend on the same control-frame lease: {first:?}"
    );
}

#[test]
fn control_frame_lease_guard_is_exclusive_for_same_runtime_path() {
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let binding_source = external_runtime_worker_binding("source", &source_socket_dir);
    let binding_sink = external_runtime_worker_binding("sink", &sink_socket_dir);
    let lease_path = shared_control_frame_lease_path_for_runtime(
        &NodeId("node-a".into()),
        &binding_source,
        &binding_sink,
    )
    .expect("external runtime bindings should derive a cross-process lease path");

    let _first_guard = ControlFrameLeaseGuard::try_acquire(&lease_path)
        .expect("first control-frame lease acquisition should succeed")
        .expect("first control-frame lease acquisition should return a guard");
    let second_guard = ControlFrameLeaseGuard::try_acquire(&lease_path)
        .expect("second control-frame lease attempt should complete");
    assert!(
        second_guard.is_none(),
        "a second independent open on the same control-frame lease path must block/fail until the first guard is dropped"
    );
}

#[test]
fn control_frame_lease_path_exists_for_external_bindings_without_explicit_socket_dirs() {
    let binding_source = default_runtime_worker_binding("source", WorkerMode::External, None);
    let binding_sink = default_runtime_worker_binding("sink", WorkerMode::External, None);

    let lease_path = shared_control_frame_lease_path_for_runtime(
            &NodeId("node-a".into()),
            &binding_source,
            &binding_sink,
        )
        .expect("external runtime bindings should still derive a cross-process lease path even when socket_dir is implicit");

    assert!(
        lease_path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains("node-a")),
        "implicit-socket external bindings should still resolve to a node-scoped lease path: {lease_path:?}"
    );
}
