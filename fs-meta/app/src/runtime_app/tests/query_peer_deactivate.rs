#[tokio::test]
async fn query_peer_deactivate_does_not_wait_for_inflight_internal_sink_status_request() {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let fs_source = tmp.path().display().to_string();
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
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    if cfg!(target_os = "linux") {
        match app.start().await {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("start app: {err}"),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

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
            app.close().await.expect("close app after bind restriction");
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    app.on_control_frame(&[
        activate_envelope_with_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            &[("test-root", &["listener-a"])],
            2,
        ),
    ])
    .await
    .expect("activate sink + query-peer sink-status");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _pause_reset = RuntimeProxyRequestPauseHookReset {
        label: "sink_status",
    };
    install_runtime_proxy_request_pause_hook(
        "sink_status",
        RuntimeProxyRequestPauseHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );

    let request_task = tokio::spawn({
        let boundary = boundary.clone();
        async move { internal_sink_status_request(boundary, NodeId("single-app-node".into())).await }
    });

    entered.notified().await;

    let mut deactivate_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[deactivate_envelope_with_route_key(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                3,
            )])
            .await
        }
    });

    let deactivate_result =
        tokio::time::timeout(Duration::from_millis(600), &mut deactivate_task).await;
    assert!(
        deactivate_result.is_ok(),
        "query-peer sink-status deactivate should not wait for an in-flight internal sink-status request"
    );

    release.notify_waiters();
    let request_result = tokio::time::timeout(Duration::from_secs(2), request_task).await;
    match request_result {
        Ok(joined) => {
            let events = joined
                .expect("join sink-status request")
                .expect("internal sink-status request should settle after route deactivate");
            assert_eq!(
                events.len(),
                1,
                "internal sink-status request should emit exactly one explicit reply after route deactivate"
            );
            let snapshot =
                rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(events[0].payload_bytes())
                    .expect("decode sink-status snapshot after route deactivate");
            assert!(
                snapshot.groups.is_empty() && snapshot.scheduled_groups_by_node.is_empty(),
                "deactivated sink-status reply should fail closed with an explicit empty snapshot: {snapshot:?}"
            );
        }
        Err(_) => {
            let reply_channel = format!("{}.req:reply", ROUTE_KEY_SINK_STATUS_INTERNAL);
            let reply_events = boundary
                .channels
                .try_lock()
                .ok()
                .and_then(|channels| channels.get(&reply_channel).map(|events| events.len()));
            let endpoint_reasons = app.runtime_endpoint_tasks.try_lock().ok().map(|tasks| {
                tasks
                    .iter()
                    .map(|task| {
                        (
                            task.route_key().to_string(),
                            task.is_finished(),
                            task.finish_reason(),
                        )
                    })
                    .collect::<Vec<_>>()
            });
            panic!(
                "internal sink-status request did not finish within 2s after route deactivate; reply_events={reply_events:?}; endpoint_reasons={endpoint_reasons:?}"
            );
        }
    }
    deactivate_result
        .expect("join sink-status deactivate via timeout completion")
        .expect("join sink-status deactivate task")
        .expect("deactivate query-peer sink-status after request drain");

    app.close().await.expect("close app");
}

#[tokio::test]
async fn query_peer_internal_status_deactivate_does_not_wait_for_inflight_source_control_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let fs_source = tmp.path().display().to_string();
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
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    if cfg!(target_os = "linux") {
        match app.start().await {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("start app: {err}"),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

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
            app.close().await.expect("close app after bind restriction");
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    app.on_control_frame(&[
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            &[("test-root", &["listener-a"])],
            2,
        ),
    ])
    .await
    .expect("activate source/sink + query-peer source-status");

    let source_entered = Arc::new(Notify::new());
    let source_release = Arc::new(Notify::new());
    let _source_pause_reset = SourceWorkerControlFramePauseHookReset;
    crate::workers::source::install_source_worker_control_frame_pause_hook(
        crate::workers::source::SourceWorkerControlFramePauseHook {
            entered: source_entered.clone(),
            release: source_release.clone(),
        },
    );

    let source_control_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[activate_envelope_with_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                3,
            )])
            .await
        }
    });

    source_entered.notified().await;

    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    let mut deactivate_task = tokio::spawn({
        let app = app.clone();
        let source_status_route = source_status_route.clone();
        async move {
            app.apply_facade_deactivate(FacadeRuntimeUnit::QueryPeer, &source_status_route, 3, false)
                .await
        }
    });

    let deactivate_result =
        tokio::time::timeout(Duration::from_millis(600), &mut deactivate_task).await;
    source_release.notify_waiters();
    source_control_task
        .await
        .expect("join source control followup")
        .expect("source control followup should finish");
    if deactivate_result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut deactivate_task).await;
    }
    assert!(
        deactivate_result.is_ok(),
        "query-peer source-status deactivate should not wait for unrelated in-flight source control handoff"
    );
    deactivate_result
        .expect("join source-status deactivate via timeout completion")
        .expect("join source-status deactivate task")
        .expect("deactivate query-peer source-status route");

    app.close().await.expect("close app");
}

#[tokio::test]
async fn stale_query_peer_sink_query_proxy_deactivate_does_not_wait_for_inflight_sink_control_handoff(
) {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let fs_source = tmp.path().display().to_string();
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
            NodeId("single-app-node".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    if cfg!(target_os = "linux") {
        match app.start().await {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("start app: {err}"),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

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
            app.close().await.expect("close app after bind restriction");
            return;
        }
        Err(err) => panic!("spawn active facade: {err}"),
    };
    *app.api_task.lock().await = Some(FacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let sink_query_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
    app.on_control_frame(&[
        activate_envelope_with_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            &[("test-root", &["single-app-node::root-1"])],
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            sink_query_proxy_route.clone(),
            &[("test-root", &["listener-a"])],
            2,
        ),
    ])
    .await
    .expect("activate source/sink + query-peer sink-query-proxy");

    let sink_entered = Arc::new(Notify::new());
    let sink_release = Arc::new(Notify::new());
    let _sink_pause_reset = SinkWorkerControlFramePauseHookReset;
    crate::workers::sink::install_sink_worker_control_frame_pause_hook(
        crate::workers::sink::SinkWorkerControlFramePauseHook {
            entered: sink_entered.clone(),
            release: sink_release.clone(),
        },
    );

    let sink_control_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                &[("test-root", &["single-app-node::root-1"])],
                3,
            )])
            .await
        }
    });

    sink_entered.notified().await;

    let mut deactivate_task = tokio::spawn({
        let app = app.clone();
        let sink_query_proxy_route = sink_query_proxy_route.clone();
        async move {
            app.apply_facade_deactivate(FacadeRuntimeUnit::QueryPeer, &sink_query_proxy_route, 1, false)
                .await
        }
    });

    let deactivate_result =
        tokio::time::timeout(Duration::from_millis(600), &mut deactivate_task).await;
    sink_release.notify_waiters();
    sink_control_task
        .await
        .expect("join sink control followup")
        .expect("sink control followup should finish");
    if deactivate_result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut deactivate_task).await;
    }
    assert!(
        deactivate_result.is_ok(),
        "stale query-peer sink-query-proxy deactivate should not wait for unrelated in-flight sink control handoff"
    );
    deactivate_result
        .expect("join stale sink-query-proxy deactivate via timeout completion")
        .expect("join stale sink-query-proxy deactivate task")
        .expect("stale deactivate query-peer sink-query-proxy route");

    app.close().await.expect("close app");
}
