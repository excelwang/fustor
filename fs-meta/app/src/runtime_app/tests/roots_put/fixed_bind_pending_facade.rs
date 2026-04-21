#[tokio::test]
async fn close_clears_process_wide_fixed_bind_owner_and_pending_handoff_state() {
    struct ProcessFacadeClaimReset;

    impl Drop for ProcessFacadeClaimReset {
        fn drop(&mut self) {
            clear_process_facade_claim_for_tests();
        }
    }

    clear_process_facade_claim_for_tests();
    let _claim_reset = ProcessFacadeClaimReset;

    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "listener-a".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let app = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", tmp.path())],
                host_object_grants: vec![granted_mount_root("node-a::root-a", tmp.path())],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: listener_resource.resource_id.clone(),
                local_listener_resources: vec![listener_resource.clone()],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("node-a-fixed-bind-cleanup".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");

    *app.pending_facade.lock().await = Some(PendingFacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 1,
        resource_ids: vec![listener_resource.resource_id.clone()],
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: listener_resource.resource_id.clone(),
            resource_ids: vec![listener_resource.resource_id.clone()],
        }],
        group_ids: Vec::new(),
        runtime_managed: true,
        runtime_exposure_confirmed: true,
        resolved: app
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve facade config"),
    });

    match app.try_spawn_pending_facade().await {
        Ok(true) => {}
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            return;
        }
        Ok(false) => panic!("fixed-bind facade should claim the listener"),
        Err(err) => panic!("spawn fixed-bind facade: {err}"),
    }

    mark_active_fixed_bind_facade_owner(
        &bind_addr,
        ActiveFixedBindFacadeRegistrant {
            instance_id: app.instance_id,
            api_task: app.api_task.clone(),
            api_request_tracker: app.api_request_tracker.clone(),
            api_control_gate: app.api_control_gate.clone(),
        },
    );
    let active_owner_present = {
        let guard = match active_fixed_bind_facade_owner_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains_key(&bind_addr)
    };
    assert!(
        active_owner_present,
        "active fixed-bind facade owner must be registered before close"
    );

    mark_pending_fixed_bind_handoff_ready(
        &bind_addr,
        PendingFixedBindHandoffRegistrant {
            instance_id: app.instance_id,
            api_task: app.api_task.clone(),
            pending_facade: app.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: app
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: app
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            facade_spawn_in_progress: app.facade_spawn_in_progress.clone(),
            facade_pending_status: app.facade_pending_status.clone(),
            facade_service_state: app.facade_service_state.clone(),
            rollout_status: app.rollout_status.clone(),
            api_request_tracker: app.api_request_tracker.clone(),
            api_control_gate: app.api_control_gate.clone(),
            runtime_gate_state: app.runtime_gate_state.clone(),
            runtime_state_changed: app.runtime_state_changed.clone(),
            node_id: app.node_id.clone(),
            runtime_boundary: app.runtime_boundary.clone(),
            source: app.source.clone(),
            sink: app.sink.clone(),
            query_sink: app.query_sink.clone(),
            query_runtime_boundary: app.runtime_boundary.clone(),
        },
    );
    let pending_handoff_present = {
        let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains_key(&bind_addr)
    };
    assert!(
        pending_handoff_present,
        "pending fixed-bind handoff state must be present before close"
    );

    app.close().await.expect("close app");

    let active_owner_present_after_close = {
        let guard = match active_fixed_bind_facade_owner_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains_key(&bind_addr)
    };
    assert!(
        !active_owner_present_after_close,
        "close must clear the process-wide active fixed-bind owner record"
    );
    let pending_handoff_present_after_close = {
        let guard = match pending_fixed_bind_handoff_ready_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains_key(&bind_addr)
    };
    assert!(
        !pending_handoff_present_after_close,
        "close must clear the process-wide pending fixed-bind handoff record"
    );
    let guard = match process_facade_claim_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert!(
        !guard.contains_key(&bind_addr),
        "close must clear the owned process facade claim"
    );
}

#[tokio::test]
async fn pending_facade_exposure_confirmed_waits_for_inflight_roots_put_after_sink_update_begins()
 {
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
            Some(boundary),
            state_boundary,
        )
        .expect("init external-worker app"),
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
    let client = Client::new();
    let login = client
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "login failed: {}",
        login.status()
    );
    let login_body: serde_json::Value = login.json().await.expect("decode login");
    let token = login_body["token"].as_str().expect("token").to_string();

    let update_entered = Arc::new(Notify::new());
    let update_release = Arc::new(Notify::new());
    let _update_reset = SinkWorkerUpdateRootsHookReset;
    crate::workers::sink::install_sink_worker_update_roots_hook(
        crate::workers::sink::SinkWorkerUpdateRootsHook {
            entered: update_entered.clone(),
            release: update_release.clone(),
        },
    );

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });

    let roots_body = json!({
        "roots": [{
            "id": "test-root",
            "selector": { "fs_source": fs_source },
            "subpath_scope": "/",
            "watch": false,
            "scan": true,
        }]
    });
    let request = tokio::spawn({
        let client = client.clone();
        let bind_addr = bind_addr.clone();
        let token = token.clone();
        async move {
            client
                .put(format!(
                    "http://{bind_addr}/api/fs-meta/v1/monitoring/roots"
                ))
                .bearer_auth(token)
                .json(&roots_body)
                .send()
                .await
        }
    });

    update_entered.notified().await;
    let control_task = tokio::spawn({
        let app = app.clone();
        async move {
            app.on_control_frame(&[trusted_exposure_confirmed_envelope(
                execution_units::FACADE_RUNTIME_UNIT_ID,
                2,
            )])
            .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(600), shutdown_started.notified())
            .await
            .is_err(),
        "pending facade exposure confirmation must not start shutdown while roots_put is still dispatching update_logical_roots to the sink worker"
    );

    update_release.notify_waiters();

    let response = request
        .await
        .expect("join roots_put request")
        .expect("roots_put request should complete");
    let status = response.status();
    let body = response.text().await.expect("decode roots_put body");
    assert!(
        status.is_success(),
        "in-flight roots_put should complete before pending facade replacement shutdown starts: status={status} body={body}"
    );
    control_task
        .await
        .expect("join control-frame task")
        .expect("handle exposure confirmation after roots_put");
    app.close().await.expect("close app");
}
