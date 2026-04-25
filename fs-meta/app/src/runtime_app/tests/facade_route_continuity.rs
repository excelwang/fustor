#[tokio::test]
async fn query_peer_deactivate_does_not_shutdown_active_facade_listener() {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    let query_peer_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    app.apply_facade_activate(
        FacadeRuntimeUnit::QueryPeer,
        &query_peer_route,
        1,
        &[RuntimeBoundScope {
            scope_id: "root-a".to_string(),
            resource_ids: vec!["listener-a".to_string()],
        }],
    )
    .await
    .expect("activate query-peer route");

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });
    let shutdown_wait = shutdown_started.notified();

    app.apply_facade_deactivate(FacadeRuntimeUnit::QueryPeer, &query_peer_route, 1, false)
        .await
        .expect("deactivate query-peer route");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), shutdown_wait)
            .await
            .is_err(),
        "query-peer deactivation must not start active facade shutdown"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "query-peer deactivation must leave the active facade task installed"
    );

    let login = Client::new()
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "query-peer deactivation must leave the listener serving login: status={}",
        login.status()
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn mark_control_uninitialized_clears_retained_active_facade_continuity_before_later_query_cleanup()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });
    app.update_runtime_control_state_for_tests(|state| {
        state.set_control_initialized(true);
    });
    app.api_control_gate.set_ready(true);

    let route_scopes = [RuntimeBoundScope {
        scope_id: "root-a".to_string(),
        resource_ids: vec!["listener-a".to_string()],
    }];
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    app.apply_facade_activate(
        FacadeRuntimeUnit::QueryPeer,
        &source_status_route,
        1,
        &route_scopes,
    )
    .await
    .expect("activate query-peer source-status route");
    assert!(
        app.facade_gate
            .route_active(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                &source_status_route
            )
            .expect("query-peer source-status active before failure"),
        "query-peer source-status route should be active before retaining facade continuity"
    );

    app.apply_facade_deactivate(FacadeRuntimeUnit::Facade, &facade_control_stream_route(), 2, false)
        .await
        .expect("retain active facade continuity");
    assert!(
        app.retained_active_facade_continuity
            .load(Ordering::Acquire),
        "future-generation facade deactivate should arm retained active facade continuity before the failure seam"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "retained active facade continuity should keep the active facade handle installed"
    );

    app.mark_control_uninitialized_after_failure().await;
    assert!(
        !app.control_initialized(),
        "forced failure seam should leave runtime uninitialized before later cleanup-only query deactivates"
    );

    app.apply_facade_activate(
        FacadeRuntimeUnit::QueryPeer,
        &source_status_route,
        3,
        &route_scopes,
    )
    .await
    .expect("reactivate query-peer source-status route under later cleanup-only wave");
    assert!(
        app.facade_gate
            .route_active(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                &source_status_route
            )
            .expect("query-peer source-status active after stale reactivation"),
        "later query-peer activation should take effect before cleanup-only deactivate"
    );

    app.apply_facade_deactivate(FacadeRuntimeUnit::QueryPeer, &source_status_route, 4, false)
        .await
        .expect("cleanup-only query-peer source-status deactivate");

    assert!(
        !app.facade_gate
            .route_active(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                &source_status_route
            )
            .expect("query-peer source-status active after cleanup-only deactivate"),
        "cleanup-only query-peer deactivate after an uninitialized failure must not keep stale source-status route active under retained facade continuity"
    );
    assert!(
        !app.facade_gate
            .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &source_status_route)
            .expect("mirrored query source-status active after cleanup-only deactivate"),
        "cleanup-only query-peer deactivate after an uninitialized failure must also clear the mirrored local query lane"
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn future_generation_facade_deactivate_without_successor_activate_keeps_active_listener_available()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    app.apply_facade_activate(
        FacadeRuntimeUnit::Facade,
        &facade_control_stream_route(),
        2,
        &[RuntimeBoundScope {
            scope_id: "root-a".to_string(),
            resource_ids: vec!["listener-a".to_string()],
        }],
    )
    .await
    .expect("same-resource generation refresh should succeed");

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });
    let shutdown_wait = shutdown_started.notified();

    app.apply_facade_deactivate(FacadeRuntimeUnit::Facade, &facade_control_stream_route(), 3, false)
        .await
        .expect("future-generation facade deactivate should not error");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), shutdown_wait)
            .await
            .is_err(),
        "future-generation facade deactivate must not tear down the current listener before any successor facade activate arrives"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "future-generation facade deactivate without successor activate must retain the active facade handle"
    );

    let login = Client::new()
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "future-generation facade deactivate without successor activate must leave login available: status={}",
        login.status()
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn future_generation_facade_deactivate_without_successor_activate_keeps_query_api_key_provision_available()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    app.apply_facade_activate(
        FacadeRuntimeUnit::Facade,
        &facade_control_stream_route(),
        2,
        &[RuntimeBoundScope {
            scope_id: "root-a".to_string(),
            resource_ids: vec!["listener-a".to_string()],
        }],
    )
    .await
    .expect("same-resource generation refresh should succeed");

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });
    let shutdown_wait = shutdown_started.notified();

    app.apply_facade_deactivate(FacadeRuntimeUnit::Facade, &facade_control_stream_route(), 3, false)
        .await
        .expect("future-generation facade deactivate should not error");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), shutdown_wait)
            .await
            .is_err(),
        "future-generation facade deactivate must not tear down the current listener before any successor facade activate arrives"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "future-generation facade deactivate without successor activate must retain the active facade handle"
    );

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

    let query_key = client
        .post(format!("http://{bind_addr}/api/fs-meta/v1/query-api-keys"))
        .bearer_auth(&token)
        .json(&json!({"label":"continuity"}))
        .send()
        .await
        .expect("query api key request");
    assert!(
        query_key.status().is_success(),
        "future-generation facade deactivate without successor activate must keep query-api-key provisioning available: status={}",
        query_key.status()
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn uninitialized_same_generation_facade_deactivate_keeps_listener_available() {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });

    assert!(
        !app.control_initialized(),
        "test seam requires uninitialized runtime"
    );

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });
    let shutdown_wait = shutdown_started.notified();

    app.apply_facade_deactivate(FacadeRuntimeUnit::Facade, &facade_control_stream_route(), 1, false)
        .await
        .expect("same-generation facade deactivate should not error");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), shutdown_wait)
            .await
            .is_err(),
        "uninitialized same-generation facade deactivate must not tear down the active listener before replacement continuity is recovered"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "uninitialized same-generation facade deactivate must retain the active facade handle"
    );

    let login = Client::new()
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request");
    assert!(
        login.status().is_success(),
        "uninitialized same-generation facade deactivate must keep login available: status={}",
        login.status()
    );

    app.close().await.expect("close app");
}

#[tokio::test]
async fn initialized_same_generation_facade_deactivate_without_successor_keeps_login_transport_available()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root");
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("root-a", &root)],
            host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root)],
            ..local_source_config()
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("single-app-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
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
        generation: 1,
        resource_ids: vec!["listener-a".to_string()],
        handle: active_facade,
    });
    app.update_runtime_control_state_for_tests(|state| {
        state.set_control_initialized(true);
    });

    let shutdown_started = Arc::new(Notify::new());
    let _shutdown_reset = FacadeShutdownStartHookReset;
    install_facade_shutdown_start_hook(FacadeShutdownStartHook {
        entered: shutdown_started.clone(),
    });
    let shutdown_wait = shutdown_started.notified();

    app.apply_facade_deactivate(FacadeRuntimeUnit::Facade, &facade_control_stream_route(), 1, false)
        .await
        .expect("same-generation facade deactivate should not error");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), shutdown_wait)
            .await
            .is_err(),
        "initialized same-generation facade deactivate must not tear down listener before successor continuity is established"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "initialized same-generation facade deactivate must retain active facade handle"
    );

    let login = Client::new()
        .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
        .json(&json!({"username":"admin","password":"admin"}))
        .send()
        .await
        .expect("login request transport");
    assert!(
        login.status().is_success(),
        "initialized same-generation facade deactivate must keep login transport and session endpoint available: status={}",
        login.status()
    );

    app.close().await.expect("close app");
}
