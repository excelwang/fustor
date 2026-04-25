#[tokio::test]
async fn facade_spawn_dedupes_across_distinct_app_instances_with_same_fixed_bind() {
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "shared-app-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path_1, shadow_path_1) = write_auth_files(&tmp);
    let cfg_1 = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("test-root-1", tmp.path())],
            host_object_grants: vec![granted_mount_root("node-a::root-1", tmp.path())],
            ..local_source_config()
        },
        api: api::ApiConfig {
            enabled: true,
            facade_resource_id: listener_resource.resource_id.clone(),
            local_listener_resources: vec![listener_resource.clone()],
            auth: api::ApiAuthConfig {
                passwd_path: passwd_path_1,
                shadow_path: shadow_path_1,
                ..api::ApiAuthConfig::default()
            },
        },
    };
    let app_1 = FSMetaApp::with_boundaries(
        cfg_1,
        NodeId("fixed-bind-node-a".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init first app");
    *app_1.pending_facade.lock().await = Some(PendingFacadeActivation {
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
        resolved: app_1
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve first facade config"),
    });
    assert!(
        app_1
            .try_spawn_pending_facade()
            .await
            .expect("spawn first facade"),
        "first app instance should claim and activate the fixed facade"
    );

    let (passwd_path_2, shadow_path_2) = write_auth_files(&tmp);
    let cfg_2 = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("test-root-2", tmp.path())],
            host_object_grants: vec![granted_mount_root("node-b::root-1", tmp.path())],
            ..local_source_config()
        },
        api: api::ApiConfig {
            enabled: true,
            facade_resource_id: listener_resource.resource_id.clone(),
            local_listener_resources: vec![listener_resource.clone()],
            auth: api::ApiAuthConfig {
                passwd_path: passwd_path_2,
                shadow_path: shadow_path_2,
                ..api::ApiAuthConfig::default()
            },
        },
    };
    let app_2 = FSMetaApp::with_boundaries(
        cfg_2,
        NodeId("fixed-bind-node-b".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init second app");
    *app_2.pending_facade.lock().await = Some(PendingFacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 2,
        resource_ids: vec![listener_resource.resource_id.clone()],
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: listener_resource.resource_id.clone(),
            resource_ids: vec![listener_resource.resource_id.clone()],
        }],
        group_ids: Vec::new(),
        runtime_managed: true,
        runtime_exposure_confirmed: true,
        resolved: app_2
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve second facade config"),
    });
    let second_spawn = app_2
        .try_spawn_pending_facade()
        .await
        .expect("second app facade spawn should not error");
    assert!(
        !second_spawn,
        "distinct app instance must not start a duplicate fixed-bind facade"
    );
    assert!(
        app_2.api_task.lock().await.is_none(),
        "second app instance must not activate a duplicate facade handle"
    );

    let claim = {
        let guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .get(&bind_addr)
            .cloned()
            .expect("fixed-bind facade claim should remain owned")
    };
    assert_eq!(claim.owner_instance_id, app_1.instance_id);
    assert_eq!(claim.bind_addr, bind_addr);

    app_2.close().await.expect("close second app");
    app_1.close().await.expect("close first app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn retry_pending_facade_releases_stale_process_claim_after_exposure_confirmed() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "stale-claim-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
            host_object_grants: vec![granted_mount_root("node-a::root-1", tmp.path())],
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
    };
    let app = FSMetaApp::with_boundaries(
        cfg,
        NodeId("fixed-bind-node-stale-claim".into()),
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
    {
        let mut guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(
            bind_addr.clone(),
            ProcessFacadeClaim {
                owner_instance_id: 2,
                bind_addr: bind_addr.clone(),
            },
        );
    }

    app.retry_pending_facade(&facade_control_stream_route(), 1, false)
        .await
        .expect("retry pending facade with stale fixed-bind claim");

    let stale_claim_still_present = {
        let guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.get(&bind_addr).is_some_and(|claim| claim.owner_instance_id == 2)
    };
    assert!(
        !stale_claim_still_present,
        "retry_pending_facade must clear a stale fixed-bind process claim once runtime exposure is confirmed, otherwise the successor facade stays permanently suppressed by an ownerless in-process claim",
    );

    app.close().await.expect("close app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn retry_pending_facade_does_not_wait_for_runtime_exposure_behind_dead_active_handle() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "dead-active-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let app = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("node-a::root-1", tmp.path())],
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
        NodeId("fixed-bind-node-dead-active".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");

    if !app
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 1)
        .await
    {
        return;
    }

    {
        let guard = app.api_task.lock().await;
        let active = guard.as_ref().expect("active facade installed");
        active.handle.cancel_for_tests();
    }
    let stale_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let dead = {
            let guard = app.api_task.lock().await;
            let active = guard.as_ref().expect("active facade installed");
            !active.handle.is_running()
        };
        if dead {
            break;
        }
        assert!(
            tokio::time::Instant::now() < stale_deadline,
            "test precondition: active facade handle should stop after cancellation but remain retained in api_task",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    {
        let guard = app.api_task.lock().await;
        let active = guard.as_ref().expect("active facade installed");
        assert!(
            !active.handle.is_running(),
            "test precondition: active facade handle should be dead but still retained in api_task",
        );
    }

    *app.pending_facade.lock().await = Some(PendingFacadeActivation {
        route_key: facade_control_stream_route(),
        generation: 2,
        resource_ids: vec![listener_resource.resource_id.clone()],
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: listener_resource.resource_id.clone(),
            resource_ids: vec![listener_resource.resource_id.clone()],
        }],
        group_ids: Vec::new(),
        runtime_managed: true,
        runtime_exposure_confirmed: false,
        resolved: app
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve pending facade config"),
    });

    app.retry_pending_facade(&facade_control_stream_route(), 2, false)
        .await
        .expect("retry pending facade behind dead active handle");

    let active_guard = app.api_task.lock().await;
    let active = active_guard
        .as_ref()
        .expect("retry should reinstall active facade");
    assert_eq!(
        active.generation, 2,
        "retry_pending_facade must not keep waiting for runtime exposure behind a dead retained facade handle; it should promote the replacement generation immediately",
    );
    assert!(
        active.handle.is_running(),
        "replacement facade handle should be running after retry promotes the new generation",
    );
    assert!(
        app.pending_facade.lock().await.is_none(),
        "retry should clear the pending facade once the replacement is promoted",
    );

    {
        let guard = app.api_task.lock().await;
        let active = guard
            .as_ref()
            .expect("replacement facade should remain installed for teardown");
        active.handle.cancel_for_tests();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let running = app
                .api_task
                .lock()
                .await
                .as_ref()
                .is_some_and(|active| active.handle.is_running());
            if !running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement facade thread should stop after teardown cancel");
    tokio::time::timeout(Duration::from_secs(2), app.close())
        .await
        .expect("close app should not hang after explicit facade teardown")
        .expect("close app");
    clear_process_facade_claim_for_tests();
}
