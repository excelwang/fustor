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
    assert_eq!(
        claim.resource_ids,
        vec![listener_resource.resource_id.clone()]
    );

    app_2.close().await.expect("close second app");
    app_1.close().await.expect("close first app");
    clear_process_facade_claim_for_tests();
}
