#[tokio::test]
async fn facade_spawn_dedupes_across_distinct_app_instances_with_same_fixed_bind() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
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
        runtime_exposure_confirmed: false,
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
        "distinct app instance must not start a duplicate fixed-bind facade before runtime exposure is confirmed"
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
async fn fixed_bind_cold_start_publishes_same_frame_facade_dependent_routes() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root-a");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "cold-start-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let app = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root)],
                host_object_grants: vec![granted_mount_root("node-d::root-a", &root)],
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
        NodeId("fixed-bind-node-cold-start".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");

    let query_route = format!("{}.req", ROUTE_KEY_QUERY);
    let force_find_route = format!("{}.req", ROUTE_KEY_FORCE_FIND);
    app.on_control_frame(&[
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::FACADE_RUNTIME_UNIT_ID,
            facade_control_stream_route(),
            &[("cold-start-listener", &["cold-start-listener"])],
            1,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            &[("root-a", &["node-d::root-a"])],
            1,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            query_route.clone(),
            &[("cold-start-listener", &["cold-start-listener"])],
            1,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            force_find_route.clone(),
            &[("root-a", &["node-d::root-a"])],
            1,
        ),
    ])
    .await
    .expect("cold-start facade/query/sink wave should settle");

    assert!(
        app.pending_facade.lock().await.is_none(),
        "cold-start fixed-bind facade publication should complete in the same control frame"
    );
    assert!(
        app.api_task.lock().await.is_some(),
        "cold-start fixed-bind facade should install an active API task"
    );
    assert!(
        app.api_control_gate.is_ready(),
        "cold-start fixed-bind facade should leave the API gate ready after publication"
    );
    assert!(
        app.facade_gate
            .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &query_route)
            .expect("query route state"),
        "same-frame query route must not be suppressed by a stale pending fixed-bind snapshot after cold-start publication"
    );
    let claims = app.shared_sink_route_claims.lock().await;
    assert!(
        claims.contains_key(&(
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            force_find_route.clone()
        )),
        "same-frame sink-owned query route must not be suppressed by a stale pending fixed-bind snapshot after cold-start publication"
    );
    drop(claims);
    assert!(
        !app.pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire),
        "cold-start fixed-bind publication should not leave suppressed dependent routes pending"
    );

    app.close().await.expect("close app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn fixed_bind_publication_replay_uses_fresh_session_for_sink_owned_routes() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let root = tmp.path().join("root-a");
    fs::create_dir_all(&root).expect("create root-a");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "publication-tail-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let app = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root)],
                host_object_grants: vec![granted_mount_root("node-d::root-a", &root)],
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
        NodeId("fixed-bind-publication-tail-node".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");

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
            .expect("resolve facade config"),
    });
    let stale_pre_publication_session = app.begin_fixed_bind_lifecycle_session().await;

    let materialized_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL);
    let retained_sink_signals = crate::runtime::orchestration::sink_control_signals_from_envelopes(
        &[activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            materialized_route.clone(),
            &[("root-a", &["node-d::root-a"])],
            2,
        )],
    )
    .expect("retained sink activate");
    app.record_retained_sink_control_state(&retained_sink_signals)
        .await;
    let facade_scopes = vec![RuntimeBoundScope {
        scope_id: listener_resource.resource_id.clone(),
        resource_ids: vec![listener_resource.resource_id.clone()],
    }];
    let materialized_proxy_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY);
    let sink_status_route = format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL);
    app.record_suppressed_public_query_activate(
        FacadeRuntimeUnit::QueryPeer,
        &materialized_proxy_route,
        2,
        &facade_scopes,
    )
    .await;
    app.record_suppressed_public_query_activate(
        FacadeRuntimeUnit::QueryPeer,
        &sink_status_route,
        2,
        &facade_scopes,
    )
    .await;
    app.pending_fixed_bind_has_suppressed_dependent_routes
        .store(true, Ordering::Release);

    assert!(
        app.try_spawn_pending_facade()
            .await
            .expect("spawn fixed-bind facade"),
        "fixed-bind facade publication should complete"
    );
    assert!(
        app.pending_facade.lock().await.is_none(),
        "publication must clear pending facade before replaying suppressed routes"
    );

    app.replay_suppressed_public_query_activates_after_publication_with_session(
        stale_pre_publication_session,
    )
    .await
    .expect("replay suppressed routes after publication");

    let claims = app.shared_sink_route_claims.lock().await;
    assert!(
        claims.contains_key(&(
            execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            materialized_route.clone()
        )),
        "suppressed sink-owned materialized route must replay from a fresh post-publication fixed-bind session"
    );
    drop(claims);
    assert!(
        app.facade_gate
            .route_active(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                &materialized_proxy_route
            )
            .expect("materialized query proxy route state after publication replay"),
        "fixed-bind publication replay must restore retained materialized query proxy"
    );
    assert!(
        app.facade_gate
            .route_active(execution_units::QUERY_PEER_RUNTIME_UNIT_ID, &sink_status_route)
            .expect("sink-status route state after publication replay"),
        "fixed-bind publication replay must restore retained sink-status"
    );
    assert!(
        !app.pending_fixed_bind_has_suppressed_dependent_routes
            .load(Ordering::Acquire),
        "publication replay tail should clear the suppressed-route marker only after replay"
    );

    app.close().await.expect("close app");
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

#[tokio::test]
async fn pending_successor_fail_closed_releases_continuity_retained_fixed_bind_predecessor() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "retained-predecessor-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let predecessor_root = tmp.path().join("predecessor-root");
    let successor_root = tmp.path().join("successor-root");
    fs::create_dir_all(&predecessor_root).expect("create predecessor root");
    fs::create_dir_all(&successor_root).expect("create successor root");

    let (predecessor_passwd, predecessor_shadow) = write_auth_files(&tmp);
    let predecessor = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new(
                    "predecessor-root",
                    &predecessor_root,
                )],
                host_object_grants: vec![granted_mount_root(
                    "node-a::predecessor-root",
                    &predecessor_root,
                )],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: listener_resource.resource_id.clone(),
                local_listener_resources: vec![listener_resource.clone()],
                auth: api::ApiAuthConfig {
                    passwd_path: predecessor_passwd,
                    shadow_path: predecessor_shadow,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-predecessor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init predecessor app");
    *predecessor.pending_facade.lock().await = Some(PendingFacadeActivation {
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
        resolved: predecessor
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve predecessor facade config"),
    });
    match predecessor.try_spawn_pending_facade().await {
        Ok(true) => {}
        Err(CnxError::InvalidInput(msg))
            if msg.contains("fs-meta api bind failed: Operation not permitted") =>
        {
            clear_process_facade_claim_for_tests();
            return;
        }
        Ok(false) => panic!("predecessor should claim the fixed-bind facade"),
        Err(err) => panic!("spawn predecessor facade: {err}"),
    }

    let (successor_passwd, successor_shadow) = write_auth_files(&tmp);
    let successor = FSMetaApp::with_boundaries(
        FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("successor-root", &successor_root)],
                host_object_grants: vec![granted_mount_root(
                    "node-b::successor-root",
                    &successor_root,
                )],
                ..local_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: listener_resource.resource_id.clone(),
                local_listener_resources: vec![listener_resource.clone()],
                auth: api::ApiAuthConfig {
                    passwd_path: successor_passwd,
                    shadow_path: successor_shadow,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-successor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init successor app");

    successor
        .apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            &[RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
        )
        .await
        .expect("successor should record pending fixed-bind facade");
    successor
        .apply_facade_activate(
            FacadeRuntimeUnit::Query,
            &format!("{}.req", ROUTE_KEY_QUERY),
            2,
            &[RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
        )
        .await
        .expect("successor should retain suppressed public query route");
    let stale_claim_session = successor.begin_fixed_bind_lifecycle_session().await;

    predecessor
        .apply_facade_deactivate(
            FacadeRuntimeUnit::Facade,
            &facade_control_stream_route(),
            2,
            false,
        )
        .await
        .expect("predecessor should retain active facade for continuity");
    assert!(
        predecessor
            .retained_active_facade_continuity
            .load(AtomicOrdering::Acquire),
        "test precondition: predecessor must be retained only for fixed-bind continuity"
    );

    successor.mark_control_uninitialized_after_failure().await;

    assert!(
        predecessor.api_task.lock().await.is_none(),
        "pending successor fail-closed after accepting desired state must release a continuity-retained fixed-bind predecessor instead of leaving old runtime PID serving the listener"
    );
    let active = successor.api_task.lock().await;
    assert!(
        active.as_ref().is_some_and(|active| active.generation == 2),
        "successor should bind its fail-closed app boundary after releasing the retained predecessor"
    );

    drop(active);
    let query_route = format!("{}.req", ROUTE_KEY_QUERY);
    successor
        .apply_facade_activate_with_session(
            stale_claim_session,
            FacadeRuntimeUnit::Query,
            &query_route,
            2,
            &[RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
        )
        .await
        .expect("stale fixed-bind session should refresh after predecessor release");
    assert!(
        successor
            .facade_gate
            .route_active(execution_units::QUERY_RUNTIME_UNIT_ID, &query_route)
            .expect("query route state after fixed-bind predecessor release"),
        "released fixed-bind predecessor claim must not keep suppressing facade-dependent query routes through a stale lifecycle session"
    );

    successor.close().await.expect("close successor app");
    predecessor.close().await.expect("close predecessor app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn after_release_handoff_promotes_unconfirmed_successor_facade_boundary() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "after-release-successor-listener".to_string(),
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
        NodeId("fixed-bind-after-release-successor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");

    if !app
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 1)
        .await
    {
        clear_process_facade_claim_for_tests();
        return;
    }
    {
        let mut guard = match process_facade_claim_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(
            bind_addr.clone(),
            ProcessFacadeClaim {
                owner_instance_id: app.instance_id,
                bind_addr: bind_addr.clone(),
            },
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

    let completed = tokio::time::timeout(
        Duration::from_secs(2),
        execute_fixed_bind_after_release_handoff(PendingFixedBindHandoffContinuation {
            bind_addr: bind_addr.clone(),
            registrant: PendingFixedBindHandoffRegistrant {
                instance_id: app.instance_id,
                api_task: app.api_task.clone(),
                pending_facade: app.pending_facade.clone(),
                pending_fixed_bind_claim_release_followup: app
                    .pending_fixed_bind_claim_release_followup
                    .clone(),
                pending_fixed_bind_has_suppressed_dependent_routes: app
                    .pending_fixed_bind_has_suppressed_dependent_routes
                    .clone(),
                retained_active_facade_continuity: app.retained_active_facade_continuity.clone(),
                facade_spawn_in_progress: app.facade_spawn_in_progress.clone(),
                facade_pending_status: app.facade_pending_status.clone(),
                facade_service_state: app.facade_service_state.clone(),
                rollout_status: app.rollout_status.clone(),
                api_request_tracker: app.api_request_tracker.clone(),
                api_control_gate: app.api_control_gate.clone(),
                control_failure_uninitialized: app.control_failure_uninitialized.clone(),
                runtime_gate_state: app.runtime_gate_state.clone(),
                runtime_state_changed: app.runtime_state_changed.clone(),
                node_id: app.node_id.clone(),
                runtime_boundary: app.runtime_boundary.clone(),
                source: app.source.clone(),
                sink: app.sink.clone(),
                query_sink: app.query_sink.clone(),
                query_runtime_boundary: app.runtime_boundary.clone(),
            },
        }),
    )
    .await
    .expect("after-release handoff must not wait for normal runtime exposure")
    .expect("after-release handoff should complete");

    assert!(
        completed,
        "after fixed-bind owner release, a successor that already has the listener boundary must promote the pending facade even when runtime exposure is still unconfirmed"
    );
    let active = app.api_task.lock().await;
    assert_eq!(
        active.as_ref().map(|active| active.generation),
        Some(2),
        "after-release handoff should promote the serving successor boundary to the pending generation"
    );
    drop(active);
    assert!(
        app.pending_facade.lock().await.is_none(),
        "after-release handoff should clear the pending facade after promoting the successor boundary"
    );

    app.close().await.expect("close app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn failed_owner_shutdown_drives_successor_after_release_handoff() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "failed-owner-after-release-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (predecessor_passwd_path, predecessor_shadow_path) = write_auth_files(&tmp);
    let predecessor = FSMetaApp::with_boundaries(
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
                    passwd_path: predecessor_passwd_path,
                    shadow_path: predecessor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-failed-owner-predecessor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init predecessor app");
    if !predecessor
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 1)
        .await
    {
        clear_process_facade_claim_for_tests();
        return;
    }

    let (successor_passwd_path, successor_shadow_path) = write_auth_files(&tmp);
    let successor = FSMetaApp::with_boundaries(
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
                    passwd_path: successor_passwd_path,
                    shadow_path: successor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-failed-owner-successor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init successor app");
    *successor.pending_facade.lock().await = Some(PendingFacadeActivation {
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
        resolved: successor
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve pending facade config"),
    });

    let session = predecessor.begin_fixed_bind_lifecycle_session().await;
    let registrant = PendingFixedBindHandoffRegistrant {
        instance_id: successor.instance_id,
        api_task: successor.api_task.clone(),
        pending_facade: successor.pending_facade.clone(),
        pending_fixed_bind_claim_release_followup: successor
            .pending_fixed_bind_claim_release_followup
            .clone(),
        pending_fixed_bind_has_suppressed_dependent_routes: successor
            .pending_fixed_bind_has_suppressed_dependent_routes
            .clone(),
        retained_active_facade_continuity: successor.retained_active_facade_continuity.clone(),
        facade_spawn_in_progress: successor.facade_spawn_in_progress.clone(),
        facade_pending_status: successor.facade_pending_status.clone(),
        facade_service_state: successor.facade_service_state.clone(),
        rollout_status: successor.rollout_status.clone(),
        api_request_tracker: successor.api_request_tracker.clone(),
        api_control_gate: successor.api_control_gate.clone(),
        control_failure_uninitialized: successor.control_failure_uninitialized.clone(),
        runtime_gate_state: successor.runtime_gate_state.clone(),
        runtime_state_changed: successor.runtime_state_changed.clone(),
        node_id: successor.node_id.clone(),
        runtime_boundary: successor.runtime_boundary.clone(),
        source: successor.source.clone(),
        sink: successor.sink.clone(),
        query_sink: successor.query_sink.clone(),
        query_runtime_boundary: successor.runtime_boundary.clone(),
    };
    let stale_facade_request = predecessor.api_control_gate.begin_facade_request();
    tokio::time::timeout(
        Duration::from_secs(5),
        predecessor.drive_fixed_bind_shutdown_handoff(
            session,
            Some(ActiveFixedBindShutdownContinuation {
                bind_addr: bind_addr.clone(),
                pending_handoff: Some(PendingFixedBindHandoffContinuation {
                    bind_addr: bind_addr.clone(),
                    registrant,
                }),
                release_mode: FixedBindOwnerReleaseMode::FailedOwnerCutover,
            }),
        ),
    )
    .await
    .expect("failed owner fixed-bind handoff must not wait for stale predecessor facade reads")
    .expect("failed owner shutdown handoff should complete");
    drop(stale_facade_request);

    let active = successor.api_task.lock().await;
    assert_eq!(
        active.as_ref().map(|active| active.generation),
        Some(2),
        "failed owner release must spawn the pending successor facade"
    );
    drop(active);
    assert!(
        successor.pending_facade.lock().await.is_none(),
        "successor pending facade should be cleared after after-release spawn"
    );

    successor.close().await.expect("close successor app");
    predecessor.close().await.expect("close predecessor app");
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn failed_owner_release_precedes_uninitialized_query_cleanup() {
    struct UninitializedQueryWithdrawPauseHookReset;

    impl Drop for UninitializedQueryWithdrawPauseHookReset {
        fn drop(&mut self) {
            clear_uninitialized_query_withdraw_pause_hook();
        }
    }

    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "failed-owner-before-cleanup-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (predecessor_passwd_path, predecessor_shadow_path) = write_auth_files(&tmp);
    let predecessor = FSMetaApp::with_boundaries(
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
                    passwd_path: predecessor_passwd_path,
                    shadow_path: predecessor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-failed-owner-before-cleanup-predecessor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init predecessor app");
    if !predecessor
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 1)
        .await
    {
        clear_process_facade_claim_for_tests();
        return;
    }

    let (successor_passwd_path, successor_shadow_path) = write_auth_files(&tmp);
    let successor = FSMetaApp::with_boundaries(
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
                    passwd_path: successor_passwd_path,
                    shadow_path: successor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-failed-owner-before-cleanup-successor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init successor app");
    *successor.pending_facade.lock().await = Some(PendingFacadeActivation {
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
        resolved: successor
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve pending facade config"),
    });

    mark_pending_fixed_bind_handoff_ready_with_registrant(
        &bind_addr,
        PendingFixedBindHandoffRegistrant {
            instance_id: successor.instance_id,
            api_task: successor.api_task.clone(),
            pending_facade: successor.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: successor
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: successor
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            retained_active_facade_continuity: successor.retained_active_facade_continuity.clone(),
            facade_spawn_in_progress: successor.facade_spawn_in_progress.clone(),
            facade_pending_status: successor.facade_pending_status.clone(),
            facade_service_state: successor.facade_service_state.clone(),
            rollout_status: successor.rollout_status.clone(),
            api_request_tracker: successor.api_request_tracker.clone(),
            api_control_gate: successor.api_control_gate.clone(),
            control_failure_uninitialized: successor.control_failure_uninitialized.clone(),
            runtime_gate_state: successor.runtime_gate_state.clone(),
            runtime_state_changed: successor.runtime_state_changed.clone(),
            node_id: successor.node_id.clone(),
            runtime_boundary: successor.runtime_boundary.clone(),
            source: successor.source.clone(),
            sink: successor.sink.clone(),
            query_sink: successor.query_sink.clone(),
            query_runtime_boundary: successor.runtime_boundary.clone(),
        },
    );

    let _hook_reset = UninitializedQueryWithdrawPauseHookReset;
    let cleanup_entered = Arc::new(tokio::sync::Notify::new());
    let cleanup_release = Arc::new(tokio::sync::Notify::new());
    install_uninitialized_query_withdraw_pause_hook(UninitializedQueryWithdrawPauseHook {
        entered: cleanup_entered.clone(),
        release: cleanup_release.clone(),
    });

    let fixed_bind_session = predecessor.begin_fixed_bind_lifecycle_session().await;
    let cleanup_wait = cleanup_entered.notified();
    tokio::pin!(cleanup_wait);
    let fail_closed = predecessor.mark_control_uninitialized_after_failure_with_session_and_policy(
        &fixed_bind_session,
        ControlFailureRecoveryLanePolicy::WithdrawInternalStatus,
    );
    tokio::pin!(fail_closed);
    tokio::select! {
        _ = &mut cleanup_wait => {}
        _ = &mut fail_closed => panic!("control-failure cleanup reached completion before the test pause"),
    }

    let successor_active_before_cleanup = successor.api_task.lock().await.is_some();
    cleanup_release.notify_waiters();
    fail_closed.await;
    assert!(
        successor_active_before_cleanup,
        "failed fixed-bind owner release must publish the successor facade before uninitialized query cleanup can consume the recovery window"
    );

    successor.close().await.expect("close successor app");
    predecessor.close().await.expect("close predecessor app");
    clear_pending_fixed_bind_handoff_ready(&bind_addr);
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn cleanup_only_facade_followup_releases_failed_fixed_bind_owner() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "cleanup-only-failed-owner-listener".to_string(),
        bind_addr: bind_addr.clone(),
    };
    let (predecessor_passwd_path, predecessor_shadow_path) = write_auth_files(&tmp);
    let predecessor = FSMetaApp::with_boundaries(
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
                    passwd_path: predecessor_passwd_path,
                    shadow_path: predecessor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-cleanup-only-failed-owner-predecessor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init predecessor app");
    if !predecessor
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 1)
        .await
    {
        clear_process_facade_claim_for_tests();
        return;
    }
    let source_status_route = format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL);
    predecessor
        .apply_facade_activate(
            FacadeRuntimeUnit::QueryPeer,
            &source_status_route,
            1,
            &[RuntimeBoundScope {
                scope_id: listener_resource.resource_id.clone(),
                resource_ids: vec![listener_resource.resource_id.clone()],
            }],
        )
        .await
        .expect("activate cleanup-only query-peer route");
    set_control_initialized_for_tests(&predecessor, false);
    predecessor.api_control_gate.set_ready(false);

    let (successor_passwd_path, successor_shadow_path) = write_auth_files(&tmp);
    let successor = FSMetaApp::with_boundaries(
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
                    passwd_path: successor_passwd_path,
                    shadow_path: successor_shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        },
        NodeId("fixed-bind-cleanup-only-failed-owner-successor".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init successor app");
    *successor.pending_facade.lock().await = Some(PendingFacadeActivation {
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
        resolved: successor
            .config
            .api
            .resolve_for_candidate_ids(std::slice::from_ref(&listener_resource.resource_id))
            .expect("resolve pending facade config"),
    });
    mark_pending_fixed_bind_handoff_ready_with_registrant(
        &bind_addr,
        PendingFixedBindHandoffRegistrant {
            instance_id: successor.instance_id,
            api_task: successor.api_task.clone(),
            pending_facade: successor.pending_facade.clone(),
            pending_fixed_bind_claim_release_followup: successor
                .pending_fixed_bind_claim_release_followup
                .clone(),
            pending_fixed_bind_has_suppressed_dependent_routes: successor
                .pending_fixed_bind_has_suppressed_dependent_routes
                .clone(),
            retained_active_facade_continuity: successor.retained_active_facade_continuity.clone(),
            facade_spawn_in_progress: successor.facade_spawn_in_progress.clone(),
            facade_pending_status: successor.facade_pending_status.clone(),
            facade_service_state: successor.facade_service_state.clone(),
            rollout_status: successor.rollout_status.clone(),
            api_request_tracker: successor.api_request_tracker.clone(),
            api_control_gate: successor.api_control_gate.clone(),
            control_failure_uninitialized: successor.control_failure_uninitialized.clone(),
            runtime_gate_state: successor.runtime_gate_state.clone(),
            runtime_state_changed: successor.runtime_state_changed.clone(),
            node_id: successor.node_id.clone(),
            runtime_boundary: successor.runtime_boundary.clone(),
            source: successor.source.clone(),
            sink: successor.sink.clone(),
            query_sink: successor.query_sink.clone(),
            query_runtime_boundary: successor.runtime_boundary.clone(),
        },
    );

    predecessor
        .on_control_frame(&[deactivate_envelope_with_route_key(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            source_status_route,
            2,
        )])
        .await
        .expect("cleanup-only facade followup should settle");

    assert!(
        predecessor.api_task.lock().await.is_none(),
        "cleanup-only facade followup on a failed fixed-bind owner must release the old listener instead of waiting for a later cleanup wave"
    );
    let active = successor.api_task.lock().await;
    assert_eq!(
        active.as_ref().map(|active| active.generation),
        Some(2),
        "cleanup-only facade followup should drive the pending successor facade immediately after releasing the failed owner"
    );
    drop(active);

    successor.close().await.expect("close successor app");
    predecessor.close().await.expect("close predecessor app");
    clear_pending_fixed_bind_handoff_ready(&bind_addr);
    clear_process_facade_claim_for_tests();
}

#[tokio::test]
async fn cleanup_only_facade_deactivate_preserves_fail_closed_facade_control_route() {
    let _serial = fixed_bind_handoff_test_serial().lock().await;
    clear_process_facade_claim_for_tests();
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let listener_resource = api::config::ApiListenerResource {
        resource_id: "cleanup-only-facade-control-retain-listener".to_string(),
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
        NodeId("fixed-bind-cleanup-only-retain-control-route".into()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init app");
    if !app
        .install_active_facade_for_tests(&[listener_resource.resource_id.as_str()], 2)
        .await
    {
        clear_process_facade_claim_for_tests();
        return;
    }
    app.apply_facade_activate(
        FacadeRuntimeUnit::Facade,
        &facade_control_stream_route(),
        2,
        &[RuntimeBoundScope {
            scope_id: listener_resource.resource_id.clone(),
            resource_ids: vec![listener_resource.resource_id.clone()],
        }],
    )
    .await
    .expect("activate serving facade-control route");
    set_control_initialized_for_tests(&app, false);
    app.control_failure_uninitialized
        .store(true, AtomicOrdering::Release);
    app.api_control_gate.set_ready(false);

    app.on_control_frame(&[deactivate_envelope_with_route_key(
        execution_units::FACADE_RUNTIME_UNIT_ID,
        facade_control_stream_route(),
        3,
    )])
    .await
    .expect("cleanup-only facade deactivate should settle");

    assert!(
        app.api_task.lock().await.is_some(),
        "cleanup-only facade deactivate during fail-closed recovery must not shut down the serving HTTP boundary"
    );
    assert!(
        app.facade_gate
            .route_active(
                execution_units::FACADE_RUNTIME_UNIT_ID,
                &facade_control_stream_route()
            )
            .expect("facade-control route state"),
        "cleanup-only facade deactivate during fail-closed recovery must not clear the facade-control route that proves the operator API boundary is serving"
    );

    app.close().await.expect("close app");
    clear_process_facade_claim_for_tests();
}
