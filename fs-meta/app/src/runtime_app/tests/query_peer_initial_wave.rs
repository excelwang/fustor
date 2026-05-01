#[test]
fn internal_status_availability_allows_peer_nodes_without_local_api_task_even_when_facade_is_unavailable()
{
    assert_eq!(
        FSMetaApp::internal_status_available_from_published_facade_state(
            FacadeServiceState::Unavailable,
            false,
        ),
        InternalStatusAvailability::Available,
        "peer-node internal status must stay available for cluster aggregation even when the local published facade state is unavailable"
    );
}

#[test]
fn internal_status_availability_fails_closed_for_local_api_task_when_facade_is_not_serving() {
    for state in [
        FacadeServiceState::Pending,
        FacadeServiceState::Unavailable,
    ] {
        assert_eq!(
            FSMetaApp::internal_status_available_from_published_facade_state(state, true),
            InternalStatusAvailability::UnavailableLocalFacadeNotServing,
            "local internal status routes must fail closed for a local api_task when the published facade state is not serving: state={state:?}"
        );
    }
    for state in [FacadeServiceState::Serving, FacadeServiceState::Degraded] {
        assert_eq!(
            FSMetaApp::internal_status_available_from_published_facade_state(state, true),
            InternalStatusAvailability::Available,
            "local internal status routes must stay available when the published facade state remains serving/degraded: state={state:?}"
        );
    }
}

#[test]
fn source_owned_status_availability_allows_local_api_task_when_facade_is_not_serving() {
    for state in [
        FacadeServiceState::Pending,
        FacadeServiceState::Unavailable,
    ] {
        assert_eq!(
            FSMetaApp::source_status_available_for_endpoint(state, true, true),
            InternalStatusAvailability::Available,
            "source-owned source-status must stay available for source runtime-scope aggregation even when a local api_task exists and the facade state is not serving: state={state:?}"
        );
    }
}

#[test]
fn query_owned_status_availability_still_fails_closed_for_local_api_task() {
    for state in [
        FacadeServiceState::Pending,
        FacadeServiceState::Unavailable,
    ] {
        assert_eq!(
            FSMetaApp::source_status_available_for_endpoint(state, true, false),
            InternalStatusAvailability::UnavailableLocalFacadeNotServing,
            "query-owned source-status remains tied to local facade serving state: state={state:?}"
        );
    }
}

#[derive(Default)]
struct DropUntilFirstEventsRecvBoundary {
    inner: LoopbackWorkerBoundary,
    stream_recv_armed: std::sync::atomic::AtomicBool,
    dropped_origin_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
    recv_counts: StdMutex<std::collections::BTreeMap<String, usize>>,
}

impl DropUntilFirstEventsRecvBoundary {
    fn events_route() -> String {
        format!("{}.stream", ROUTE_KEY_EVENTS)
    }

    fn dropped_origin_count(&self, origin: &str) -> usize {
        self.dropped_origin_counts
            .lock()
            .expect("dropped_origin_count lock")
            .get(origin)
            .copied()
            .unwrap_or(0)
    }

    fn recv_count(&self, route: &str) -> usize {
        self.recv_counts
            .lock()
            .expect("recv_count lock")
            .get(route)
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl ChannelIoSubset for DropUntilFirstEventsRecvBoundary {
    async fn channel_send(&self, ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        if request.channel_key.0 == Self::events_route()
            && !self.stream_recv_armed.load(AtomicOrdering::Acquire)
        {
            let mut dropped = self
                .dropped_origin_counts
                .lock()
                .expect("dropped_origin_counts lock");
            for event in request.events {
                *dropped
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
            }
            return Ok(());
        }
        self.inner.channel_send(ctx, request).await
    }

    async fn channel_recv(
        &self,
        ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        let route = request.channel_key.0.clone();
        if route == Self::events_route() {
            self.stream_recv_armed.store(true, AtomicOrdering::Release);
        }
        *self
            .recv_counts
            .lock()
            .expect("recv_counts lock")
            .entry(route)
            .or_default() += 1;
        self.inner.channel_recv(ctx, request).await
    }

    fn channel_close(&self, ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.inner.channel_close(ctx, channel)
    }
}

impl ChannelBoundary for DropUntilFirstEventsRecvBoundary {
    fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_one_initial_source_sink_and_query_peer_wave_settles_without_local_apply_timeout()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1_dir = tmp.path().join("nfs1");
    let nfs2_dir = tmp.path().join("nfs2");
    let nfs3_dir = tmp.path().join("nfs3");
    fs::create_dir_all(&nfs1_dir).expect("create nfs1 dir");
    fs::create_dir_all(&nfs2_dir).expect("create nfs2 dir");
    fs::create_dir_all(&nfs3_dir).expect("create nfs3 dir");
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
                    roots: vec![
                        worker_watch_scan_root("nfs1", &nfs1_dir),
                        worker_watch_scan_root("nfs2", &nfs2_dir),
                        worker_watch_scan_root("nfs3", &nfs3_dir),
                    ],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "127.0.0.1", nfs1_dir.clone()),
                        worker_export("node-a::nfs2", "node-a", "127.0.0.2", nfs2_dir.clone()),
                        worker_export("node-a::nfs3", "node-a", "127.0.0.3", nfs3_dir.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                &[
                    ("nfs1", &["node-a::nfs1"]),
                    ("nfs2", &["node-a::nfs2"]),
                    ("nfs3", &["node-a::nfs3"]),
                ],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                &[
                    ("nfs1", &["node-a::nfs1"]),
                    ("nfs2", &["node-a::nfs2"]),
                    ("nfs3", &["node-a::nfs3"]),
                ],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[
                    ("nfs1", &["node-a::nfs1"]),
                    ("nfs2", &["node-a::nfs2"]),
                    ("nfs3", &["node-a::nfs3"]),
                ],
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                &[
                    ("nfs1", &["node-a::nfs1"]),
                    ("nfs2", &["node-a::nfs2"]),
                    ("nfs3", &["node-a::nfs3"]),
                ],
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let root_scopes = &[
            ("nfs1", &["node-a::nfs1"][..]),
            ("nfs2", &["node-a::nfs2"][..]),
            ("nfs3", &["node-a::nfs3"][..]),
        ];
        let mut signals = vec![
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                root_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                root_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                root_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_FORCE_FIND),
                root_scopes,
                generation,
            ),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                sink_query_request_route_for(node_id).0,
                root_scopes,
                generation,
            ));
        }
        signals
    };
    let query_peer_wave = |generation| {
        let listener_scopes = &[
            ("nfs1", &["listener-a"][..]),
            ("nfs2", &["listener-a"][..]),
            ("nfs3", &["listener-a"][..]),
        ];
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                listener_scopes,
                generation,
            ),
        ]
    };

    let mut initial = source_wave(1);
    initial.extend(sink_wave(1));
    initial.extend(query_peer_wave(1));
    tokio::time::timeout(Duration::from_secs(2), app.on_control_frame(&initial))
        .await
        .expect(
            "generation-one mixed source/sink/query-peer local apply must settle within the bounded runtime-app control window instead of hanging after internal route endpoint spawn",
        )
        .expect(
            "generation-one mixed source/sink/query-peer local apply should succeed instead of exhausting the outer caller timeout",
        );

    assert!(
        app.control_initialized(),
        "successful generation-one mixed source/sink/query-peer local apply must leave runtime initialized",
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_status_routes_stay_available_while_published_facade_state_remains_serving_after_control_gate_reclose()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let nfs1_dir = tmp.path().join("nfs1");
    fs::create_dir_all(&nfs1_dir).expect("create nfs1 dir");
    fs::write(nfs1_dir.join("seed.txt"), b"a").expect("seed nfs1");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1_dir)],
            host_object_grants: vec![worker_export(
                "node-a::nfs1",
                "node-a",
                "127.0.0.1",
                nfs1_dir.clone(),
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
    };
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            cfg,
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
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
            Err(err) => panic!(
                "start app: {err}\nsource stderr:\n{}\nsink stderr:\n{}",
                worker_stderr_excerpt(&source_socket_dir),
                worker_stderr_excerpt(&sink_socket_dir),
            ),
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

    let root_scopes = &[("nfs1", &["node-a::nfs1"][..])];
    let listener_scopes = &[("nfs1", &["listener-a"][..])];
    let mut initial = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, root_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_FORCE_FIND),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
    ];
    for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        initial.push(activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            sink_query_request_route_for(node_id).0,
            root_scopes,
            2,
        ));
    }

    app.on_control_frame(&initial)
        .await
        .expect("activate internal status routes");

    FSMetaApp::publish_facade_service_state_from_runtime_state(
        &app.facade_service_state,
        FacadeServiceStateDecisionInput {
            control_gate_ready: true,
            publication_ready: true,
            pending_facade_present: false,
        },
    );
    assert_eq!(
        *app.facade_service_state
            .read()
            .expect("read published facade state"),
        FacadeServiceState::Serving,
        "precondition: active facade must already publish serving before gate reclose"
    );
    assert!(
        app.pending_facade.lock().await.is_none(),
        "precondition: this seam must not depend on a pending replacement facade"
    );

    app.api_control_gate.set_ready(false);

    let source_snapshots = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "source-status route must stay available while the published facade state remains serving even after the control gate recloses",
    );
    assert!(
        !source_snapshots.is_empty(),
        "source-status route should still return at least one snapshot while serving continuity holds"
    );

    let sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "sink-status route must stay available while the published facade state remains serving even after the control gate recloses",
    );
    let sink_snapshots = sink_events
        .iter()
        .map(|event| {
            rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                .expect("decode sink-status snapshot")
        })
        .collect::<Vec<_>>();
    assert!(
        !sink_snapshots.is_empty(),
        "sink-status route should still return at least one snapshot while serving continuity holds"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_status_routes_stay_available_on_peer_nodes_without_api_task_while_published_facade_state_remains_serving_after_control_gate_reclose()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let nfs1_dir = tmp.path().join("nfs1");
    fs::create_dir_all(&nfs1_dir).expect("create nfs1 dir");
    fs::write(nfs1_dir.join("seed.txt"), b"a").expect("seed nfs1");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1_dir)],
            host_object_grants: vec![worker_export(
                "node-a::nfs1",
                "node-a",
                "127.0.0.1",
                nfs1_dir.clone(),
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
    };
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            cfg,
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
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
            Err(err) => panic!(
                "start app: {err}\nsource stderr:\n{}\nsink stderr:\n{}",
                worker_stderr_excerpt(&source_socket_dir),
                worker_stderr_excerpt(&sink_socket_dir),
            ),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

    assert!(
        app.api_task.lock().await.is_none(),
        "peer-node continuity seam must not depend on a local api_task"
    );

    let root_scopes = &[("nfs1", &["node-a::nfs1"][..])];
    let listener_scopes = &[("nfs1", &["listener-a"][..])];
    let mut initial = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, root_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_FORCE_FIND),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
    ];
    for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        initial.push(activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            sink_query_request_route_for(node_id).0,
            root_scopes,
            2,
        ));
    }

    app.on_control_frame(&initial)
        .await
        .expect("activate internal status routes");

    FSMetaApp::publish_facade_service_state_from_runtime_state(
        &app.facade_service_state,
        FacadeServiceStateDecisionInput {
            control_gate_ready: true,
            publication_ready: true,
            pending_facade_present: false,
        },
    );
    assert_eq!(
        *app.facade_service_state
            .read()
            .expect("read published facade state"),
        FacadeServiceState::Serving,
        "precondition: peer node must already publish serving before gate reclose"
    );

    let source_snapshots = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "source-status route should be live on peer nodes once the published facade state is already serving",
    );
    assert!(
        !source_snapshots.is_empty(),
        "source-status precondition should return at least one snapshot on peer nodes once serving is published"
    );

    let initial_sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "sink-status route should be live on peer nodes once the published facade state is already serving",
    );
    assert!(
        !initial_sink_events.is_empty(),
        "sink-status precondition should produce at least one event on peer nodes once serving is published"
    );

    app.api_control_gate.set_ready(false);

    let source_snapshots_after_reclose = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "source-status route must stay available on peer nodes without api_task while the published facade state remains serving after the control gate recloses",
    );
    assert!(
        !source_snapshots_after_reclose.is_empty(),
        "source-status route should still return at least one snapshot on peer nodes while serving continuity holds"
    );

    let sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "sink-status route must stay available on peer nodes without api_task while the published facade state remains serving after the control gate recloses",
    );
    let sink_snapshots = sink_events
        .iter()
        .map(|event| {
            rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                .expect("decode sink-status snapshot")
        })
        .collect::<Vec<_>>();
    assert!(
        !sink_snapshots.is_empty(),
        "sink-status route should still return at least one snapshot on peer nodes while serving continuity holds"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_status_routes_stay_available_on_peer_nodes_without_api_task_when_published_facade_state_is_unavailable()
 {
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let nfs1_dir = tmp.path().join("nfs1");
    fs::create_dir_all(&nfs1_dir).expect("create nfs1 dir");
    fs::write(nfs1_dir.join("seed.txt"), b"a").expect("seed nfs1");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1_dir)],
            host_object_grants: vec![worker_export(
                "node-a::nfs1",
                "node-a",
                "127.0.0.1",
                nfs1_dir.clone(),
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
    };
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            cfg,
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
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
            Err(err) => panic!(
                "start app: {err}\nsource stderr:\n{}\nsink stderr:\n{}",
                worker_stderr_excerpt(&source_socket_dir),
                worker_stderr_excerpt(&sink_socket_dir),
            ),
        }
    } else {
        let err = app.start().await.expect_err("non-linux should fail fast");
        assert!(matches!(err, CnxError::NotSupported(_)));
        return;
    }

    assert!(
        app.api_task.lock().await.is_none(),
        "peer-node internal-status seam must not depend on a local public facade task"
    );

    let root_scopes = &[("nfs1", &["node-a::nfs1"][..])];
    let listener_scopes = &[("nfs1", &["listener-a"][..])];
    let mut initial = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, root_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_FORCE_FIND),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
    ];
    for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        initial.push(activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            sink_query_request_route_for(node_id).0,
            root_scopes,
            2,
        ));
    }

    app.on_control_frame(&initial)
        .await
        .expect("activate peer internal status routes");

    FSMetaApp::publish_facade_service_state_from_runtime_state(
        &app.facade_service_state,
        FacadeServiceStateDecisionInput {
            control_gate_ready: true,
            publication_ready: true,
            pending_facade_present: false,
        },
    );
    let initial_source_snapshots = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect("peer source-status precondition while serving is published");
    assert!(
        !initial_source_snapshots.is_empty(),
        "peer source-status precondition should return at least one snapshot before forcing facade state unavailable"
    );
    let initial_sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect("peer sink-status precondition while serving is published");
    assert!(
        !initial_sink_events.is_empty(),
        "peer sink-status precondition should return at least one event before forcing facade state unavailable"
    );

    *app.facade_service_state
        .write()
        .expect("write published facade state") = FacadeServiceState::Unavailable;
    app.api_control_gate.set_ready(false);

    let source_snapshots = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "peer source-status route must stay available for cluster aggregation even when the local published facade state is unavailable",
    );
    assert!(
        !source_snapshots.is_empty(),
        "peer source-status route should still return at least one snapshot when the local published facade state is unavailable"
    );

    let sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect(
        "peer sink-status route must stay available for cluster aggregation even when the local published facade state is unavailable",
    );
    assert!(
        !sink_events.is_empty(),
        "peer sink-status route should still return at least one event when the local published facade state is unavailable"
    );

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_status_routes_fail_closed_on_local_facade_owner_when_published_facade_state_is_not_serving_even_if_control_gate_is_ready()
{
    let tmp = tempdir().expect("create temp dir");
    let bind_addr = reserve_bind_addr();
    let (passwd_path, shadow_path) = write_auth_files(&tmp);
    let nfs1_dir = tmp.path().join("nfs1");
    fs::create_dir_all(&nfs1_dir).expect("create nfs1 dir");
    fs::write(nfs1_dir.join("seed.txt"), b"a").expect("seed nfs1");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "source");
    let sink_socket_dir = worker_role_socket_dir(worker_socket_root.path(), "sink");

    let cfg = FSMetaConfig {
        source: SourceConfig {
            roots: vec![worker_watch_scan_root("nfs1", &nfs1_dir)],
            host_object_grants: vec![worker_export(
                "node-a::nfs1",
                "node-a",
                "127.0.0.1",
                nfs1_dir.clone(),
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
    };
    let app = Arc::new(
        FSMetaApp::with_boundaries_and_state(
            cfg,
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-a".into()),
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
            Err(err) => panic!(
                "start app: {err}\nsource stderr:\n{}\nsink stderr:\n{}",
                worker_stderr_excerpt(&source_socket_dir),
                worker_stderr_excerpt(&sink_socket_dir),
            ),
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

    let root_scopes = &[("nfs1", &["node-a::nfs1"][..])];
    let listener_scopes = &[("nfs1", &["listener-a"][..])];
    let mut initial = vec![
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            root_scopes,
            2,
        ),
        activate_envelope_with_scope_rows(execution_units::SINK_RUNTIME_UNIT_ID, root_scopes, 2),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_EVENTS),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_FORCE_FIND),
            root_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
        activate_envelope_with_route_key_and_scope_rows(
            execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
            format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
            listener_scopes,
            2,
        ),
    ];
    for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        initial.push(activate_envelope_with_route_key_and_scope_rows(
            execution_units::SINK_RUNTIME_UNIT_ID,
            sink_query_request_route_for(node_id).0,
            root_scopes,
            2,
        ));
    }

    app.on_control_frame(&initial)
        .await
        .expect("activate internal status routes");

    app.api_control_gate.set_ready(true);
    FSMetaApp::publish_facade_service_state_from_runtime_state(
        &app.facade_service_state,
        FacadeServiceStateDecisionInput {
            control_gate_ready: true,
            publication_ready: true,
            pending_facade_present: false,
        },
    );

    let source_snapshots = internal_source_status_snapshots_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect("source-status precondition when published facade state is serving");
    assert!(
        !source_snapshots.is_empty(),
        "source-status precondition should produce at least one snapshot while serving is published"
    );

    let initial_sink_events = internal_sink_status_request_with_timeout(
        boundary.clone(),
        NodeId("node-a".into()),
        Duration::from_millis(700),
        Duration::from_millis(100),
    )
    .await
    .expect("sink-status precondition when published facade state is serving");
    assert!(
        !initial_sink_events.is_empty(),
        "sink-status precondition should produce at least one event while serving is published"
    );

    for published_state in [FacadeServiceState::Pending, FacadeServiceState::Unavailable] {
        *app.facade_service_state
            .write()
            .expect("write published facade state") = published_state;
        app.api_control_gate.set_ready(true);

        let source_request = internal_source_status_snapshots_with_timeout(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .await;
        assert!(
            source_request.is_err(),
            "source-status route must fail closed on a local facade owner once the published facade state is {published_state:?}, even if the control gate is ready",
        );

        let sink_request = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-a".into()),
            Duration::from_millis(250),
            Duration::from_millis(50),
        )
        .await;
        assert!(
            sink_request.is_err(),
            "sink-status route must fail closed on a local facade owner once the published facade state is {published_state:?}, even if the control gate is ready",
        );
    }

    app.close().await.expect("close app");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_backed_split_primary_initial_wave_arms_sink_before_source_publication() {
    struct SinkApplyPauseHookReset;
    struct SourceApplyEntryCountHookReset;

    impl Drop for SinkApplyPauseHookReset {
        fn drop(&mut self) {
            clear_sink_apply_pause_hook();
        }
    }

    impl Drop for SourceApplyEntryCountHookReset {
        fn drop(&mut self) {
            clear_source_apply_entry_count_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1_dir = tmp.path().join("nfs1");
    let nfs2_dir = tmp.path().join("nfs2");
    let nfs3_dir = tmp.path().join("nfs3");
    fs::create_dir_all(nfs1_dir.join("data")).expect("create nfs1 dir");
    fs::create_dir_all(nfs2_dir.join("data")).expect("create nfs2 dir");
    fs::create_dir_all(nfs3_dir.join("data")).expect("create nfs3 dir");
    fs::write(nfs1_dir.join("data").join("remote-a.txt"), b"a").expect("seed nfs1");
    fs::write(nfs2_dir.join("data").join("remote-b.txt"), b"b").expect("seed nfs2");
    fs::write(nfs3_dir.join("data").join("local-c.txt"), b"c").expect("seed nfs3");

    let boundary = Arc::new(DropUntilFirstEventsRecvBoundary::default());
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
                    roots: vec![
                        worker_watch_scan_root("nfs1", &nfs1_dir),
                        worker_watch_scan_root("nfs2", &nfs2_dir),
                        worker_watch_scan_root("nfs3", &nfs3_dir),
                    ],
                    host_object_grants: vec![
                        worker_export("node-a::nfs1", "node-a", "127.0.0.1", nfs1_dir.clone()),
                        worker_export("node-a::nfs2", "node-a", "127.0.0.2", nfs2_dir.clone()),
                        worker_export("node-b::nfs3", "node-b", "127.0.0.3", nfs3_dir.clone()),
                    ],
                    ..SourceConfig::default()
                },
                ..FSMetaConfig::default()
            },
            external_runtime_worker_binding("source", &source_socket_dir),
            external_runtime_worker_binding("sink", &sink_socket_dir),
            NodeId("node-b".into()),
            Some(boundary.clone()),
            Some(boundary.clone()),
            state_boundary,
        )
        .expect("init app"),
    );

    let bare_scopes = &[("nfs1", &[][..]), ("nfs2", &[][..]), ("nfs3", &[][..])];
    let local_sink_scopes = &[("nfs3", &["node-b::nfs3"][..])];
    let listener_scopes = &[
        ("nfs1", &["listener-a"][..]),
        ("nfs2", &["listener-a"][..]),
        ("nfs3", &["listener-a"][..]),
    ];
    let source_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                bare_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                bare_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                bare_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                bare_scopes,
                generation,
            ),
        ]
    };
    let sink_wave = |generation| {
        let mut signals = vec![
            activate_envelope_with_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                local_sink_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_EVENTS),
                local_sink_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                local_sink_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_QUERY),
                local_sink_scopes,
                generation,
            ),
        ];
        for node_id in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
            signals.push(activate_envelope_with_route_key_and_scope_rows(
                execution_units::SINK_RUNTIME_UNIT_ID,
                sink_query_request_route_for(node_id).0,
                local_sink_scopes,
                generation,
            ));
        }
        signals
    };
    let query_peer_wave = |generation| {
        vec![
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL),
                listener_scopes,
                generation,
            ),
            activate_envelope_with_route_key_and_scope_rows(
                execution_units::QUERY_PEER_RUNTIME_UNIT_ID,
                format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL),
                listener_scopes,
                generation,
            ),
        ]
    };

    let sink_entered = Arc::new(Notify::new());
    let sink_release = Arc::new(Notify::new());
    let _sink_pause_reset = SinkApplyPauseHookReset;
    install_sink_apply_pause_hook(SinkApplyPauseHook {
        entered: sink_entered.clone(),
        release: sink_release.clone(),
    });

    let source_apply_entries = Arc::new(AtomicUsize::new(0));
    let _source_entry_reset = SourceApplyEntryCountHookReset;
    install_source_apply_entry_count_hook(source_apply_entries.clone());

    let mut initial = source_wave(1);
    initial.extend(sink_wave(1));
    initial.extend(query_peer_wave(1));
    let control_task = tokio::spawn({
        let app = app.clone();
        async move { app.on_control_frame(&initial).await }
    });

    tokio::time::timeout(Duration::from_secs(5), sink_entered.notified())
        .await
        .expect("initial split-primary mixed wave should reach the sink-apply pause point");

    let source_observability_while_paused = app.source.observability_snapshot_nonblocking_for_status_route().await.0;
    assert_eq!(
        source_apply_entries.load(AtomicOrdering::Acquire),
        0,
        "worker-backed split-primary initial wave must arm sink apply before source apply so node-b local baseline publication cannot race ahead of sink stream readiness: dropped_nfs3={} events_recv_count={} source_observability={source_observability_while_paused:?}",
        boundary.dropped_origin_count("node-b::nfs3"),
        boundary.recv_count(&DropUntilFirstEventsRecvBoundary::events_route()),
    );

    sink_release.notify_waiters();
    control_task
        .await
        .expect("join initial split-primary mixed wave")
        .expect("initial split-primary mixed wave should succeed after sink-apply release");

    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last_sink_status = Err(CnxError::Timeout);
    let mut last_snapshots = Vec::<crate::sink::SinkStatusSnapshot>::new();
    while tokio::time::Instant::now() < ready_deadline {
        last_sink_status = internal_sink_status_request_with_timeout(
            boundary.clone(),
            NodeId("node-a".to_string()),
            Duration::from_millis(700),
            Duration::from_millis(100),
        )
        .await;
        if let Ok(events) = &last_sink_status {
            last_snapshots = events
                .iter()
                .map(|event| {
                    rmp_serde::from_slice::<crate::sink::SinkStatusSnapshot>(event.payload_bytes())
                        .expect("decode sink-status snapshot")
                })
                .collect::<Vec<_>>();
            if last_snapshots.iter().any(|snapshot| {
                snapshot.groups.iter().any(|group| {
                    group.group_id == "nfs3"
                        && group.is_ready()
                        && group.live_nodes > 0
                        && group.total_nodes > 0
                })
            }) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let source_observability = app
        .source
        .observability_snapshot_with_failure()
        .await
        .expect("source observability after initial split-primary mixed wave");
    assert!(
        last_snapshots.iter().any(|snapshot| {
            snapshot.groups.iter().any(|group| {
                group.group_id == "nfs3"
                    && group.is_ready()
                    && group.live_nodes > 0
                    && group.total_nodes > 0
            })
        }),
        "worker-backed split-primary initial wave must leave node-b sink-status ready for local nfs3 once source publishes baseline after sink stream readiness: sink_status_result={last_sink_status:?} sink_status_snapshots={last_snapshots:?} dropped_nfs3={} events_recv_count={} source_observability={source_observability:?}",
        boundary.dropped_origin_count("node-b::nfs3"),
        boundary.recv_count(&DropUntilFirstEventsRecvBoundary::events_route()),
    );

    app.close().await.expect("close app");
}
