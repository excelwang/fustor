    use super::*;
    use crate::EpochType;
    use crate::shared_types::query::UnreliableReason;
    use bytes::Bytes;
    use capanix_app_sdk::runtime::EventMetadata;
    use capanix_host_fs_types::UnixStat;
    use capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext;
    use capanix_runtime_entry_sdk::control::{
        RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
        RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
        RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick,
        encode_runtime_exec_control, encode_runtime_host_grant_change, encode_runtime_unit_tick,
    };

    struct NoopBoundary;

    #[async_trait::async_trait]
    impl ChannelIoSubset for NoopBoundary {}

    #[derive(Default)]
    struct RouteCountingTimeoutBoundary {
        recv_counts: std::sync::Mutex<std::collections::BTreeMap<String, usize>>,
    }

    impl RouteCountingTimeoutBoundary {
        fn recv_count(&self, route_key: &str) -> usize {
            self.recv_counts
                .lock()
                .expect("route recv counts lock")
                .get(route_key)
                .copied()
                .unwrap_or(0)
        }

        fn recv_counts_snapshot(&self) -> std::collections::BTreeMap<String, usize> {
            self.recv_counts
                .lock()
                .expect("route recv counts lock")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl ChannelIoSubset for RouteCountingTimeoutBoundary {
        async fn channel_recv(
            &self,
            _ctx: BoundaryContext,
            request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
        ) -> Result<Vec<Event>> {
            let route_key = request.channel_key.0;
            *self
                .recv_counts
                .lock()
                .expect("route recv counts lock")
                .entry(route_key)
                .or_default() += 1;
            Err(CnxError::Timeout)
        }
    }

    fn default_materialized_request() -> InternalQueryRequest {
        InternalQueryRequest::default()
    }

    fn decode_tree_payload(event: &Event) -> TreeGroupPayload {
        match rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .expect("decode materialized query payload")
        {
            MaterializedQueryPayload::Tree(payload) => payload,
            MaterializedQueryPayload::Stats(_) => {
                panic!("unexpected stats payload while decoding tree response")
            }
        }
    }

    fn payload_contains_path(payload: &TreeGroupPayload, path: &[u8]) -> bool {
        payload.root.exists && payload.root.path == path
            || payload.entries.iter().any(|entry| entry.path == path)
    }

    fn mk_source_event(origin: &str, record: FileMetaRecord) -> Event {
        let payload = rmp_serde::to_vec_named(&record).expect("encode record");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: record.unix_stat.mtime_us,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn mk_record(path: &[u8], file_name: &str, ts: u64, event_kind: EventKind) -> FileMetaRecord {
        mk_record_with_track(path, file_name, ts, event_kind, crate::SyncTrack::Scan)
    }

    fn mk_record_with_track(
        path: &[u8],
        file_name: &str,
        ts: u64,
        event_kind: EventKind,
        source: crate::SyncTrack,
    ) -> FileMetaRecord {
        FileMetaRecord::from_unix(
            path.to_vec(),
            file_name.as_bytes().to_vec(),
            UnixStat {
                is_dir: false,
                size: 1,
                mtime_us: ts,
                ctime_us: ts,
                dev: None,
                ino: None,
            },
            event_kind,
            true,
            source,
            b"/".to_vec(),
            ts,
            false,
        )
    }

    fn mk_control_event(origin: &str, control: ControlEvent, ts: u64) -> Event {
        let payload = rmp_serde::to_vec_named(&control).expect("encode control event");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: ts,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn granted_mount_root(
        object_ref: &str,
        host_ref: &str,
        host_ip: &str,
        mount_point: impl Into<std::path::PathBuf>,
        active: bool,
    ) -> GrantedMountRoot {
        let mount_point = mount_point.into();
        GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: mount_point.clone(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active,
        }
    }

    fn build_single_group_sink() -> SinkFileMeta {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("build sink")
    }

    fn finished_endpoint_task_for_test(route_key: &str) -> ManagedEndpointTask {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let task = ManagedEndpointTask::spawn(
            Arc::new(NoopBoundary),
            capanix_app_sdk::runtime::RouteKey(route_key.to_string()),
            format!("test-finished-{route_key}"),
            shutdown,
            |_requests| async { Vec::<Event>::new() },
        );
        let started = std::time::Instant::now();
        while !task.is_finished() && started.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            task.is_finished(),
            "test fixture endpoint task must finish deterministically before insertion"
        );
        task
    }

    fn bound_scope(scope_id: &str) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: Vec::new(),
        }
    }

    fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
        }
    }

    fn host_object_grants_changed_envelope(
        version: u64,
        grants: &[GrantedMountRoot],
    ) -> ControlEnvelope {
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version,
            grants: grants
                .iter()
                .map(|grant| RuntimeHostGrant {
                    object_ref: grant.object_ref.clone(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: grant.interfaces.clone(),
                    host: RuntimeHostDescriptor {
                        host_ref: grant.host_ref.clone(),
                        host_ip: grant.host_ip.clone(),
                        host_name: grant.host_name.clone(),
                        site: grant.site.clone(),
                        zone: grant.zone.clone(),
                        host_labels: grant.host_labels.clone(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: grant.mount_point.display().to_string(),
                        fs_source: grant.fs_source.clone(),
                        fs_type: grant.fs_type.clone(),
                        mount_options: grant.mount_options.clone(),
                    },
                    grant_state: if grant.active {
                        RuntimeHostGrantState::Active
                    } else {
                        RuntimeHostGrantState::Revoked
                    },
                })
                .collect(),
        })
        .expect("encode runtime host object grants changed envelope")
    }

    #[tokio::test]
    async fn unit_tick_control_frame_is_accepted() {
        let sink = build_single_group_sink();
        let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        sink.on_control_frame(&[envelope])
            .await
            .expect("sink should accept unit tick control frame");
    }

    #[tokio::test]
    async fn start_runtime_endpoints_restarts_after_finished_endpoint_task() {
        let sink = build_single_group_sink();
        lock_or_recover(
            &sink.endpoint_tasks,
            "test.sink.restart_after_finished.endpoint_tasks.seed",
        )
        .push(finished_endpoint_task_for_test("sink.test.finished:v1.req"));

        sink.start_runtime_endpoints(Arc::new(NoopBoundary), NodeId("node-a".to_string()))
            .expect("start runtime endpoints");

        let endpoint_count = lock_or_recover(
            &sink.endpoint_tasks,
            "test.sink.restart_after_finished.endpoint_tasks.after",
        )
        .len();
        assert!(
            endpoint_count > 1,
            "sink runtime start must prune terminal endpoint tasks and restart endpoints instead of treating any non-empty task list as already-started"
        );

        sink.close().await.expect("close sink");
    }

    #[tokio::test]
    async fn roots_control_stream_stays_gated_until_route_activation() {
        let sink = build_single_group_sink();
        let boundary = Arc::new(RouteCountingTimeoutBoundary::default());

        sink.start_runtime_endpoints(boundary.clone(), NodeId("node-a".to_string()))
            .expect("start runtime endpoints");

        let roots_control_route = format!(
            "{}.stream",
            crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
        );
        let pre_activation_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < pre_activation_deadline {
            let recv = boundary.recv_count(&roots_control_route);
            assert_eq!(
                recv,
                0,
                "sink roots-control stream must remain gated before runtime route activation; recv_counts={:?}",
                boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key: roots_control_route.clone(),
                unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("nfs1")],
            },
        ))
        .expect("encode sink roots-control activate")])
            .await
            .expect("activate sink roots-control route");

        let activated_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let recv = boundary.recv_count(&roots_control_route);
            if recv > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < activated_deadline,
                "sink roots-control stream must begin receiving after route activation; recv_counts={:?}",
                boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        sink.close().await.expect("close sink");
    }

    #[tokio::test]
    async fn events_stream_stays_gated_until_route_activation() {
        let sink = build_single_group_sink();
        let boundary = Arc::new(RouteCountingTimeoutBoundary::default());

        assert!(
            sink.has_scheduled_stream_targets(),
            "fixture must include scheduled stream targets so pre-activation recv attempts are meaningful"
        );

        sink.start_runtime_endpoints(boundary.clone(), NodeId("node-a".to_string()))
            .expect("start runtime endpoints");
        sink.enable_stream_receive();

        let events_route = format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS);
        let pre_activation_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < pre_activation_deadline {
            let recv = boundary.recv_count(&events_route);
            assert_eq!(
                recv,
                0,
                "sink events stream must remain gated before runtime route activation; recv_counts={:?}",
                boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key: events_route.clone(),
                unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("nfs1")],
            },
        ))
        .expect("encode sink events activate")])
            .await
            .expect("activate sink events route");

        let activated_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let recv = boundary.recv_count(&events_route);
            if recv > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < activated_deadline,
                "sink events stream must begin receiving after route activation; recv_counts={:?}",
                boundary.recv_counts_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        sink.close().await.expect("close sink");
    }

    #[tokio::test]
    async fn unit_tick_with_unknown_unit_id_is_rejected() {
        let sink = build_single_group_sink();
        let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.unknown".to_string(),
            generation: 1,
            at_ms: 1,
        })
        .expect("encode unit tick");

        let err = sink
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn exec_activate_with_unknown_unit_id_is_rejected() {
        let sink = build_single_group_sink();
        let envelope =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.unknown".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode exec activate");

        let err = sink
            .on_control_frame(&[envelope])
            .await
            .expect_err("unknown unit id must be rejected");
        assert!(matches!(err, CnxError::NotSupported(_)));
        assert!(err.to_string().contains("unsupported unit_id"));
    }

    #[tokio::test]
    async fn stale_deactivate_generation_is_ignored() {
        let sink = build_single_group_sink();
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let stale_deactivate =
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 4,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        sink.on_control_frame(&[stale_deactivate])
            .await
            .expect("stale deactivate should be ignored");

        let state = sink.unit_control.snapshot("runtime.exec.sink");
        assert_eq!(state, Some((5, true)));
    }

    #[tokio::test]
    async fn stale_activate_generation_is_ignored() {
        let sink = build_single_group_sink();
        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let deactivate =
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        sink.on_control_frame(&[deactivate])
            .await
            .expect("deactivate should pass");

        let stale_activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 4,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode stale activate");
        sink.on_control_frame(&[stale_activate])
            .await
            .expect("stale activate should be ignored");

        let state = sink.unit_control.snapshot("runtime.exec.sink");
        assert_eq!(state, Some((5, false)));
    }

    #[test]
    fn force_find_aggregation_delete_wins_on_same_mtime() {
        let update = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Update));
        let delete = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Delete));
        let query = query_response_from_source_events(&[update, delete], b"/")
            .expect("decode source events");
        assert!(query.nodes.is_empty());
    }

    #[test]
    fn force_find_stats_are_derived_from_aggregated_query() {
        let file = mk_source_event(
            "src",
            FileMetaRecord::scan_update(
                b"/a.txt".to_vec(),
                b"a.txt".to_vec(),
                UnixStat {
                    is_dir: false,
                    size: 42,
                    mtime_us: 10,
                    ctime_us: 10,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                10,
                false,
            ),
        );
        let dir = mk_source_event(
            "src",
            FileMetaRecord::scan_update(
                b"/dir".to_vec(),
                b"dir".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 11,
                    ctime_us: 11,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                11,
                false,
            ),
        );

        let query =
            query_response_from_source_events(&[file, dir], b"/").expect("decode source events");
        let stats = subtree_stats_from_query_response(&query);
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.total_dirs, 1);
        assert_eq!(stats.total_size, 42);
    }

    #[test]
    fn force_find_aggregation_rejects_invalid_source_payloads() {
        let bad = Event::new(
            EventMetadata {
                origin_id: NodeId("src".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from_static(b"not-msgpack-record"),
        );
        assert!(query_response_from_source_events(&[bad], b"/").is_err());
    }

    #[test]
    fn ensure_group_state_requires_explicit_group_mapping() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = Vec::new();
        let mut state = SinkState::new(&cfg);
        let err = match state.ensure_group_state_mut("node-a::nfs1") {
            Ok(_) => panic!("unknown object ref must fail closed"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("configured group mapping"));
    }

    #[tokio::test]
    async fn scheduled_root_id_stream_events_materialize_ready_state_without_host_grants() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = Vec::new();
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "nfs1",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 1,
                        ctime_us: 1,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    1,
                    false,
                ),
            ),
            mk_control_event(
                "nfs1",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_source_event(
                "nfs1",
                mk_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
            ),
            mk_control_event(
                "nfs1",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
            mk_source_event(
                "nfs2",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 5,
                        ctime_us: 5,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    5,
                    false,
                ),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
            mk_source_event(
                "nfs2",
                mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                8,
            ),
        ])
        .expect("scheduled root-id stream events should apply");

        let snapshot = sink.status_snapshot().expect("sink status");
        let ready_groups = snapshot
            .groups
            .iter()
            .filter(|group| group.initial_audit_completed && group.live_nodes > 0)
            .map(|group| group.group_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            ready_groups,
            std::collections::BTreeSet::from(["nfs1", "nfs2"]),
            "scheduled root-id stream events must materialize ready state even when host grants are temporarily absent: {snapshot:?}"
        );
    }

    #[test]
    fn group_primary_prefers_lowest_active_process_member() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![
            granted_mount_root("node-b::exp", "node-b", "10.0.0.12", "/mnt/nfs1", false),
            granted_mount_root("node-a::exp", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        ];

        let state = SinkState::new(&cfg);
        let group = state.groups.get("nfs1").expect("group must exist");
        assert_eq!(group.primary_object_ref, "node-a::exp");
    }

    #[test]
    fn update_logical_roots_repartitions_groups_with_current_exports() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("old-root", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
            .expect("init sink");
        {
            let state = sink.state.read().expect("state lock");
            assert!(state.groups.contains_key("old-root"));
            assert!(!state.groups.contains_key("new-root"));
        }

        sink.update_logical_roots(
            vec![RootSpec::new("new-root", "/mnt/nfs1")],
            &cfg.host_object_grants,
        )
        .expect("update roots");

        let state = sink.state.read().expect("state lock");
        assert!(!state.groups.contains_key("old-root"));
        assert!(state.groups.contains_key("new-root"));
    }

    #[tokio::test]
    async fn activate_limits_sink_state_to_bound_scopes_only() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate");

        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        let state = sink.state.read().expect("state lock");
        assert!(state.groups.contains_key("root-a"));
        assert!(!state.groups.contains_key("root-b"));
        drop(state);

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string()])
        );
    }

    #[tokio::test]
    async fn deactivate_then_reactivate_preserves_ready_materialized_group_state() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a"), bound_scope("root-b")],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.send(&[
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/ready-a.txt", "ready-a.txt", 1, EventKind::Update),
            ),
            mk_control_event(
                "node-a::exp-a",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-a::exp-a",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready-b.txt", "ready-b.txt", 4, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                5,
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
        ])
        .await
        .expect("seed ready materialized state");

        let snapshot_before = sink.status_snapshot().expect("sink status before deactivate");
        assert!(snapshot_before.groups.iter().all(|group| group.initial_audit_completed));
        let revisions_before = snapshot_before
            .groups
            .iter()
            .map(|group| (group.group_id.clone(), group.materialized_revision))
            .collect::<std::collections::BTreeMap<_, _>>();

        let deactivate =
            encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                reason: "test".to_string(),
            }))
            .expect("encode deactivate");
        sink.on_control_frame(&[deactivate])
            .await
            .expect("deactivate should pass");

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![bound_scope("root-a"), bound_scope("root-b")],
            }))
            .expect("encode reactivate both");
        sink.on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass");

        let snapshot_after = sink.status_snapshot().expect("sink status after reactivate");
        assert!(
            snapshot_after.groups.iter().all(|group| group.initial_audit_completed),
            "deactivate/reactivate continuity must not regress ready groups back to initial_audit_completed=false: {snapshot_after:?}"
        );
        let revisions_after = snapshot_after
            .groups
            .iter()
            .map(|group| (group.group_id.clone(), group.materialized_revision))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            revisions_after, revisions_before,
            "deactivate/reactivate continuity must preserve materialized revision instead of recreating groups from scratch"
        );
    }

    #[tokio::test]
    async fn scope_wobble_does_not_reset_ready_group_state_after_stream_apply() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready-b.txt", "ready-b.txt", 2, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .expect("seed stream-applied ready state for root-b");

        let snapshot_before = sink.status_snapshot().expect("status before scope wobble");
        let root_b_before = snapshot_before
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group before scope wobble");
        assert!(
            root_b_before.initial_audit_completed,
            "precondition: root-b must be ready before scope wobble"
        );
        assert!(
            root_b_before.live_nodes > 0,
            "precondition: root-b must have live materialized nodes before scope wobble"
        );
        assert!(
            root_b_before.materialized_revision > 1,
            "precondition: root-b must have advanced materialized revision before scope wobble"
        );
        let applied_origin_counts = snapshot_before
            .stream_applied_origin_counts_by_node
            .get("node-a")
            .expect("stream-applied origin counts for local sink node");
        assert!(
            applied_origin_counts
                .iter()
                .any(|entry| entry.starts_with("node-b::exp-b=")),
            "precondition: stream-applied origin counts must include root-b source origin before scope wobble: {snapshot_before:?}"
        );

        let activate_root_a_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("root-a", &["node-a::exp-a"])],
            }))
            .expect("encode activate root-a only");
        sink.on_control_frame(&[activate_root_a_only])
            .await
            .expect("activate root-a only should pass");

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode reactivate both");
        sink.on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass");

        let snapshot_after = sink.status_snapshot().expect("status after scope wobble");
        let root_b_after = snapshot_after
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group after scope wobble");
        assert!(
            root_b_after.initial_audit_completed,
            "ready root-b must stay ready across a non-empty scope wobble instead of regressing to initial_audit_completed=false: {snapshot_after:?}"
        );
        assert!(
            root_b_after.live_nodes > 0,
            "ready root-b must keep live materialized nodes across a non-empty scope wobble instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            root_b_after.materialized_revision, root_b_before.materialized_revision,
            "ready root-b must preserve materialized revision across a non-empty scope wobble instead of resetting to revision 1"
        );
    }

    #[tokio::test]
    async fn alternating_scope_wobble_preserves_ready_materialized_state_for_all_groups() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_control_event(
                "node-a::exp-a",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/ready-a.txt", "ready-a.txt", 2, EventKind::Update),
            ),
            mk_control_event(
                "node-a::exp-a",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready-b.txt", "ready-b.txt", 5, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
        ])
        .expect("seed ready stream-applied state for both groups");

        let snapshot_before = sink.status_snapshot().expect("status before alternating wobble");
        let before_groups = snapshot_before
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id.clone(),
                    (
                        group.initial_audit_completed,
                        group.live_nodes,
                        group.materialized_revision,
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            before_groups.get("root-a").map(|row| row.0),
            Some(true),
            "precondition: root-a must be ready before alternating scope wobble: {snapshot_before:?}"
        );
        assert_eq!(
            before_groups.get("root-b").map(|row| row.0),
            Some(true),
            "precondition: root-b must be ready before alternating scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("root-a").is_some_and(|row| row.1 > 0),
            "precondition: root-a must have live materialized nodes before alternating scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("root-b").is_some_and(|row| row.1 > 0),
            "precondition: root-b must have live materialized nodes before alternating scope wobble: {snapshot_before:?}"
        );

        let activate_root_a_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("root-a", &["node-a::exp-a"])],
            }))
            .expect("encode activate root-a only");
        sink.on_control_frame(&[activate_root_a_only])
            .await
            .expect("activate root-a only should pass");

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode reactivate both");
        sink.on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass");

        let activate_root_b_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 4,
                expires_at_ms: 4,
                bound_scopes: vec![bound_scope_with_resources("root-b", &["node-b::exp-b"])],
            }))
            .expect("encode activate root-b only");
        sink.on_control_frame(&[activate_root_b_only])
            .await
            .expect("activate root-b only should pass");

        let reactivate_both_again =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 5,
                expires_at_ms: 5,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode reactivate both again");
        sink.on_control_frame(&[reactivate_both_again])
            .await
            .expect("reactivate both again should pass");

        let snapshot_after = sink.status_snapshot().expect("status after alternating wobble");
        let after_groups = snapshot_after
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id.clone(),
                    (
                        group.initial_audit_completed,
                        group.live_nodes,
                        group.materialized_revision,
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            after_groups.get("root-a").map(|row| row.0),
            Some(true),
            "ready root-a must stay ready across alternating single-group runtime scope wobble instead of regressing to initial_audit_completed=false: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("root-b").map(|row| row.0),
            Some(true),
            "ready root-b must stay ready across alternating single-group runtime scope wobble instead of regressing to initial_audit_completed=false: {snapshot_after:?}"
        );
        assert!(
            after_groups.get("root-a").is_some_and(|row| row.1 > 0),
            "ready root-a must keep live materialized nodes across alternating scope wobble instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert!(
            after_groups.get("root-b").is_some_and(|row| row.1 > 0),
            "ready root-b must keep live materialized nodes across alternating scope wobble instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("root-a").map(|row| row.2),
            before_groups.get("root-a").map(|row| row.2),
            "root-a materialized revision must survive alternating scope wobble instead of resetting from scratch: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("root-b").map(|row| row.2),
            before_groups.get("root-b").map(|row| row.2),
            "root-b materialized revision must survive alternating scope wobble instead of resetting from scratch: {snapshot_after:?}"
        );
    }

    #[tokio::test]
    async fn logical_roots_window_missing_group_does_not_reset_ready_state_on_readd() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        let host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        cfg.host_object_grants = host_object_grants.clone();
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready-b.txt", "ready-b.txt", 2, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .expect("seed stream-applied ready state for root-b");

        let snapshot_before = sink
            .status_snapshot()
            .expect("status before logical-roots window wobble");
        let root_b_before = snapshot_before
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group before logical-roots window wobble");
        assert!(
            root_b_before.initial_audit_completed,
            "precondition: root-b must be ready before logical-roots window wobble"
        );
        assert!(
            root_b_before.live_nodes > 0,
            "precondition: root-b must have live materialized nodes before logical-roots window wobble"
        );
        assert!(
            root_b_before.materialized_revision > 1,
            "precondition: root-b must have advanced materialized revision before logical-roots window wobble"
        );
        let applied_origin_counts = snapshot_before
            .stream_applied_origin_counts_by_node
            .get("node-a")
            .expect("stream-applied origin counts for local sink node");
        assert!(
            applied_origin_counts
                .iter()
                .any(|entry| entry.starts_with("node-b::exp-b=")),
            "precondition: stream-applied origin counts must include root-b source origin before logical-roots window wobble: {snapshot_before:?}"
        );

        sink.update_logical_roots(vec![RootSpec::new("root-a", "/mnt/nfs1")], &host_object_grants)
            .expect("temporary logical-roots window omitting root-b");
        {
            let state = sink
                .state
                .read()
                .expect("state lock after logical-roots omission");
            assert!(
                !state.groups.contains_key("root-b"),
                "temporary logical-roots omission should move root-b out of active groups"
            );
            let retained_root_b = state
                .retained_groups
                .get("root-b")
                .expect("temporary logical-roots omission must retain ready root-b state");
            assert!(
                retained_root_b.initial_audit_completed,
                "retained root-b must stay ready during temporary logical-roots omission"
            );
            assert!(
                retained_root_b.tree.node_count() > 0,
                "retained root-b must keep materialized nodes during temporary logical-roots omission"
            );
            assert_eq!(
                retained_root_b.materialized_revision, root_b_before.materialized_revision,
                "retained root-b must preserve materialized revision during temporary logical-roots omission"
            );
        }
        sink.update_logical_roots(
            vec![
                RootSpec::new("root-a", "/mnt/nfs1"),
                RootSpec::new("root-b", "/mnt/nfs2"),
            ],
            &host_object_grants,
        )
        .expect("logical-roots window restore should re-add root-b");

        let snapshot_after = sink
            .status_snapshot()
            .expect("status after logical-roots window wobble");
        let root_b_after = snapshot_after
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group after logical-roots window wobble");
        assert!(
            root_b_after.initial_audit_completed,
            "ready root-b must stay ready across temporary logical-roots omission instead of regressing to initial_audit_completed=false: {snapshot_after:?}"
        );
        assert!(
            root_b_after.live_nodes > 0,
            "ready root-b must keep live materialized nodes across temporary logical-roots omission instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            root_b_after.materialized_revision, root_b_before.materialized_revision,
            "ready root-b must preserve materialized revision across temporary logical-roots omission instead of resetting to revision 1"
        );
    }

    #[tokio::test]
    async fn retained_ready_group_state_survives_reopen_after_scope_wobble_and_later_reactivate() {
        let state_boundary = in_memory_state_boundary();
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary.clone(),
            cfg.clone(),
        )
        .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready-b.txt", "ready-b.txt", 2, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .expect("seed ready stream-applied state for root-b");

        let snapshot_before = sink.status_snapshot().expect("status before scope wobble");
        let root_b_before = snapshot_before
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group before scope wobble");
        assert!(
            root_b_before.initial_audit_completed,
            "precondition: root-b must be ready before scope wobble: {snapshot_before:?}"
        );
        assert!(
            root_b_before.live_nodes > 0,
            "precondition: root-b must have live materialized nodes before scope wobble: {snapshot_before:?}"
        );
        assert!(
            root_b_before.materialized_revision > 1,
            "precondition: root-b must have advanced materialized revision before scope wobble: {snapshot_before:?}"
        );

        let activate_root_a_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("root-a", &["node-a::exp-a"])],
            }))
            .expect("encode activate root-a only");
        sink.on_control_frame(&[activate_root_a_only])
            .await
            .expect("activate root-a only should pass");

        {
            let state = sink.state.read().expect("state lock after scope wobble");
            assert!(
                !state.groups.contains_key("root-b"),
                "scope wobble should move root-b out of active groups: groups={:?}",
                state.groups.keys().collect::<Vec<_>>()
            );
            let retained_root_b = state
                .retained_groups
                .get("root-b")
                .expect("root-b must be retained after scope wobble");
            assert!(
                retained_root_b.initial_audit_completed,
                "retained root-b must keep ready state before reopen"
            );
            assert!(
                retained_root_b.tree.node_count() > 0,
                "retained root-b must keep materialized nodes before reopen"
            );
        }

        sink.close().await.expect("close sink before reopen");

        let reopened = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary,
            cfg,
        )
        .expect("reopen sink with same state boundary");

        {
            let state = reopened.state.read().expect("state lock after reopen");
            let retained_root_b = state.retained_groups.get("root-b").expect(
                "reopened sink must preserve retained ready group state across the shared state boundary",
            );
            assert!(
                retained_root_b.initial_audit_completed,
                "reopened retained root-b must keep ready state instead of regressing to init=false"
            );
            assert!(
                retained_root_b.tree.node_count() > 0,
                "reopened retained root-b must keep materialized nodes instead of regressing to an empty tree"
            );
        }

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode reactivate both");
        reopened
            .on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass after reopen");

        let snapshot_after = reopened
            .status_snapshot()
            .expect("status after reopen + reactivate");
        let root_b_after = snapshot_after
            .groups
            .iter()
            .find(|group| group.group_id == "root-b")
            .expect("root-b group after reopen + reactivate");
        assert!(
            root_b_after.initial_audit_completed,
            "later reactivate must preserve retained ready root-b state across reopen instead of regressing to initial_audit_completed=false: {snapshot_after:?}"
        );
        assert!(
            root_b_after.live_nodes > 0,
            "later reactivate must preserve retained live root-b nodes across reopen instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            root_b_after.materialized_revision, root_b_before.materialized_revision,
            "later reactivate must preserve retained root-b materialized revision across reopen instead of resetting to revision 1"
        );

        let query_events = reopened
            .materialized_query(&default_materialized_request())
            .expect("query after reopen + reactivate");
        let responses = query_events
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let root_b_response = responses
            .get("root-b")
            .expect("root-b response after reopen + reactivate");
        assert!(
            payload_contains_path(root_b_response, b"/ready-b.txt"),
            "later reactivate must preserve retained root-b tree payload across reopen instead of recreating it from scratch"
        );

        reopened.close().await.expect("close reopened sink");
    }

    #[tokio::test]
    async fn retained_root_id_ready_state_persists_control_only_scope_wobble_before_reopen_without_close()
     {
        let state_boundary = in_memory_state_boundary();
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = Vec::new();
        let sink = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary.clone(),
            cfg.clone(),
        )
        .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "nfs2",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 1,
                        ctime_us: 1,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    1,
                    false,
                ),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_source_event(
                "nfs2",
                mk_record(b"/ready-b.txt", "ready-b.txt", 3, EventKind::Update),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
        ])
        .expect("seed ready root-id stream-applied state for nfs2");

        let snapshot_before = sink.status_snapshot().expect("status before scope wobble");
        let nfs2_before = snapshot_before
            .groups
            .iter()
            .find(|group| group.group_id == "nfs2")
            .expect("nfs2 before scope wobble");
        assert!(
            nfs2_before.initial_audit_completed,
            "precondition: nfs2 must be ready before scope wobble: {snapshot_before:?}"
        );
        assert!(
            nfs2_before.live_nodes > 0,
            "precondition: nfs2 must have live materialized nodes before scope wobble: {snapshot_before:?}"
        );
        assert!(
            nfs2_before.materialized_revision > 1,
            "precondition: nfs2 must have advanced materialized revision before scope wobble: {snapshot_before:?}"
        );

        let activate_nfs1_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode activate nfs1 only");
        sink.on_control_frame(&[activate_nfs1_only])
            .await
            .expect("activate nfs1 only should pass");

        {
            let state = sink.state.read().expect("state lock after scope wobble");
            assert!(
                !state.groups.contains_key("nfs2"),
                "scope wobble should move nfs2 out of active groups: groups={:?}",
                state.groups.keys().collect::<Vec<_>>()
            );
            let retained_nfs2 = state
                .retained_groups
                .get("nfs2")
                .expect("nfs2 must be retained after scope wobble");
            assert!(
                retained_nfs2.initial_audit_completed,
                "retained nfs2 must keep ready state in memory before reopen"
            );
            assert!(
                retained_nfs2.tree.node_count() > 0,
                "retained nfs2 must keep materialized nodes in memory before reopen"
            );
        }

        let reopened = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary,
            cfg,
        )
        .expect("reopen sink from shared state boundary without explicit close");

        {
            let state = reopened
                .state
                .read()
                .expect("reopened state after control-only scope wobble");
            assert!(
                !state.groups.contains_key("nfs2"),
                "control-only scope wobble must persist nfs2 as retained before reopen instead of restoring stale active state: active_groups={:?} retained_groups={:?}",
                state.groups.keys().collect::<Vec<_>>(),
                state.retained_groups.keys().collect::<Vec<_>>()
            );
            let retained_nfs2 = state.retained_groups.get("nfs2").expect(
                "reopened sink must preserve retained root-id ready state across control-only scope wobble without requiring close()",
            );
            assert!(
                retained_nfs2.initial_audit_completed,
                "reopened retained nfs2 must keep ready state instead of regressing to init=false"
            );
            assert!(
                retained_nfs2.tree.node_count() > 0,
                "reopened retained nfs2 must keep materialized nodes instead of regressing to an empty tree"
            );
            assert_eq!(
                retained_nfs2.materialized_revision, nfs2_before.materialized_revision,
                "reopened retained nfs2 must preserve materialized revision across control-only scope wobble without requiring close()"
            );
        }

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode reactivate both");
        reopened
            .on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass after reopen");

        let snapshot_after = reopened
            .status_snapshot()
            .expect("status after reopen + reactivate");
        let nfs2_after = snapshot_after
            .groups
            .iter()
            .find(|group| group.group_id == "nfs2")
            .expect("nfs2 after reopen + reactivate");
        assert!(
            nfs2_after.initial_audit_completed,
            "later reactivate must preserve retained root-id ready state across reopen instead of regressing to init=false: {snapshot_after:?}"
        );
        assert!(
            nfs2_after.live_nodes > 0,
            "later reactivate must preserve retained root-id live nodes across reopen instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            nfs2_after.materialized_revision, nfs2_before.materialized_revision,
            "later reactivate must preserve retained root-id materialized revision across reopen instead of resetting to revision 1"
        );

        let query_events = reopened
            .materialized_query(&default_materialized_request())
            .expect("query after reopen + reactivate");
        let responses = query_events
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let nfs2_response = responses
            .get("nfs2")
            .expect("nfs2 response after reopen + reactivate");
        assert!(
            payload_contains_path(nfs2_response, b"/ready-b.txt"),
            "later reactivate must preserve retained root-id tree payload across reopen instead of recreating it from scratch"
        );

        reopened.close().await.expect("close reopened sink");
        sink.close().await.expect("close original sink");
    }

    #[tokio::test]
    async fn retained_root_id_ready_state_restores_when_logical_roots_sync_readds_scheduled_scope_after_control_only_wobble_without_new_stream_events()
     {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = Vec::new();
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "nfs2",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 1,
                        ctime_us: 1,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    1,
                    false,
                ),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_source_event(
                "nfs2",
                mk_record(b"/ready-b.txt", "ready-b.txt", 3, EventKind::Update),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
        ])
        .expect("seed ready root-id stream-applied state for nfs2");

        let snapshot_before = sink.status_snapshot().expect("status before scope wobble");
        let nfs2_before = snapshot_before
            .groups
            .iter()
            .find(|group| group.group_id == "nfs2")
            .expect("nfs2 before scope wobble");
        assert!(
            nfs2_before.initial_audit_completed,
            "precondition: nfs2 must be ready before scope wobble: {snapshot_before:?}"
        );
        assert!(
            nfs2_before.live_nodes > 0,
            "precondition: nfs2 must have live materialized nodes before scope wobble: {snapshot_before:?}"
        );
        assert!(
            nfs2_before.materialized_revision > 1,
            "precondition: nfs2 must have advanced materialized revision before scope wobble: {snapshot_before:?}"
        );

        let activate_nfs1_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode activate nfs1 only");
        sink.on_control_frame(&[activate_nfs1_only])
            .await
            .expect("activate nfs1 only should pass");

        {
            let state = sink.state.read().expect("state lock after control-only wobble");
            assert!(
                !state.groups.contains_key("nfs2"),
                "control-only wobble should move nfs2 out of active groups: groups={:?}",
                state.groups.keys().collect::<Vec<_>>()
            );
            let retained_nfs2 = state
                .retained_groups
                .get("nfs2")
                .expect("nfs2 must be retained after control-only wobble");
            assert!(
                retained_nfs2.initial_audit_completed,
                "retained nfs2 must keep ready state after control-only wobble"
            );
            assert!(
                retained_nfs2.tree.node_count() > 0,
                "retained nfs2 must keep materialized nodes after control-only wobble"
            );
        }

        sink.update_logical_roots(
            vec![
                RootSpec::new("nfs1", "/mnt/nfs1"),
                RootSpec::new("nfs2", "/mnt/nfs2"),
            ],
            &[],
        )
        .expect("later logical-roots sync should restore runtime scope without new stream events");

        let snapshot_after = sink
            .status_snapshot()
            .expect("status after logical-roots sync restores runtime scope");
        let nfs2_after = snapshot_after
            .groups
            .iter()
            .find(|group| group.group_id == "nfs2")
            .expect("nfs2 after logical-roots sync restores runtime scope");
        assert!(
            nfs2_after.initial_audit_completed,
            "later logical-roots sync must restore retained nfs2 ready state instead of regressing to init=false: {snapshot_after:?}"
        );
        assert!(
            nfs2_after.live_nodes > 0,
            "later logical-roots sync must restore retained nfs2 live nodes instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            nfs2_after.materialized_revision, nfs2_before.materialized_revision,
            "later logical-roots sync must preserve retained nfs2 materialized revision instead of resetting to revision 1"
        );

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query after logical-roots sync restores runtime scope");
        let responses = query_events
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let nfs2_response = responses
            .get("nfs2")
            .expect("nfs2 response after logical-roots sync restores runtime scope");
        assert!(
            payload_contains_path(nfs2_response, b"/ready-b.txt"),
            "later logical-roots sync must restore retained nfs2 tree payload instead of recreating it from scratch"
        );

        sink.close().await.expect("close sink");
    }

    #[tokio::test]
    async fn retained_root_id_ready_state_survives_same_instance_scope_wobble_and_later_reactivate()
     {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = Vec::new();
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "nfs1",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 1,
                        ctime_us: 1,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    1,
                    false,
                ),
            ),
            mk_control_event(
                "nfs1",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_source_event(
                "nfs1",
                mk_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
            ),
            mk_control_event(
                "nfs1",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
            mk_source_event(
                "nfs2",
                FileMetaRecord::scan_update(
                    b"/".to_vec(),
                    b"".to_vec(),
                    UnixStat {
                        is_dir: true,
                        size: 0,
                        mtime_us: 5,
                        ctime_us: 5,
                        dev: None,
                        ino: None,
                    },
                    b"/".to_vec(),
                    5,
                    false,
                ),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                6,
            ),
            mk_source_event(
                "nfs2",
                mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
            ),
            mk_control_event(
                "nfs2",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                8,
            ),
        ])
        .expect("seed ready root-id state for both groups");

        let snapshot_before = sink.status_snapshot().expect("status before scope wobble");
        let before_groups = snapshot_before
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id.clone(),
                    (
                        group.initial_audit_completed,
                        group.live_nodes,
                        group.materialized_revision,
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            before_groups.get("nfs1").map(|row| row.0),
            Some(true),
            "precondition: nfs1 must be ready before scope wobble: {snapshot_before:?}"
        );
        assert_eq!(
            before_groups.get("nfs2").map(|row| row.0),
            Some(true),
            "precondition: nfs2 must be ready before scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("nfs1").is_some_and(|row| row.1 > 0),
            "precondition: nfs1 must have live nodes before scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("nfs2").is_some_and(|row| row.1 > 0),
            "precondition: nfs2 must have live nodes before scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("nfs1").is_some_and(|row| row.2 > 1),
            "precondition: nfs1 must have advanced materialized revision before scope wobble: {snapshot_before:?}"
        );
        assert!(
            before_groups.get("nfs2").is_some_and(|row| row.2 > 1),
            "precondition: nfs2 must have advanced materialized revision before scope wobble: {snapshot_before:?}"
        );

        let activate_nfs1_only =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode activate nfs1 only");
        sink.on_control_frame(&[activate_nfs1_only])
            .await
            .expect("activate nfs1 only should pass");

        {
            let state = sink.state.read().expect("state lock after scope wobble");
            assert!(
                !state.groups.contains_key("nfs2"),
                "scope wobble should move nfs2 out of active groups before same-instance reactivate: groups={:?}",
                state.groups.keys().collect::<Vec<_>>()
            );
            let retained_nfs2 = state
                .retained_groups
                .get("nfs2")
                .expect("nfs2 must be retained after same-instance scope wobble");
            assert!(
                retained_nfs2.initial_audit_completed,
                "retained nfs2 must stay ready in memory before same-instance reactivate"
            );
            assert!(
                retained_nfs2.tree.node_count() > 0,
                "retained nfs2 must keep materialized nodes in memory before same-instance reactivate"
            );
        }

        let reactivate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 3,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["nfs1"]),
                    bound_scope_with_resources("nfs2", &["nfs2"]),
                ],
            }))
            .expect("encode reactivate both");
        sink.on_control_frame(&[reactivate_both])
            .await
            .expect("reactivate both should pass after same-instance scope wobble");

        let snapshot_after = sink
            .status_snapshot()
            .expect("status after same-instance reactivate");
        let after_groups = snapshot_after
            .groups
            .iter()
            .map(|group| {
                (
                    group.group_id.clone(),
                    (
                        group.initial_audit_completed,
                        group.live_nodes,
                        group.materialized_revision,
                    ),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            after_groups.get("nfs1").map(|row| row.0),
            Some(true),
            "same-instance reactivate must preserve nfs1 ready state instead of regressing to init=false: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("nfs2").map(|row| row.0),
            Some(true),
            "same-instance reactivate must preserve retained nfs2 ready state instead of regressing to init=false: {snapshot_after:?}"
        );
        assert!(
            after_groups.get("nfs1").is_some_and(|row| row.1 > 0),
            "same-instance reactivate must preserve nfs1 live nodes instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert!(
            after_groups.get("nfs2").is_some_and(|row| row.1 > 0),
            "same-instance reactivate must preserve retained nfs2 live nodes instead of regressing to live_nodes=0: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("nfs1").map(|row| row.2),
            before_groups.get("nfs1").map(|row| row.2),
            "same-instance reactivate must preserve nfs1 materialized revision instead of resetting from scratch: {snapshot_after:?}"
        );
        assert_eq!(
            after_groups.get("nfs2").map(|row| row.2),
            before_groups.get("nfs2").map(|row| row.2),
            "same-instance reactivate must preserve retained nfs2 materialized revision instead of resetting from scratch: {snapshot_after:?}"
        );

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query after same-instance reactivate");
        let responses = query_events
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert!(
            payload_contains_path(responses.get("nfs1").expect("nfs1 query response"), b"/ready-a.txt"),
            "same-instance reactivate must preserve nfs1 materialized payload instead of recreating it from scratch"
        );
        assert!(
            payload_contains_path(responses.get("nfs2").expect("nfs2 query response"), b"/ready-b.txt"),
            "same-instance reactivate must preserve retained nfs2 materialized payload instead of recreating it from scratch"
        );
    }

    #[tokio::test]
    async fn sink_activate_can_expand_to_newly_scheduled_group() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate_root_a =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate root-a");
        sink.on_control_frame(&[activate_root_a])
            .await
            .expect("activate root-a should pass");

        let activate_both =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 2,
                bound_scopes: vec![bound_scope("root-a"), bound_scope("root-b")],
            }))
            .expect("encode activate both");
        sink.on_control_frame(&[activate_both])
            .await
            .expect("activate both should pass");

        let state = sink.state.read().expect("state lock");
        assert!(state.groups.contains_key("root-a"));
        assert!(state.groups.contains_key("root-b"));
        drop(state);

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
    }

    #[tokio::test]
    async fn stream_replays_buffered_events_after_grants_catch_up() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        sink.ingest_stream_events(&[
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/kept.txt", "kept.txt", 10, EventKind::Update),
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/delayed.txt", "delayed.txt", 11, EventKind::Update),
            ),
        ])
        .expect("stream ingest should defer only the scheduled-but-unmapped event");

        let before = sink
            .materialized_query(&default_materialized_request())
            .expect("query before grants catch up");
        let before_origins = before
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            before_origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
        let before_responses = before
            .iter()
            .map(|event| {
                (
                    event.metadata().origin_id.0.clone(),
                    decode_tree_payload(event),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let root_a_before = before_responses
            .get("root-a")
            .expect("root-a response should exist before grants catch up");
        assert!(payload_contains_path(root_a_before, b"/kept.txt"));
        let root_b_before = before_responses
            .get("root-b")
            .expect("root-b response should exist before grants catch up");
        assert!(!root_b_before.root.exists);
        assert!(root_b_before.entries.is_empty());

        let grants_changed = host_object_grants_changed_envelope(
            1,
            &[
                granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
                granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
            ],
        );
        sink.on_control_frame(&[grants_changed])
            .await
            .expect("grants change should flush buffered stream events");

        let query_events = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            loop {
                let query_events = sink
                    .materialized_query(&default_materialized_request())
                    .expect("query after grants catch up");
                let responses = query_events
                    .iter()
                    .map(decode_tree_payload)
                    .collect::<Vec<_>>();
                if responses
                    .iter()
                    .any(|response| payload_contains_path(response, b"/delayed.txt"))
                {
                    break query_events;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for buffered stream replay after grants catch up"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        let origins = query_events
            .iter()
            .map(|event| event.metadata().origin_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            origins,
            std::collections::BTreeSet::from(["root-a".to_string(), "root-b".to_string()])
        );
        let responses = query_events
            .iter()
            .map(decode_tree_payload)
            .collect::<Vec<_>>();
        assert!(
            responses
                .iter()
                .any(|response| payload_contains_path(response, b"/kept.txt"))
        );
        assert!(
            responses
                .iter()
                .any(|response| payload_contains_path(response, b"/delayed.txt"))
        );
    }

    #[tokio::test]
    async fn buffered_audit_control_events_restore_initial_audit_after_grants_catch_up() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("root-a", &["node-a::exp-a"]),
                    bound_scope_with_resources("root-b", &["node-b::exp-b"]),
                ],
            }))
            .expect("encode activate");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate should pass");

        sink.ingest_stream_events(&[
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ready.txt", "ready.txt", 2, EventKind::Update),
            ),
            mk_control_event(
                "node-b::exp-b",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .expect("stream ingest should buffer scheduled root-b events");

        let grants_changed = host_object_grants_changed_envelope(
            1,
            &[
                granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
                granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
            ],
        );
        sink.on_control_frame(&[grants_changed])
            .await
            .expect("grants change should flush buffered stream events");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let snapshot = sink.status_snapshot().expect("sink status");
            if snapshot
                .groups
                .iter()
                .find(|group| group.group_id == "root-b")
                .is_some_and(|group| group.initial_audit_completed)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for buffered control events to restore initial audit"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn runtime_scoped_send_ignores_out_of_scope_group_events() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("root-a", "/mnt/nfs1"),
            RootSpec::new("root-b", "/mnt/nfs2"),
        ];
        cfg.host_object_grants = vec![
            granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        ];
        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
            .expect("init sink");

        let activate =
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("root-a")],
            }))
            .expect("encode activate root-a");
        sink.on_control_frame(&[activate])
            .await
            .expect("activate root-a should pass");

        sink.send(&[
            mk_source_event(
                "node-a::exp-a",
                mk_record(b"/kept.txt", "kept.txt", 10, EventKind::Update),
            ),
            mk_source_event(
                "node-b::exp-b",
                mk_record(b"/ignored.txt", "ignored.txt", 11, EventKind::Update),
            ),
        ])
        .await
        .expect("out-of-scope events should be ignored");

        let query_events = sink
            .materialized_query(&default_materialized_request())
            .expect("query state");
        assert_eq!(query_events.len(), 1, "only scheduled group should reply");
        let response = decode_tree_payload(&query_events[0]);
        assert!(payload_contains_path(&response, b"/kept.txt"));
        assert!(!payload_contains_path(&response, b"/ignored.txt"));
    }

    #[tokio::test]
    async fn stream_receive_gate_stays_closed_until_enabled() {
        let sink = build_single_group_sink();
        assert!(
            !sink.should_receive_stream_events(),
            "stream receive gate should stay closed before runtime startup"
        );
        sink.enable_stream_receive();
        assert!(
            !sink.should_receive_stream_events(),
            "stream receive gate must remain closed until events stream route is runtime-activated"
        );
        sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope("nfs1")],
            },
        ))
        .expect("encode events route activation")])
            .await
            .expect("activate events stream route");
        assert!(
            sink.should_receive_stream_events(),
            "stream receive gate should open after both bootstrap enablement and current-generation route activation"
        );
        sink.disable_stream_receive();
        assert!(
            !sink.should_receive_stream_events(),
            "stream receive gate should close again during shutdown"
        );
    }

    #[test]
    fn sink_state_authority_log_records_root_updates() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("root-1", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
            .expect("init sink");
        let before = sink.state.authority_log_len();
        assert!(before >= 1, "bootstrap should append authority record");

        sink.update_logical_roots(
            vec![RootSpec::new("root-2", "/mnt/nfs1")],
            &cfg.host_object_grants,
        )
        .expect("update roots");

        let after = sink.state.authority_log_len();
        assert!(
            after > before,
            "root update should append authority record (before={}, after={})",
            before,
            after
        );
    }

    #[tokio::test]
    async fn watch_overflow_marks_group_unreliable_until_audit_end() {
        let sink = build_single_group_sink();
        sink.send(&[mk_control_event(
            "node-a::exp",
            ControlEvent::WatchOverflow,
            1,
        )])
        .await
        .expect("apply overflow control event");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(!response.reliability.reliable);
        assert_eq!(
            response.reliability.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );

        sink.send(&[
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .await
        .expect("apply audit epoch boundaries");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(response.reliability.reliable);
        assert_eq!(response.reliability.unreliable_reason, None);
    }

    #[tokio::test]
    async fn watch_overflow_reason_has_higher_priority_than_node_level_reasons() {
        let sink = build_single_group_sink();
        sink.send(&[mk_source_event(
            "node-a::exp",
            mk_record(b"/a.txt", "a.txt", 10, EventKind::Update),
        )])
        .await
        .expect("apply scan record");

        sink.send(&[mk_control_event(
            "node-a::exp",
            ControlEvent::WatchOverflow,
            11,
        )])
        .await
        .expect("apply overflow control event");

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(!response.reliability.reliable);
        assert_eq!(
            response.reliability.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );
    }

    #[tokio::test]
    async fn initial_audit_completion_waits_for_materialized_root() {
        let sink = build_single_group_sink();
        sink.send(&[
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                1,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
        ])
        .await
        .expect("apply empty audit epoch");

        let snapshot = sink.status_snapshot().expect("sink status");
        assert_eq!(snapshot.groups.len(), 1);
        assert!(
            !snapshot.groups[0].initial_audit_completed,
            "audit epoch without a materialized root must stay not-ready"
        );

        sink.send(&[mk_source_event(
            "node-a::exp",
            mk_record(b"/ready.txt", "ready.txt", 3, EventKind::Update),
        )])
        .await
        .expect("materialize root path");

        let snapshot = sink
            .status_snapshot()
            .expect("sink status after root materializes");
        assert!(
            snapshot.groups[0].initial_audit_completed,
            "materialized root should unlock initial audit readiness after audit completion"
        );
    }

    #[tokio::test]
    async fn sink_reopen_with_same_state_boundary_preserves_materialized_state_and_initial_audit() {
        let state_boundary = in_memory_state_boundary();
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![RootSpec::new("root-a", "/mnt/nfs1")];
        cfg.host_object_grants = vec![granted_mount_root(
            "node-a::exp",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )];

        let sink = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary.clone(),
            cfg.clone(),
        )
        .expect("init sink");
        sink.send(&[
            mk_source_event(
                "node-a::exp",
                mk_record(b"/ready.txt", "ready.txt", 1, EventKind::Update),
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .await
        .expect("materialize and complete initial audit");
        let snapshot_before = sink.status_snapshot().expect("sink status before reopen");
        assert!(
            snapshot_before
                .groups
                .iter()
                .find(|group| group.group_id == "root-a")
                .is_some_and(|group| group.initial_audit_completed),
            "precondition: root-a should be trusted before reopen"
        );
        let events_before = sink
            .materialized_query(&default_materialized_request())
            .expect("query before reopen");
        let response_before = decode_tree_payload(&events_before[0]);
        assert!(payload_contains_path(&response_before, b"/ready.txt"));
        sink.close().await.expect("close sink before reopen");

        let reopened = SinkFileMeta::with_boundaries_and_state(
            NodeId("node-a".to_string()),
            None,
            state_boundary,
            cfg,
        )
        .expect("reopen sink with same state boundary");
        let snapshot_after = reopened
            .status_snapshot()
            .expect("sink status after reopen");
        assert!(
            snapshot_after
                .groups
                .iter()
                .find(|group| group.group_id == "root-a")
                .is_some_and(|group| group.initial_audit_completed),
            "reopened sink should preserve initial audit completion from the shared state boundary"
        );
        let events_after = reopened
            .materialized_query(&default_materialized_request())
            .expect("query after reopen");
        let response_after = decode_tree_payload(&events_after[0]);
        assert!(
            payload_contains_path(&response_after, b"/ready.txt"),
            "reopened sink should preserve the previously materialized tree payload"
        );
        reopened.close().await.expect("close reopened sink");
    }

    #[tokio::test]
    async fn initial_audit_completion_rejects_tombstone_only_state() {
        let sink = build_single_group_sink();
        sink.send(&[
            mk_source_event(
                "node-a::exp",
                mk_record_with_track(
                    b"/gone.txt",
                    "gone.txt",
                    1,
                    EventKind::Update,
                    crate::SyncTrack::Realtime,
                ),
            ),
            mk_source_event(
                "node-a::exp",
                mk_record_with_track(
                    b"/gone.txt",
                    "gone.txt",
                    2,
                    EventKind::Delete,
                    crate::SyncTrack::Realtime,
                ),
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                4,
            ),
        ])
        .await
        .expect("apply tombstone-only audit state");

        let snapshot = sink.status_snapshot().expect("sink status");
        assert_eq!(snapshot.groups.len(), 1);
        assert!(
            !snapshot.groups[0].initial_audit_completed,
            "tombstone-only state must not satisfy initial audit readiness"
        );

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response");
        let response = decode_tree_payload(&events[0]);
        assert!(!response.root.exists, "tombstone-only root must stay empty");
        assert!(
            response.entries.is_empty(),
            "tombstone-only tree must not expose entries"
        );
    }

    #[tokio::test]
    async fn initial_audit_completion_clears_when_group_regresses_to_structural_root_only() {
        let sink = build_single_group_sink();
        sink.send(&[
            mk_source_event(
                "node-a::exp",
                mk_record(b"/ready.txt", "ready.txt", 1, EventKind::Update),
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochStart {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                2,
            ),
            mk_control_event(
                "node-a::exp",
                ControlEvent::EpochEnd {
                    epoch_id: 0,
                    epoch_type: EpochType::Audit,
                },
                3,
            ),
        ])
        .await
        .expect("materialize and complete initial audit");

        let snapshot_ready = sink.status_snapshot().expect("sink status after ready");
        assert!(
            snapshot_ready.groups[0].initial_audit_completed,
            "precondition: group should be ready after materializing /ready.txt"
        );
        assert!(
            snapshot_ready.groups[0].total_nodes > 0,
            "precondition: ready group should have live materialized nodes"
        );

        sink.send(&[mk_source_event(
            "node-a::exp",
            mk_record(b"/ready.txt", "ready.txt", 4, EventKind::Delete),
        )])
        .await
        .expect("delete only materialized child");

        let snapshot_after_delete = sink.status_snapshot().expect("sink status after delete");
        assert!(
            !snapshot_after_delete.groups[0].initial_audit_completed,
            "deleting the only live child must clear initial audit readiness instead of treating a structural-root-only tree as ready: {snapshot_after_delete:?}"
        );

        let events = sink
            .materialized_query(&default_materialized_request())
            .expect("query response after delete");
        let response = decode_tree_payload(&events[0]);
        assert!(
            !response.root.exists,
            "structural-root-only tree must not keep reporting a live root after the only child is deleted"
        );
    }
