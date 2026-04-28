#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_materialized_status_snapshots_preserves_cached_ready_groups_when_routed_sink_status_reports_explicit_empty_root_transition_snapshot()
 {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let node_a_root = tmp.path().join("node-a-nfs2");
    let node_c_root = tmp.path().join("node-c-nfs4");
    fs::create_dir_all(&node_a_root).expect("create node-a nfs2 dir");
    fs::create_dir_all(&node_c_root).expect("create node-c nfs4 dir");
    let grants = vec![
        GrantedMountRoot {
            object_ref: "node-a::nfs2".to_string(),
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_a_root.clone(),
            fs_source: "nfs2".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
        GrantedMountRoot {
            object_ref: "node-c::nfs4".to_string(),
            host_ref: "node-c".to_string(),
            host_ip: "10.0.0.3".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: std::collections::BTreeMap::new(),
            mount_point: node_c_root.clone(),
            fs_source: "nfs4".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: Vec::new(),
            active: true,
        },
    ];
    let source = source_facade_with_roots(
        vec![
            RootSpec::new("nfs2", &node_a_root),
            RootSpec::new("nfs4", &node_c_root),
        ],
        &grants,
    );
    let sink = sink_facade_with_group(&grants);
    let sink_status_payload = rmp_serde::to_vec_named(&SinkStatusSnapshot {
        groups: vec![
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs2".to_string(),
                primary_object_ref: "node-a::nfs2".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
            crate::sink::SinkGroupStatusSnapshot {
                group_id: "nfs4".to_string(),
                primary_object_ref: "node-c::nfs4".to_string(),
                total_nodes: 0,
                live_nodes: 0,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 0,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
                materialized_revision: 1,
                estimated_heap_bytes: 0,
            },
        ],
        ..SinkStatusSnapshot::default()
    })
    .expect("encode retire-shaped explicit-empty sink status snapshot");
    let boundary = Arc::new(SourceStatusTimeoutSinkStatusOkBoundary::new(
        sink_status_payload,
    ));
    let cached_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: BTreeMap::from([
            ("node-a".to_string(), vec!["nfs2".to_string()]),
            ("node-c".to_string(), vec!["nfs4".to_string()]),
        ]),
        groups: vec![
            sink_group_status("nfs2", true),
            sink_group_status("nfs4", true),
        ],
        ..SinkStatusSnapshot::default()
    };
    let state = ApiState {
        backend: QueryBackend::Route {
            sink: sink.clone(),
            boundary,
            origin_id: NodeId("node-d".to_string()),
            source: source.clone(),
        },
        policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
        pit_store: Arc::new(Mutex::new(QueryPitStore::default())),
        force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
        force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
        force_find_route_rr: Arc::new(Mutex::new(BTreeMap::new())),
        readiness_source: Some(source),
        readiness_sink: Some(sink),
        materialized_source_status_cache: Arc::new(Mutex::new(None)),
        materialized_sink_status_cache: Arc::new(Mutex::new(Some(CachedSinkStatusSnapshot {
            snapshot: cached_snapshot,
        }))),
        materialized_stats_cache: Arc::new(Mutex::new(None)),
        materialized_tree_cache: Arc::new(Mutex::new(None)),
        tree_query_serial: Arc::new(tokio::sync::Mutex::new(())),
    };

    let (source_status, sink_status) = load_materialized_status_snapshots(&state)
        .await
        .expect("materialized status snapshots");

    let ready_groups = sink_status
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.as_str(),
                group.is_ready(),
                group.total_nodes,
                group.live_nodes,
                group.shadow_time_us,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ready_groups,
        vec![("nfs2", true, 1, 1, 1), ("nfs4", true, 1, 1, 1)],
        "routed explicit-empty sink-status during retire-shaped root transition must not clear previously trusted cached ready groups before a better authoritative sink snapshot arrives"
    );
    assert!(
        materialized_query_readiness_error(&source_status, &sink_status).is_none(),
        "retire-shaped routed explicit-empty sink-status must not reintroduce trusted-materialized NOT_READY for cached-ready groups nfs2/nfs4"
    );
}
