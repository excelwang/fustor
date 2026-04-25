use super::*;
use crate::EpochType;
use crate::query::{QueryScope, TreeQueryOptions};
use crate::shared_types::query::UnreliableReason;
use bytes::Bytes;
use capanix_app_sdk::runtime::EventMetadata;
use capanix_host_fs_types::UnixStat;
use capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext;
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
    RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick, encode_runtime_exec_control,
    encode_runtime_host_grant_change, encode_runtime_unit_tick,
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

fn materialized_tree_request(
    path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
) -> InternalQueryRequest {
    InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: path.to_vec(),
            recursive,
            max_depth,
            selected_group: None,
        },
        Some(TreeQueryOptions::default()),
    )
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

#[test]
fn sink_status_snapshot_concern_projection_reports_coverage_gap_from_stream_evidence() {
    let snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        ..SinkStatusSnapshot::default()
    };

    let projection = snapshot.concern_projection();
    assert_eq!(
        projection.concern,
        Some(SinkStatusConcern::CoverageGap),
        "sink snapshot owner should project the canonical coverage-gap concern directly from readiness summary and stream evidence"
    );
    assert_eq!(
        projection.summary.missing_scheduled_groups,
        std::collections::BTreeSet::from(["nfs3".to_string()]),
    );
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
async fn owner_scoped_roots_control_stream_stays_gated_until_route_activation() {
    let sink = build_single_group_sink();
    let boundary = Arc::new(RouteCountingTimeoutBoundary::default());

    sink.start_runtime_endpoints(boundary.clone(), NodeId("node-a".to_string()))
        .expect("start runtime endpoints");

    let roots_control_route =
        crate::runtime::routes::sink_roots_control_stream_route_for("node-a").0;
    let pre_activation_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < pre_activation_deadline {
        let recv = boundary.recv_count(&roots_control_route);
        assert_eq!(
            recv,
            0,
            "sink owner-scoped roots-control stream must remain gated before runtime route activation; recv_counts={:?}",
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
    .expect("encode scoped sink roots-control activate")])
        .await
        .expect("activate owner-scoped sink roots-control route");

    let activated_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let recv = boundary.recv_count(&roots_control_route);
        if recv > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < activated_deadline,
            "sink owner-scoped roots-control stream must begin receiving after route activation; recv_counts={:?}",
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

#[tokio::test]
async fn sink_runtime_exec_failover_reactivate_replaces_scoped_holder_without_stale_activated_scope()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = vec![
        granted_mount_root("node-b::nfs2-old", "node-b", "10.0.0.12", "/mnt/nfs2", true),
        granted_mount_root("node-c::nfs2-new", "node-c", "10.0.0.13", "/mnt/nfs2", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-c".to_string()), None, cfg).expect("init sink");

    let initial_holder =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-b::nfs2-old"])],
        }))
        .expect("encode initial holder activate");
    sink.on_control_frame(&[initial_holder])
        .await
        .expect("initial holder activate should pass");

    let successor_holder =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 2,
            bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-c::nfs2-new"])],
        }))
        .expect("encode successor holder activate");
    sink.on_control_frame(&[successor_holder])
        .await
        .expect("successor holder activate should pass");

    let state = sink
        .unit_control
        .unit_state("runtime.exec.sink")
        .expect("unit state after successor activate")
        .expect("runtime.exec.sink unit state should exist");
    assert!(
        state.0,
        "runtime.exec.sink should remain active after successor activate"
    );
    assert_eq!(
        state.1,
        vec![RuntimeBoundScope {
            scope_id: "nfs2".to_string(),
            resource_ids: vec!["node-c::nfs2-new".to_string()],
        }],
        "successor scoped holder activate must replace the stale activated scope instead of merging the dead holder resource into runtime.exec.sink state",
    );
}

#[tokio::test]
async fn sink_runtime_exec_partial_route_activations_union_disjoint_groups() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs4", "/mnt/nfs4"),
        RootSpec::new("nfs5", "/mnt/nfs5"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs4", "node-a", "10.0.0.11", "/mnt/nfs4", true),
        granted_mount_root("node-a::nfs5", "node-a", "10.0.0.11", "/mnt/nfs5", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

    let query_activation =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope("nfs1")],
        }))
        .expect("encode query activation");
    let stream_activation =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 2,
            bound_scopes: vec![bound_scope("nfs4"), bound_scope("nfs5")],
        }))
        .expect("encode stream activation");

    sink.on_control_frame(&[query_activation])
        .await
        .expect("query activation should pass");
    sink.on_control_frame(&[stream_activation])
        .await
        .expect("stream activation should pass");

    let state = sink
        .unit_control
        .unit_state("runtime.exec.sink")
        .expect("unit state after partial activations")
        .expect("runtime.exec.sink unit state should exist");
    let groups = state
        .1
        .into_iter()
        .map(|scope| scope.scope_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        groups,
        BTreeSet::from(["nfs1".to_string(), "nfs4".to_string(), "nfs5".to_string()]),
        "disjoint partial route activations are shards of the same sink schedule and must not erase each other",
    );
}

#[test]
fn stable_host_ref_treats_cluster_scoped_node_id_as_local_member() {
    let grants = vec![granted_mount_root(
        "node-b::nfs2",
        "node-b",
        "10.0.0.12",
        "/mnt/nfs2",
        true,
    )];
    let stable = crate::workers::sink::stable_host_ref_for_node_id(
        &NodeId("cluster-node-b-29775277610492238759985153".to_string()),
        &grants,
    );
    assert_eq!(
        stable, "node-b",
        "cluster-scoped runtime node ids must collapse back to the bare host_ref for sink-side local ownership"
    );
}

#[test]
fn force_find_aggregation_delete_wins_on_same_mtime() {
    let update = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Update));
    let delete = mk_source_event("src", mk_record(b"/a.txt", "a.txt", 10, EventKind::Delete));
    let query =
        query_response_from_source_events(&[update, delete], b"/").expect("decode source events");
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
        .filter(|group| group.is_ready() && group.live_nodes > 0)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "scheduled root-id stream events must materialize ready state even when host grants are temporarily absent: {snapshot:?}"
    );
}

#[tokio::test]
async fn stream_activate_without_runtime_endpoints_does_not_wait_for_stream_recv_observation() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![granted_mount_root(
        "node-a::nfs1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

    sink.enable_stream_receive();
    let activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope("nfs1")],
        }))
        .expect("encode stream activate");

    tokio::time::timeout(Duration::from_millis(500), async {
        sink.on_control_frame(&[activate]).await
    })
    .await
    .expect("repo-local sink activate without runtime endpoints must not wait for stream-recv observation")
    .expect("activate stream route");
}

#[tokio::test]
async fn events_stream_materializes_split_primary_mixed_cluster_publications_after_bare_scope_activate()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.13", "/mnt/nfs3", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-b".to_string()), None, cfg).expect("init sink");

    sink.enable_stream_receive();
    let activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope("nfs1"),
                bound_scope("nfs2"),
                bound_scope("nfs3"),
            ],
        }))
        .expect("encode split-primary stream activate");
    let sink_for_activate = sink.clone();
    tokio::time::timeout(Duration::from_millis(500), async move {
        sink_for_activate.on_control_frame(&[activate]).await
    })
    .await
    .expect("non-primary bare-scope split-primary stream activate must not hang")
    .expect("activate split-primary stream route");
    eprintln!("test-marker: non-primary split-primary after activate");

    assert!(
        sink.should_receive_stream_events(),
        "split-primary bare-scope stream activate should open the stream receive gate once runtime startup is enabled"
    );
    assert_eq!(
        sink.scheduled_stream_object_refs()
            .expect("scheduled stream object refs after split-primary activate"),
        Some(std::collections::BTreeSet::from([
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string(),
            "node-b::nfs3".to_string(),
        ])),
        "bare-scope mixed-cluster sink activate must expand to the concrete split-primary object refs before stream ingress"
    );

    sink.ingest_stream_events(&[
        mk_source_event(
            "node-a::nfs1",
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
            "node-a::nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_source_event(
            "node-a::nfs1",
            mk_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_control_event(
            "node-a::nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_source_event(
            "node-a::nfs2",
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
            "node-a::nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_source_event(
            "node-a::nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_control_event(
            "node-a::nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
        mk_source_event(
            "node-b::nfs3",
            FileMetaRecord::scan_update(
                b"/".to_vec(),
                b"".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 9,
                    ctime_us: 9,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                9,
                false,
            ),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            10,
        ),
        mk_source_event(
            "node-b::nfs3",
            mk_record(b"/ready-c.txt", "ready-c.txt", 11, EventKind::Update),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            12,
        ),
    ])
    .expect("split-primary stream events should apply after bare-scope activate");

    let snapshot = sink
        .status_snapshot()
        .expect("sink status after split-primary stream apply");
    let ready_groups = snapshot
        .groups
        .iter()
        .filter(|group| group.is_ready() && group.live_nodes > 0)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2", "nfs3"]),
        "split-primary mixed-cluster stream publications must materialize all scheduled groups after bare-scope activate: {snapshot:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_stream_materializes_split_primary_mixed_cluster_publications_on_non_primary_request_node_after_bare_scope_activate()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.13", "/mnt/nfs3", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-d".to_string()), None, cfg).expect("init sink");

    sink.enable_stream_receive();
    sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
        RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope("nfs1"),
                bound_scope("nfs2"),
                bound_scope("nfs3"),
            ],
        },
    ))
    .expect("encode split-primary stream activate")])
        .await
        .expect("activate split-primary stream route");

    assert!(
        sink.should_receive_stream_events(),
        "non-primary request node must still open the stream receive gate after bare-scope activate"
    );
    assert_eq!(
        sink.scheduled_stream_object_refs()
            .expect("scheduled stream object refs after split-primary activate"),
        Some(std::collections::BTreeSet::from([
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string(),
            "node-b::nfs3".to_string(),
        ])),
        "bare-scope mixed-cluster sink activate on a non-primary request node must still expand to the concrete split-primary object refs before stream ingress"
    );

    sink.ingest_stream_events(&[
        mk_source_event(
            "node-a::nfs1",
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
            "node-a::nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_source_event(
            "node-a::nfs1",
            mk_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_control_event(
            "node-a::nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_source_event(
            "node-a::nfs2",
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
            "node-a::nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_source_event(
            "node-a::nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_control_event(
            "node-a::nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
        mk_source_event(
            "node-b::nfs3",
            FileMetaRecord::scan_update(
                b"/".to_vec(),
                b"".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 9,
                    ctime_us: 9,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                9,
                false,
            ),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            10,
        ),
        mk_source_event(
            "node-b::nfs3",
            mk_record(b"/ready-c.txt", "ready-c.txt", 11, EventKind::Update),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            12,
        ),
    ])
    .expect("split-primary stream events should apply on a non-primary request node");
    eprintln!("test-marker: non-primary split-primary after ingest");

    let snapshot = sink
        .status_snapshot()
        .expect("sink status after split-primary stream apply on non-primary request node");
    eprintln!("test-marker: non-primary split-primary after status snapshot");
    let ready_groups = snapshot
        .groups
        .iter()
        .filter(|group| group.is_ready() && group.live_nodes > 0)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2", "nfs3"]),
        "split-primary mixed-cluster stream publications must materialize all scheduled groups even on a non-primary request node: {snapshot:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_logical_roots_preserves_pending_materialization_scheduled_group_during_split_primary_refresh_when_siblings_are_ready()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    let host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.13", "/mnt/nfs3", true),
    ];
    cfg.host_object_grants = host_object_grants.clone();
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-d".to_string()), None, cfg).expect("init sink");

    sink.enable_stream_receive();
    sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
        RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope("nfs1"),
                bound_scope("nfs2"),
                bound_scope("nfs3"),
            ],
        },
    ))
    .expect("encode split-primary stream activate")])
        .await
        .expect("activate split-primary stream route");

    sink.ingest_stream_events(&[
        mk_source_event(
            "node-a::nfs1",
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
            "node-a::nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_source_event(
            "node-a::nfs1",
            mk_record(b"/pending-a.txt", "pending-a.txt", 3, EventKind::Update),
        ),
        mk_source_event(
            "node-a::nfs2",
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
            "node-a::nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_source_event(
            "node-a::nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_control_event(
            "node-a::nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
        mk_source_event(
            "node-b::nfs3",
            FileMetaRecord::scan_update(
                b"/".to_vec(),
                b"".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 9,
                    ctime_us: 9,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                9,
                false,
            ),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            10,
        ),
        mk_source_event(
            "node-b::nfs3",
            mk_record(b"/ready-c.txt", "ready-c.txt", 11, EventKind::Update),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            12,
        ),
    ])
    .expect("split-primary stream events should apply with nfs1 left pending-materialization");

    let snapshot_before = sink
        .status_snapshot()
        .expect("status before logical-roots refresh");
    let nfs1_before = snapshot_before
        .groups
        .iter()
        .find(|group| group.group_id == "nfs1")
        .expect("nfs1 row before logical-roots refresh");
    assert_eq!(
        nfs1_before.readiness,
        GroupReadinessState::PendingMaterialization,
        "precondition: nfs1 must still be pending-materialization before logical-roots refresh: {snapshot_before:?}"
    );

    sink.update_logical_roots(
        vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
            RootSpec::new("nfs3", "/mnt/nfs3"),
        ],
        &host_object_grants,
    )
    .expect("logical-roots refresh should preserve pending-materialization split-primary group");

    let snapshot_after = sink
        .status_snapshot()
        .expect("status after logical-roots refresh");
    let nfs1_after = snapshot_after
        .groups
        .iter()
        .find(|group| group.group_id == "nfs1")
        .expect("nfs1 row after logical-roots refresh");
    assert_eq!(
        nfs1_after.readiness,
        GroupReadinessState::PendingMaterialization,
        "logical-roots refresh must preserve pending-materialization readiness for nfs1 instead of dropping it or rewriting it as another state: {snapshot_after:?}"
    );
    let scheduled_after = snapshot_after
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        scheduled_after.contains("nfs1"),
        "logical-roots refresh must preserve pending-materialization scheduled groups instead of dropping nfs1 from scheduled_groups_by_node: {snapshot_after:?}"
    );

    sink.ingest_stream_events(&[mk_control_event(
        "node-a::nfs1",
        ControlEvent::EpochEnd {
            epoch_id: 0,
            epoch_type: EpochType::Audit,
        },
        13,
    )])
    .expect("nfs1 audit end after logical-roots refresh should apply");

    let snapshot_ready = sink
        .status_snapshot()
        .expect("status after nfs1 audit end post-refresh");
    let nfs1_ready = snapshot_ready
        .groups
        .iter()
        .find(|group| group.group_id == "nfs1")
        .expect("nfs1 row after audit end post-refresh");
    assert_eq!(
        nfs1_ready.readiness,
        GroupReadinessState::Ready,
        "nfs1 should become ready after its pending audit completes without requiring a second reactivate: {snapshot_ready:?}"
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
fn group_primary_prefers_concrete_source_member_over_logical_group_placeholder() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs3", "/mnt/nfs3")];
    cfg.host_object_grants = vec![
        granted_mount_root("nfs3", "node-b", "10.0.0.12", "/mnt/nfs3", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.12", "/mnt/nfs3", true),
        granted_mount_root("node-d::nfs3", "node-d", "10.0.0.14", "/mnt/nfs3", true),
    ];

    let state = SinkState::new(&cfg);
    let group = state.groups.get("nfs3").expect("group must exist");
    assert_eq!(
        group.primary_object_ref, "node-b::nfs3",
        "the logical group id may only be a placeholder primary when no concrete source object is available"
    );
}

#[test]
fn restored_logical_root_keeps_ready_materialization_after_temporary_omission() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    let host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.11", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.12", "/mnt/nfs3", true),
    ];
    cfg.host_object_grants = host_object_grants.clone();
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-b".to_string()), None, cfg).expect("init sink");

    sink.ingest_stream_events(&[
        mk_source_event(
            "node-b::nfs3",
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
            "node-b::nfs3",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_source_event(
            "node-b::nfs3",
            mk_record(b"/ready-c.txt", "ready-c.txt", 3, EventKind::Update),
        ),
        mk_control_event(
            "node-b::nfs3",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
    ])
    .expect("nfs3 should materialize before temporary omission");
    let before = sink.status_snapshot().expect("status before omission");
    assert_eq!(
        before
            .groups
            .iter()
            .find(|group| group.group_id == "nfs3")
            .expect("nfs3 before omission")
            .readiness,
        GroupReadinessState::Ready,
        "precondition: nfs3 should be ready before roots contraction: {before:?}"
    );

    sink.update_logical_roots(
        vec![RootSpec::new("nfs1", "/mnt/nfs1")],
        &host_object_grants,
    )
    .expect("contract roots to nfs1");
    sink.update_logical_roots(
        vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
            RootSpec::new("nfs3", "/mnt/nfs3"),
        ],
        &host_object_grants,
    )
    .expect("restore roots to nfs1/nfs2/nfs3");

    let after = sink.status_snapshot().expect("status after restore");
    let nfs3 = after
        .groups
        .iter()
        .find(|group| group.group_id == "nfs3")
        .expect("nfs3 after restore");
    assert_eq!(
        nfs3.readiness,
        GroupReadinessState::Ready,
        "restoring a temporarily omitted ready root must preserve materialized evidence: {after:?}"
    );
    assert!(
        nfs3.total_nodes > 0 && nfs3.live_nodes > 0,
        "restored nfs3 should keep its materialized tree counters: {after:?}"
    );
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

#[test]
fn runtime_managed_sink_without_active_scope_preserves_groups_across_logical_root_update() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("root-a", "/mnt/nfs1"),
        RootSpec::new("root-b", "/mnt/nfs2"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
    ];

    let sink = SinkFileMeta::with_boundaries(
        NodeId("node-a".to_string()),
        Some(Arc::new(NoopBoundary)),
        cfg.clone(),
    )
    .expect("init runtime-managed sink");

    assert_eq!(
        sink.scheduled_group_ids_snapshot()
            .expect("scheduled group ids before any runtime activation"),
        None,
        "runtime-managed sink must treat missing runtime.exec.sink route state as no scope restriction, not an explicit empty schedule"
    );

    let snapshot_before = sink
        .status_snapshot()
        .expect("status before logical-root refresh");
    let before_groups = snapshot_before
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        before_groups,
        std::collections::BTreeSet::from(["root-a", "root-b"]),
        "runtime-managed sink must retain configured groups before any runtime.exec.sink activate instead of collapsing to zero groups: {snapshot_before:?}"
    );
    assert!(
        snapshot_before.scheduled_groups_by_node.is_empty(),
        "runtime-managed sink without an active runtime.exec.sink route must not fabricate scheduled_groups_by_node entries before scope truth arrives: {snapshot_before:?}"
    );

    sink.update_logical_roots(cfg.roots.clone(), &cfg.host_object_grants)
        .expect("logical-roots refresh before any runtime.exec.sink activate");

    let snapshot_after = sink
        .status_snapshot()
        .expect("status after logical-root refresh");
    let after_groups = snapshot_after
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        after_groups,
        std::collections::BTreeSet::from(["root-a", "root-b"]),
        "logical-roots refresh must preserve configured groups before runtime.exec.sink route truth arrives instead of collapsing sink.groups to zero: {snapshot_after:?}"
    );
    assert!(
        snapshot_after.scheduled_groups_by_node.is_empty(),
        "logical-roots refresh before any runtime.exec.sink activate must still leave scheduled_groups_by_node empty: {snapshot_after:?}"
    );
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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

    sink.send_with_failure(&[
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

    let snapshot_before = sink
        .status_snapshot()
        .expect("sink status before deactivate");
    assert!(snapshot_before.groups.iter().all(|group| group.is_ready()));
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

    {
        let state = sink.state.read().expect("state lock after deactivate");
        assert!(
            state.groups.is_empty(),
            "empty runtime scope after deactivate must not keep ready groups active: active_groups={:?}",
            state.groups.keys().collect::<Vec<_>>()
        );
        let retained_groups = state.retained_groups.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            retained_groups,
            vec!["root-a".to_string(), "root-b".to_string()],
            "empty runtime scope after deactivate must retain prior ready groups instead of leaving them active or dropping them: retained_groups={retained_groups:?}"
        );
    }

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

    let snapshot_after = sink
        .status_snapshot()
        .expect("sink status after reactivate");
    assert!(
        snapshot_after.groups.iter().all(|group| group.is_ready()),
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
async fn deactivate_empty_runtime_scope_drops_zero_state_groups_instead_of_exposing_them() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("root-a", "/mnt/nfs1"),
        RootSpec::new("root-b", "/mnt/nfs2"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::exp-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-b::exp-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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

    let snapshot_before = sink
        .status_snapshot()
        .expect("sink status before zero-state deactivate");
    assert_eq!(
        snapshot_before.groups.len(),
        2,
        "precondition: both zero-state groups must be visible before empty runtime scope: {snapshot_before:?}"
    );
    assert!(
        snapshot_before
            .groups
            .iter()
            .all(|group| !group.is_ready() && group.live_nodes == 0),
        "precondition: both groups must still be zero-state before empty runtime scope: {snapshot_before:?}"
    );

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

    {
        let state = sink
            .state
            .read()
            .expect("state lock after zero-state deactivate");
        assert!(
            state.groups.is_empty(),
            "empty runtime scope must not keep zero-state groups active: active_groups={:?}",
            state.groups.keys().collect::<Vec<_>>()
        );
        assert!(
            state.retained_groups.is_empty(),
            "empty runtime scope must not retain zero-state groups with no materialized truth: retained_groups={:?}",
            state.retained_groups.keys().collect::<Vec<_>>()
        );
    }

    let snapshot_after = sink
        .status_snapshot()
        .expect("sink status after zero-state deactivate");
    assert!(
        snapshot_after.groups.is_empty(),
        "zero-state groups must disappear after empty runtime scope instead of remaining exposed: {snapshot_after:?}"
    );
    assert!(
        snapshot_after.scheduled_groups_by_node.is_empty(),
        "zero-state deactivate should also clear scheduled groups instead of leaving stale scope evidence: {snapshot_after:?}"
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
        root_b_before.is_ready(),
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
        root_b_after.is_ready(),
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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

    let snapshot_before = sink
        .status_snapshot()
        .expect("status before alternating wobble");
    let before_groups = snapshot_before
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.clone(),
                (
                    group.is_ready(),
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

    let snapshot_after = sink
        .status_snapshot()
        .expect("status after alternating wobble");
    let after_groups = snapshot_after
        .groups
        .iter()
        .map(|group| {
            (
                group.group_id.clone(),
                (
                    group.is_ready(),
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
        root_b_before.is_ready(),
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

    sink.update_logical_roots(
        vec![RootSpec::new("root-a", "/mnt/nfs1")],
        &host_object_grants,
    )
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
            matches!(
                retained_root_b.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
        root_b_after.is_ready(),
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
        root_b_before.is_ready(),
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
            matches!(
                retained_root_b.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
            matches!(
                retained_root_b.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
        root_b_after.is_ready(),
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
            FileMetaRecord::scan_update(
                b"/nested".to_vec(),
                b"nested".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 3,
                    ctime_us: 3,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                3,
                false,
            ),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 4, EventKind::Update),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/nested/peer.txt", "peer.txt", 5, EventKind::Update),
        ),
        mk_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
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
        nfs2_before.is_ready(),
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
            matches!(
                retained_nfs2.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
            matches!(
                retained_nfs2.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
        nfs2_after.is_ready(),
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
            FileMetaRecord::scan_update(
                b"/nested".to_vec(),
                b"nested".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 3,
                    ctime_us: 3,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                3,
                false,
            ),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 4, EventKind::Update),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/nested/peer.txt", "peer.txt", 5, EventKind::Update),
        ),
        mk_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
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
        nfs2_before.is_ready(),
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
    let nested_before = sink
        .materialized_query(&materialized_tree_request(b"/nested", true, Some(1)))
        .expect("nested query before scope wobble");
    let nested_before_responses = nested_before
        .iter()
        .map(|event| {
            (
                event.metadata().origin_id.0.clone(),
                decode_tree_payload(event),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let nfs2_nested_before = nested_before_responses
        .get("nfs2")
        .expect("nfs2 nested response before scope wobble");
    assert!(
        nfs2_nested_before.root.exists && nfs2_nested_before.root.path == b"/nested",
        "precondition: nfs2 nested root must exist before scope wobble: {nfs2_nested_before:?}"
    );
    assert!(
        payload_contains_path(nfs2_nested_before, b"/nested/peer.txt"),
        "precondition: nfs2 nested query must contain peer.txt before scope wobble"
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
        let state = sink
            .state
            .read()
            .expect("state lock after control-only wobble");
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
            matches!(
                retained_nfs2.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
        nfs2_after.is_ready(),
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

    let nested_query_events = sink
        .materialized_query(&materialized_tree_request(b"/nested", true, Some(1)))
        .expect("nested query after logical-roots sync restores runtime scope");
    let nested_responses = nested_query_events
        .iter()
        .map(|event| {
            (
                event.metadata().origin_id.0.clone(),
                decode_tree_payload(event),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let nfs2_nested_response = nested_responses
        .get("nfs2")
        .expect("nfs2 nested response after logical-roots sync restores runtime scope");
    assert!(
        nfs2_nested_response.root.exists && nfs2_nested_response.root.path == b"/nested",
        "later logical-roots sync must restore retained nfs2 nested root instead of collapsing it to an empty subtree: {nfs2_nested_response:?}"
    );
    assert!(
        payload_contains_path(nfs2_nested_response, b"/nested/peer.txt"),
        "later logical-roots sync must restore retained nfs2 nested max-depth payload instead of dropping peer.txt after scope wobble"
    );

    sink.close().await.expect("close sink");
}

#[tokio::test]
async fn update_logical_roots_does_not_widen_runtime_scope_back_to_all_roots_when_current_schedule_is_subset()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    let host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.11", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.12", "/mnt/nfs3", true),
    ];
    cfg.host_object_grants = host_object_grants.clone();
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-b".to_string()), None, cfg).expect("init sink");

    let activate_nfs3_only =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs3", &["node-b::nfs3"])],
        }))
        .expect("encode activate nfs3 only");
    sink.on_control_frame(&[activate_nfs3_only])
        .await
        .expect("activate nfs3 only should pass");

    let snapshot_before = sink
        .status_snapshot()
        .expect("status before logical-roots sync");
    let before_groups = snapshot_before
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        before_groups,
        std::collections::BTreeSet::from(["nfs3"]),
        "precondition: subset runtime schedule should only expose nfs3 before logical-roots sync: {snapshot_before:?}"
    );
    assert_eq!(
        snapshot_before
            .scheduled_groups_by_node
            .get("node-b")
            .cloned()
            .unwrap_or_default(),
        vec!["nfs3".to_string()],
        "precondition: scheduled groups should only contain nfs3 before logical-roots sync: {snapshot_before:?}"
    );

    sink.update_logical_roots(
        vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
            RootSpec::new("nfs3", "/mnt/nfs3"),
        ],
        &host_object_grants,
    )
    .expect("logical-roots sync should preserve current runtime schedule");

    let snapshot_after = sink
        .status_snapshot()
        .expect("status after logical-roots sync preserves subset schedule");
    let after_groups = snapshot_after
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        after_groups,
        std::collections::BTreeSet::from(["nfs3"]),
        "logical-roots sync must not widen subset runtime schedule back to all roots with zero-state groups: {snapshot_after:?}"
    );
    assert_eq!(
        snapshot_after
            .scheduled_groups_by_node
            .get("node-b")
            .cloned()
            .unwrap_or_default(),
        vec!["nfs3".to_string()],
        "logical-roots sync must preserve the current scheduled subset instead of widening it: {snapshot_after:?}"
    );

    sink.close().await.expect("close sink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_logical_roots_drops_unrelated_pending_materialization_group_from_local_sink_schedule_after_force_find_stress_scope_contraction()
 {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
        RootSpec::new("nfs3", "/mnt/nfs3"),
    ];
    let host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", "/mnt/nfs2", true),
        granted_mount_root("node-b::nfs3", "node-b", "10.0.0.13", "/mnt/nfs3", true),
    ];
    cfg.host_object_grants = host_object_grants.clone();
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-d".to_string()), None, cfg).expect("init sink");

    let activate_force_find_subset =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 2,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode force-find subset activate");
    sink.on_control_frame(&[activate_force_find_subset])
        .await
        .expect("force-find subset activate should pass");

    let scheduled_before = sink
        .scheduled_group_ids_snapshot()
        .expect("scheduled group ids before retained pending injection")
        .expect("runtime schedule should exist before retained pending injection");
    assert_eq!(
        scheduled_before,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: force-find subset schedule must start contracted to nfs1/nfs2"
    );

    {
        let mut state = sink
            .state
            .write()
            .expect("state lock before retained pending injection");
        state.groups.remove("nfs3");
        let mut retained_nfs3 =
            GroupSinkState::new("node-b::nfs3".to_string(), TombstonePolicy::default());
        retained_nfs3.materialized_revision = 2;
        retained_nfs3.tree.insert(
            b"/pending-c.txt".to_vec(),
            tree::FileMetaNode {
                size: 1,
                modified_time_us: 11,
                created_time_us: 11,
                is_dir: false,
                source: crate::SyncTrack::Scan,
                monitoring_attested: true,
                last_confirmed_at: None,
                suspect_until: None,
                blind_spot: false,
                is_tombstoned: false,
                tombstone_expires_at: None,
                last_seen_epoch: 0,
                subtree_last_write_significant_change_at: None,
            },
        );
        state
            .retained_groups
            .insert("nfs3".to_string(), retained_nfs3);
    }

    {
        let state = sink
            .state
            .read()
            .expect("state lock after force-find subset contraction");
        assert!(
            !state.groups.contains_key("nfs3"),
            "force-find subset contraction should move nfs3 out of active groups: groups={:?}",
            state.groups.keys().collect::<Vec<_>>()
        );
        let retained_nfs3 = state
            .retained_groups
            .get("nfs3")
            .expect("nfs3 must be retained after force-find subset contraction");
        assert_eq!(
            retained_nfs3.group_readiness_state(),
            GroupReadinessState::PendingMaterialization,
            "retained nfs3 must stay pending-materialization after force-find subset contraction"
        );
    }

    sink.update_logical_roots(
        vec![
            RootSpec::new("nfs1", "/mnt/nfs1"),
            RootSpec::new("nfs2", "/mnt/nfs2"),
            RootSpec::new("nfs3", "/mnt/nfs3"),
        ],
        &host_object_grants,
    )
    .expect("logical-roots sync should preserve the contracted force-find subset");

    let scheduled_after = sink
        .scheduled_group_ids_snapshot()
        .expect("scheduled group ids after logical-roots sync")
        .expect("runtime schedule should remain available after logical-roots sync");
    assert_eq!(
        scheduled_after,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "logical-roots sync must not re-add unrelated pending-materialization nfs3 after force-find subset contraction"
    );

    let snapshot_after = sink
        .status_snapshot()
        .expect("status after logical-roots sync preserves force-find subset");
    let after_groups = snapshot_after
        .groups
        .iter()
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        after_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "logical-roots sync must not export unrelated pending-materialization nfs3 after force-find subset contraction: {snapshot_after:?}"
    );
    let scheduled_after_snapshot = snapshot_after
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        scheduled_after_snapshot,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "logical-roots sync must not republish nfs3 in scheduled_groups_by_node after force-find subset contraction: {snapshot_after:?}"
    );

    sink.close().await.expect("close sink");
}

#[tokio::test]
async fn retained_root_id_ready_state_survives_same_instance_scope_wobble_and_later_reactivate() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
    ];
    cfg.host_object_grants = Vec::new();
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
            FileMetaRecord::scan_update(
                b"/nested".to_vec(),
                b"nested".to_vec(),
                UnixStat {
                    is_dir: true,
                    size: 0,
                    mtime_us: 7,
                    ctime_us: 7,
                    dev: None,
                    ino: None,
                },
                b"/".to_vec(),
                7,
                false,
            ),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 8, EventKind::Update),
        ),
        mk_source_event(
            "nfs2",
            mk_record(b"/nested/peer.txt", "peer.txt", 9, EventKind::Update),
        ),
        mk_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            10,
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
                    group.is_ready(),
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
    let nested_before = sink
        .materialized_query(&materialized_tree_request(b"/nested", true, Some(1)))
        .expect("nested query before same-instance scope wobble");
    let nested_before_responses = nested_before
        .iter()
        .map(|event| {
            (
                event.metadata().origin_id.0.clone(),
                decode_tree_payload(event),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let nfs2_nested_before = nested_before_responses
        .get("nfs2")
        .expect("nfs2 nested response before same-instance scope wobble");
    assert!(
        nfs2_nested_before.root.exists && nfs2_nested_before.root.path == b"/nested",
        "precondition: nfs2 nested root must exist before same-instance scope wobble: {nfs2_nested_before:?}"
    );
    assert!(
        payload_contains_path(nfs2_nested_before, b"/nested/peer.txt"),
        "precondition: nfs2 nested query must contain peer.txt before same-instance scope wobble"
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
            matches!(
                retained_nfs2.group_readiness_state(),
                GroupReadinessState::Ready
            ),
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
                    group.is_ready(),
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
        payload_contains_path(
            responses.get("nfs1").expect("nfs1 query response"),
            b"/ready-a.txt"
        ),
        "same-instance reactivate must preserve nfs1 materialized payload instead of recreating it from scratch"
    );
    assert!(
        payload_contains_path(
            responses.get("nfs2").expect("nfs2 query response"),
            b"/ready-b.txt"
        ),
        "same-instance reactivate must preserve retained nfs2 materialized payload instead of recreating it from scratch"
    );

    let nested_query_events = sink
        .materialized_query(&materialized_tree_request(b"/nested", true, Some(1)))
        .expect("nested query after same-instance reactivate");
    let nested_responses = nested_query_events
        .iter()
        .map(|event| {
            (
                event.metadata().origin_id.0.clone(),
                decode_tree_payload(event),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let nfs2_nested_response = nested_responses
        .get("nfs2")
        .expect("nfs2 nested response after same-instance reactivate");
    assert!(
        nfs2_nested_response.root.exists && nfs2_nested_response.root.path == b"/nested",
        "same-instance reactivate must preserve retained nfs2 nested root instead of collapsing it after scope wobble: {nfs2_nested_response:?}"
    );
    assert!(
        payload_contains_path(nfs2_nested_response, b"/nested/peer.txt"),
        "same-instance reactivate must preserve retained nfs2 nested max-depth payload instead of dropping peer.txt"
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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
            .is_some_and(|group| group.is_ready())
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
async fn active_stream_route_refreshes_dynamic_object_refs_and_accepts_new_origin_batches() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", "/mnt/nfs1"),
        RootSpec::new("nfs2", "/mnt/nfs2"),
    ];
    cfg.host_object_grants = vec![
        granted_mount_root("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        granted_mount_root("node-a::nfs2", "node-a", "10.0.0.12", "/mnt/nfs2", true),
    ];
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

    sink.enable_stream_receive();
    sink.on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
        RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope("nfs1"), bound_scope("nfs2")],
        },
    ))
    .expect("encode stream activate")])
        .await
        .expect("activate stream route");

    assert_eq!(
        sink.scheduled_stream_object_refs()
            .expect("scheduled stream refs before grants refresh"),
        Some(std::collections::BTreeSet::from([
            "node-a::nfs1".to_string(),
            "node-a::nfs2".to_string(),
        ])),
        "precondition: stream route should start from the initial concrete object refs"
    );

    let grants_changed = host_object_grants_changed_envelope(
        1,
        &[
            granted_mount_root(
                "node-a-activation-42::nfs1",
                "node-a",
                "10.0.0.11",
                "/mnt/nfs1",
                true,
            ),
            granted_mount_root(
                "node-a-activation-42::nfs2",
                "node-a",
                "10.0.0.12",
                "/mnt/nfs2",
                true,
            ),
        ],
    );
    sink.on_control_frame(&[grants_changed])
        .await
        .expect("grants refresh should update stream targets");

    assert_eq!(
        sink.scheduled_stream_object_refs()
            .expect("scheduled stream refs after grants refresh"),
        Some(std::collections::BTreeSet::from([
            "node-a-activation-42::nfs1".to_string(),
            "node-a-activation-42::nfs2".to_string(),
        ])),
        "active stream route must refresh its concrete object refs after host grant refresh"
    );

    sink.ingest_stream_events(&[
        mk_source_event(
            "node-a-activation-42::nfs1",
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
            "node-a-activation-42::nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_source_event(
            "node-a-activation-42::nfs1",
            mk_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_control_event(
            "node-a-activation-42::nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_source_event(
            "node-a-activation-42::nfs2",
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
            "node-a-activation-42::nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_source_event(
            "node-a-activation-42::nfs2",
            mk_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_control_event(
            "node-a-activation-42::nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .expect("stream events with refreshed object refs should apply");

    let snapshot = sink
        .status_snapshot()
        .expect("status after refreshed object-ref stream ingest");
    assert_eq!(
        snapshot
            .stream_received_batches_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0),
        1,
        "sink should record one received stream batch after refreshed object-ref ingress: {snapshot:?}"
    );
    let ready_groups = snapshot
        .groups
        .iter()
        .filter(|group| group.readiness == GroupReadinessState::Ready && group.live_nodes > 0)
        .map(|group| group.group_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ready_groups,
        std::collections::BTreeSet::from(["nfs1", "nfs2"]),
        "refreshed object-ref ingress must still materialize both contracted groups: {snapshot:?}"
    );
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
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");

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

    sink.send_with_failure(&[
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
    sink.send_with_failure(&[mk_control_event(
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
        Some(UnreliableReason::WatchOverflowPendingMaterialization)
    );

    sink.send_with_failure(&[
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
    sink.send_with_failure(&[mk_source_event(
        "node-a::exp",
        mk_record(b"/a.txt", "a.txt", 10, EventKind::Update),
    )])
    .await
    .expect("apply scan record");

    sink.send_with_failure(&[mk_control_event(
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
        Some(UnreliableReason::WatchOverflowPendingMaterialization)
    );
}

#[tokio::test]
async fn initial_audit_completion_waits_for_materialized_root() {
    let sink = build_single_group_sink();
    sink.send_with_failure(&[
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
        !snapshot.groups[0].is_ready(),
        "audit epoch without a materialized root must stay not-ready"
    );

    sink.send_with_failure(&[mk_source_event(
        "node-a::exp",
        mk_record(b"/ready.txt", "ready.txt", 3, EventKind::Update),
    )])
    .await
    .expect("materialize root path");

    let snapshot = sink
        .status_snapshot()
        .expect("sink status after root materializes");
    assert!(
        snapshot.groups[0].is_ready(),
        "materialized root should unlock initial audit readiness after audit completion"
    );
}

#[tokio::test]
async fn status_snapshot_exports_waiting_for_materialized_root_after_audit_completes_without_materialized_root()
 {
    let sink = build_single_group_sink();
    sink.send_with_failure(&[
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
    assert_eq!(
        snapshot.groups[0].readiness,
        GroupReadinessState::WaitingForMaterializedRoot,
        "status snapshot must export the distinct waiting-for-materialized-root state after audit completes without a live materialized root: {snapshot:?}"
    );
    assert!(
        !snapshot.groups[0].is_ready(),
        "waiting-for-materialized-root must remain unready after an audit completes without a live materialized root"
    );
}

#[tokio::test]
async fn status_snapshot_exports_readiness_only_when_waiting_for_materialized_root() {
    let sink = build_single_group_sink();
    sink.send_with_failure(&[
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
    let group = &snapshot.groups[0];
    assert_eq!(
        group.readiness,
        GroupReadinessState::WaitingForMaterializedRoot,
        "status snapshot must keep the distinct waiting-for-materialized-root state after audit completes without a live materialized root: {snapshot:?}"
    );
    assert!(!group.is_ready());

    let group_json = serde_json::to_value(group).expect("serialize sink group status snapshot");
    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::WaitingForMaterializedRoot)
            .expect("serialize waiting-for-materialized-root"),
    );
    assert!(
        group_json.get("initial_audit_completed").is_none(),
        "status snapshot export must drop the legacy initial-audit compatibility bit: {group_json}"
    );
    assert!(
        group_json.get("audit_epoch_completed").is_none(),
        "status snapshot export must drop the legacy audit-epoch compatibility bit: {group_json}"
    );
}

#[test]
fn sink_group_status_snapshot_deserialize_preserves_waiting_for_root_readiness() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "unassigned",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::WaitingForMaterializedRoot,
        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(
        snapshot.readiness,
        GroupReadinessState::WaitingForMaterializedRoot
    );
    assert!(!snapshot.is_ready());
}

#[test]
fn sink_group_status_snapshot_deserialize_preserves_ready_readiness() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "node-a::exp",
        "total_nodes": 1,
        "live_nodes": 1,
        "tombstoned_count": 0,
        "attested_count": 1,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 1,
        "shadow_lag_us": 1,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::Ready,
        "materialized_revision": 8,
        "estimated_heap_bytes": 128
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(snapshot.readiness, GroupReadinessState::Ready);
    assert!(snapshot.is_ready());
}

#[test]
fn sink_group_status_snapshot_deserialize_requires_readiness_field() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "node-a::exp",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "overflow_pending_materialization": false,
        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let err = serde_json::from_value::<SinkGroupStatusSnapshot>(group_json)
        .expect_err("missing readiness must now fail");
    assert!(
        err.to_string().contains("missing field `readiness`"),
        "readiness-only sink snapshots must reject legacy shapes that omit readiness: {err}"
    );
}

#[test]
fn sink_group_status_snapshot_deserialize_requires_overflow_pending_materialization() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "node-a::exp",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "readiness": GroupReadinessState::PendingMaterialization,
        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let err = serde_json::from_value::<SinkGroupStatusSnapshot>(group_json)
        .expect_err("missing overflow_pending_materialization must now fail");

    assert!(
        err.to_string()
            .contains("missing field `overflow_pending_materialization`"),
        "sink group status snapshots should reject legacy shapes that omit overflow_pending_materialization: {err}",
    );
}

#[test]
fn sink_group_status_snapshot_serialize_exports_readiness_only_when_waiting_for_root() {
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::WaitingForMaterializedRoot,
        materialized_revision: 7,
        estimated_heap_bytes: 0,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::WaitingForMaterializedRoot)
            .expect("serialize waiting-for-materialized-root"),
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}

#[test]
fn sink_group_status_snapshot_serialize_exports_readiness_only_when_ready() {
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "node-a::exp".to_string(),
        total_nodes: 1,
        live_nodes: 1,
        tombstoned_count: 0,
        attested_count: 1,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 1,
        shadow_lag_us: 1,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::Ready,
        materialized_revision: 8,
        estimated_heap_bytes: 128,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::Ready).expect("serialize ready"),
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}

#[test]
fn sink_group_status_snapshot_deserialize_normalizes_zero_row_placeholder_primary_readiness() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "unassigned",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::Ready,


        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(
        snapshot.readiness,
        GroupReadinessState::WaitingForMaterializedRoot,
        "deserialization must normalize stale exported readiness=Ready to waiting-for-materialized-root when the snapshot is still a structural zero row with a placeholder primary: {snapshot:?}"
    );
    assert!(
        !snapshot.is_ready(),
        "deserialization must keep a zero-row placeholder-primary snapshot unready after normalizing it to waiting-for-materialized-root: {snapshot:?}"
    );
    assert_eq!(
        snapshot.normalized_readiness(),
        GroupReadinessState::WaitingForMaterializedRoot,
        "normalized zero-row placeholder-primary snapshots must surface waiting-for-materialized-root directly: {snapshot:?}"
    );
}

#[test]
fn sink_group_status_snapshot_deserialize_normalizes_zero_row_empty_primary_readiness() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::Ready,


        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(
        snapshot.readiness,
        GroupReadinessState::WaitingForMaterializedRoot,
        "deserialization must normalize stale exported readiness=Ready to waiting-for-materialized-root when the snapshot is still a structural zero row with an empty legacy placeholder primary: {snapshot:?}"
    );
    assert!(
        !snapshot.is_ready(),
        "deserialization must keep a zero-row empty-primary snapshot unready after normalizing it to waiting-for-materialized-root: {snapshot:?}"
    );
    assert_eq!(
        snapshot.normalized_readiness(),
        GroupReadinessState::WaitingForMaterializedRoot,
        "normalized zero-row empty-primary snapshots must surface waiting-for-materialized-root directly: {snapshot:?}"
    );
}

#[test]
fn sink_group_status_snapshot_deserialize_normalizes_zero_row_bound_primary_readiness() {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "node-a::exp",
        "total_nodes": 0,
        "live_nodes": 0,
        "tombstoned_count": 0,
        "attested_count": 0,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 0,
        "shadow_lag_us": 0,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::Ready,


        "materialized_revision": 7,
        "estimated_heap_bytes": 0
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(
        snapshot.readiness,
        GroupReadinessState::PendingMaterialization,
        "deserialization must normalize stale exported readiness=Ready to pending-materialization when the snapshot is still a structural zero row with a bound primary: {snapshot:?}"
    );
    assert!(
        !snapshot.is_ready(),
        "deserialization must keep a zero-row bound-primary snapshot unready after normalizing it to pending-materialization: {snapshot:?}"
    );
    assert_eq!(
        snapshot.normalized_readiness(),
        GroupReadinessState::PendingMaterialization,
        "normalized zero-row bound-primary snapshots must surface pending-materialization directly: {snapshot:?}"
    );
}

#[test]
fn sink_group_status_snapshot_serialize_normalizes_zero_row_placeholder_primary_readiness() {
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::Ready,

        materialized_revision: 7,
        estimated_heap_bytes: 0,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::WaitingForMaterializedRoot)
            .expect("serialize waiting-for-materialized-root"),
        "serialization must normalize stale readiness=Ready to waiting-for-materialized-root when the snapshot is still a structural zero row with a placeholder primary: {group_json}"
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}

#[test]
fn sink_group_status_snapshot_serialize_normalizes_zero_row_empty_primary_readiness() {
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::Ready,

        materialized_revision: 7,
        estimated_heap_bytes: 0,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::WaitingForMaterializedRoot)
            .expect("serialize waiting-for-materialized-root"),
        "serialization must normalize stale readiness=Ready to waiting-for-materialized-root when the snapshot is still a structural zero row with an empty legacy placeholder primary: {group_json}"
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}

#[test]
fn sink_group_status_snapshot_serialize_normalizes_zero_row_bound_primary_readiness() {
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "node-a::exp".to_string(),
        total_nodes: 0,
        live_nodes: 0,
        tombstoned_count: 0,
        attested_count: 0,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 0,
        shadow_lag_us: 0,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::Ready,

        materialized_revision: 7,
        estimated_heap_bytes: 0,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::PendingMaterialization)
            .expect("serialize pending-materialization"),
        "serialization must normalize stale readiness=Ready to pending-materialization when the snapshot is still a structural zero row with a bound primary: {group_json}"
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}

#[test]
fn persisted_group_sink_state_serializes_readiness_state_without_legacy_audit_bit() {
    let mut group = GroupSinkState::new("node-a::exp".to_string(), TombstonePolicy::default());
    group.mark_materialization_ready();

    let persisted = PersistedGroupSinkState::from_live("exp", &group);
    let persisted_json =
        serde_json::to_value(&persisted).expect("serialize persisted group sink state");

    assert_eq!(
        persisted_json["readiness_state"],
        serde_json::to_value(GroupReadinessState::WaitingForMaterializedRoot)
            .expect("serialize waiting-for-materialized-root"),
        "persisted sink core state must carry readiness-native truth once audit completes without materialized nodes: {persisted_json}"
    );
    assert!(
        persisted_json.get("audit_epoch_completed").is_none(),
        "persisted sink core state must not keep exporting legacy audit_epoch_completed once readiness_state is canonical: {persisted_json}"
    );
}

#[test]
fn group_readiness_state_distinguishes_pending_materialization_waiting_for_materialized_root_and_ready()
 {
    let mut group = GroupSinkState::new("node-a::exp".to_string(), TombstonePolicy::default());
    assert_eq!(
        group.group_readiness_state(),
        GroupReadinessState::PendingMaterialization
    );

    group.mark_materialization_ready();
    assert_eq!(
        group.group_readiness_state(),
        GroupReadinessState::WaitingForMaterializedRoot
    );

    group.tree.insert(
        b"/ready.txt".to_vec(),
        tree::FileMetaNode {
            size: 1,
            modified_time_us: 1,
            created_time_us: 1,
            is_dir: false,
            source: crate::SyncTrack::Scan,
            monitoring_attested: true,
            last_confirmed_at: None,
            suspect_until: None,
            blind_spot: false,
            is_tombstoned: false,
            tombstone_expires_at: None,
            last_seen_epoch: 0,
            subtree_last_write_significant_change_at: None,
        },
    );
    assert_eq!(group.group_readiness_state(), GroupReadinessState::Ready);
}

#[test]
fn group_readiness_state_rejects_legacy_pending_audit_aliases() {
    assert!(
        serde_json::from_value::<GroupReadinessState>(serde_json::Value::String(
            "PendingAudit".to_string()
        ))
        .is_err(),
        "hard-cut materialization vocabulary should reject legacy PendingAudit input",
    );
    assert!(
        serde_json::from_value::<GroupReadinessState>(serde_json::Value::String(
            "pending-audit".to_string()
        ))
        .is_err(),
        "hard-cut materialization vocabulary should reject legacy pending-audit input",
    );
}

#[test]
fn unreliable_reason_rejects_legacy_watch_overflow_pending_audit_alias() {
    assert!(
        serde_json::from_value::<UnreliableReason>(serde_json::Value::String(
            "WatchOverflowPendingAudit".to_string()
        ))
        .is_err(),
        "hard-cut query reliability vocabulary should reject legacy WatchOverflowPendingAudit input",
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
    sink.send_with_failure(&[
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
            .is_some_and(|group| group.is_ready()),
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
            .is_some_and(|group| group.is_ready()),
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
    sink.send_with_failure(&[
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
        !snapshot.groups[0].is_ready(),
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
    sink.send_with_failure(&[
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
        snapshot_ready.groups[0].is_ready(),
        "precondition: group should be ready after materializing /ready.txt"
    );
    assert!(
        snapshot_ready.groups[0].total_nodes > 0,
        "precondition: ready group should have live materialized nodes"
    );

    sink.send_with_failure(&[mk_source_event(
        "node-a::exp",
        mk_record(b"/ready.txt", "ready.txt", 4, EventKind::Delete),
    )])
    .await
    .expect("delete only materialized child");

    let snapshot_after_delete = sink.status_snapshot().expect("sink status after delete");
    assert!(
        !snapshot_after_delete.groups[0].is_ready(),
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

#[test]
fn sink_group_status_snapshot_deserialize_normalizes_live_row_waiting_for_materialized_root_to_ready()
 {
    let group_json = serde_json::json!({
        "group_id": "nfs1",
        "primary_object_ref": "unassigned",
        "total_nodes": 1,
        "live_nodes": 1,
        "tombstoned_count": 0,
        "attested_count": 1,
        "suspect_count": 0,
        "blind_spot_count": 0,
        "shadow_time_us": 1,
        "shadow_lag_us": 1,
        "overflow_pending_materialization": false,
        "readiness": GroupReadinessState::WaitingForMaterializedRoot,


        "materialized_revision": 9,
        "estimated_heap_bytes": 128
    });

    let snapshot: SinkGroupStatusSnapshot =
        serde_json::from_value(group_json).expect("deserialize sink group status snapshot");

    assert_eq!(
        snapshot.readiness,
        GroupReadinessState::Ready,
        "deserialization must normalize stale waiting-for-materialized-root readiness back to ready when the snapshot already has a live materialized row: {snapshot:?}"
    );
    assert!(
        snapshot.is_ready(),
        "deserialization must normalize a live-row snapshot back to ready: {snapshot:?}"
    );
    assert_eq!(
        snapshot.normalized_readiness(),
        GroupReadinessState::Ready,
        "normalized live-row snapshots must surface ready directly: {snapshot:?}"
    );
}

#[test]
fn sink_group_status_snapshot_serialize_normalizes_live_row_waiting_for_materialized_root_to_ready()
{
    let snapshot = SinkGroupStatusSnapshot {
        group_id: "nfs1".to_string(),
        primary_object_ref: "unassigned".to_string(),
        total_nodes: 1,
        live_nodes: 1,
        tombstoned_count: 0,
        attested_count: 1,
        suspect_count: 0,
        blind_spot_count: 0,
        shadow_time_us: 1,
        shadow_lag_us: 1,
        overflow_pending_materialization: false,
        readiness: GroupReadinessState::WaitingForMaterializedRoot,

        materialized_revision: 9,
        estimated_heap_bytes: 128,
    };

    let group_json = serde_json::to_value(&snapshot).expect("serialize sink group status snapshot");

    assert_eq!(
        group_json["readiness"],
        serde_json::to_value(GroupReadinessState::Ready).expect("serialize ready"),
        "serialization must normalize stale waiting-for-materialized-root readiness back to ready when the snapshot already has a live materialized row: {group_json}"
    );
    assert!(group_json.get("initial_audit_completed").is_none());
    assert!(group_json.get("audit_epoch_completed").is_none());
}
