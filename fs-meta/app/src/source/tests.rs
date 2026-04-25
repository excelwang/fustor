use super::*;
use crate::ControlEvent;
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::execution_units::SOURCE_SCAN_RUNTIME_UNIT_ID;
use crate::runtime::orchestration::encode_logical_roots_control_payload;
use crate::runtime::routes::{
    ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
    ROUTE_KEY_SOURCE_ROOTS_CONTROL,
};
use crate::sink::SinkFileMeta;
use crate::state::cell::LogicalRootsCell;
use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_runtime_entry_sdk::advanced::boundary::BoundaryContext;
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
    RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick, encode_runtime_exec_control,
    encode_runtime_host_grant_change, encode_runtime_unit_tick,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};

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

struct SingleRootsControlEventBoundary {
    route_key: String,
    payload: bytes::Bytes,
    delivered: AtomicBool,
}

impl SingleRootsControlEventBoundary {
    fn new(route_key: String, payload: Vec<u8>) -> Self {
        Self {
            route_key,
            payload: bytes::Bytes::from(payload),
            delivered: AtomicBool::new(false),
        }
    }

    fn delivered(&self) -> bool {
        self.delivered.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl ChannelIoSubset for SingleRootsControlEventBoundary {
    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: capanix_runtime_entry_sdk::advanced::boundary::ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        if request.channel_key.0 == self.route_key
            && self
                .delivered
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Ok(vec![Event::new(
                EventMetadata {
                    origin_id: NodeId("node-a".to_string()),
                    timestamp_us: 1,
                    logical_ts: None,
                    correlation_id: None,
                    ingress_auth: None,
                    trace: None,
                },
                self.payload.clone(),
            )]);
        }
        Err(CnxError::Timeout)
    }
}

fn root(id: &str, path: &str) -> RootSpec {
    RootSpec::new(id, std::path::PathBuf::from(path))
}

fn test_export(
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

fn route_export(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: &str,
    active: bool,
) -> RuntimeHostGrant {
    RuntimeHostGrant {
        object_ref: object_ref.to_string(),
        object_type: RuntimeHostObjectType::MountRoot,
        interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
        host: RuntimeHostDescriptor {
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
        },
        object: RuntimeObjectDescriptor {
            mount_point: mount_point.to_string(),
            fs_source: mount_point.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
        },
        grant_state: if active {
            RuntimeHostGrantState::Active
        } else {
            RuntimeHostGrantState::Revoked
        },
    }
}

fn build_source(initial_grants: Vec<GrantedMountRoot>) -> FSMetaSource {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = initial_grants;
    FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None).expect("build source")
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

fn pending_root_task_handle() -> RootTaskHandle {
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let (_ready_tx, ready_rx) = tokio::sync::watch::channel(false);
    let join = tokio::spawn(async move {
        task_cancel.cancelled().await;
    });
    RootTaskHandle {
        cancel,
        join,
        ready_rx,
    }
}

fn force_find_params() -> crate::query::request::InternalQueryRequest {
    crate::query::request::InternalQueryRequest::force_find(
        crate::query::request::QueryOp::Tree,
        crate::query::request::QueryScope {
            path: b"/".to_vec(),
            recursive: true,
            max_depth: None,
            selected_group: None,
        },
    )
}

fn event_origin_ids(events: &[Event]) -> BTreeSet<String> {
    events
        .iter()
        .map(|event| event.metadata().origin_id.0.clone())
        .collect()
}

#[test]
fn local_source_progress_snapshot_reads_machine_owned_published_group_epoch_from_state_cell() {
    let source = build_source(Vec::new());
    let request_epoch = source.state_cell.begin_rescan_request_epoch(0, 0, 0);

    assert!(
        source.status_snapshot().published_group_ids().is_empty(),
        "fixture must keep status-derived published groups empty so this test proves local progress reads source-owned state instead of observability/status adapters",
    );

    source.state_cell.mark_group_published("nfs1");

    let snapshot = source.progress_snapshot();
    assert_eq!(
        snapshot.rescan_observed_epoch, request_epoch,
        "local source progress must advance rescan_observed_epoch when the source owner records a published group for the active request",
    );
    assert_eq!(
        snapshot.published_group_epoch,
        BTreeMap::from([("nfs1".to_string(), request_epoch)]),
        "local source progress must read machine-owned published_group_epoch truth from SourceStateCell instead of reconstructing it from status/observability counters",
    );
    assert!(
        snapshot
            .published_expected_groups_since(request_epoch, &BTreeSet::from(["nfs1".to_string()]),),
        "local source progress should satisfy request-scoped publication once SourceStateCell records the group epoch",
    );
}

fn mk_source_record_event(origin: &str, path: &[u8], file_name: &[u8], ts: u64) -> Event {
    let record = FileMetaRecord::scan_update(
        path.to_vec(),
        file_name.to_vec(),
        capanix_host_fs_types::UnixStat {
            is_dir: false,
            size: 1,
            mtime_us: ts,
            ctime_us: ts,
            dev: None,
            ino: None,
        },
        b"/force-find-stress".to_vec(),
        ts,
        false,
    );
    Event::new(
        EventMetadata {
            origin_id: NodeId(origin.to_string()),
            timestamp_us: ts,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        bytes::Bytes::from(rmp_serde::to_vec_named(&record).expect("encode source record event")),
    )
}

fn selected_group_tree_contains_path(
    sink: &SinkFileMeta,
    group_id: &str,
    query_path: &[u8],
    needle_path: &[u8],
) -> bool {
    let request = InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: query_path.to_vec(),
            recursive: true,
            max_depth: None,
            selected_group: Some(group_id.to_string()),
        },
        Some(Default::default()),
    );
    let Ok(events) = sink.materialized_query(&request) else {
        return false;
    };
    let Some(event) = events.first() else {
        return false;
    };
    match rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes()) {
        Ok(MaterializedQueryPayload::Tree(payload)) => {
            (payload.root.exists && payload.root.path == needle_path)
                || payload
                    .entries
                    .iter()
                    .any(|entry| entry.path == needle_path)
        }
        _ => false,
    }
}

#[test]
fn dropping_source_clone_does_not_cancel_shared_shutdown() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);
    let clone = source.clone();
    drop(clone);
    assert!(
        !source.shutdown.is_cancelled(),
        "dropping a clone must not cancel the shared source runtime shutdown token"
    );
}

#[tokio::test]
async fn runtime_host_object_grants_changed_updates_fanout() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);

    let envelope = encode_runtime_host_grant_change(&RuntimeHostGrantChange {
        version: 2,
        grants: vec![
            route_export("node-a", "node-a", "10.0.0.11", "/mnt/nfs1", true),
            route_export("node-b", "node-b", "10.0.0.12", "/mnt/nfs2", true),
            route_export("node-z", "node-z", "10.0.0.13", "relative/path", true),
        ],
    })
    .expect("encode runtime host object grants changed");

    source
        .on_control_frame(&[envelope])
        .await
        .expect("apply host object grants changed frame");

    assert_eq!(source.host_object_grants_version.load(Ordering::Relaxed), 2);
    let grants = lock_or_recover(
        &source.state_cell.host_object_grants,
        "test.host_object_grants",
    )
    .clone();
    assert_eq!(grants.len(), 2, "non-absolute mount path must be filtered");

    let fanout = lock_or_recover(
        &source.state_cell.logical_root_fanout,
        "test.logical_root_fanout",
    )
    .clone();
    assert_eq!(fanout.get("nfs1").map(|v| v.len()), Some(1));
    assert_eq!(fanout.get("nfs2").map(|v| v.len()), Some(1));

    let health = lock_or_recover(&source.state_cell.fanout_health, "test.fanout_health");
    assert_eq!(
        health.logical_root.get("nfs1").map(String::as_str),
        Some("ready")
    );
    assert_eq!(
        health.logical_root.get("nfs2").map(String::as_str),
        Some("ready")
    );
}

#[tokio::test]
async fn start_runtime_endpoints_restarts_after_finished_endpoint_task() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);

    lock_or_recover(
        &source.endpoint_tasks,
        "test.source.restart_after_finished.endpoint_tasks.seed",
    )
    .push(finished_endpoint_task_for_test(
        "source.test.finished:v1.req",
    ));

    source
        .start_runtime_endpoints(Arc::new(NoopBoundary))
        .await
        .expect("start runtime endpoints");

    let endpoint_count = lock_or_recover(
        &source.endpoint_tasks,
        "test.source.restart_after_finished.endpoint_tasks.after",
    )
    .len();
    assert!(
        endpoint_count > 1,
        "source runtime start must prune terminal endpoint tasks and restart endpoints instead of treating any non-empty task list as already-started"
    );

    source.close().await.expect("close source");
}

#[tokio::test]
async fn control_stream_endpoints_stay_gated_until_route_activation() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);
    let boundary = Arc::new(RouteCountingTimeoutBoundary::default());

    source
        .start_runtime_endpoints(boundary.clone())
        .await
        .expect("start runtime endpoints");

    let rescan_control_route = format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL);
    let roots_control_route = format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL);
    let pre_activation_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < pre_activation_deadline {
        let rescan_recv = boundary.recv_count(&rescan_control_route);
        let roots_recv = boundary.recv_count(&roots_control_route);
        assert_eq!(
            rescan_recv,
            0,
            "source rescan-control stream must remain gated before runtime route activation; recv_counts={:?}",
            boundary.recv_counts_snapshot()
        );
        assert_eq!(
            roots_recv,
            0,
            "source logical-roots control stream must remain gated before runtime route activation; recv_counts={:?}",
            boundary.recv_counts_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            }))
            .expect("encode source roots-control activate"),
        ])
        .await
        .expect("activate source control streams");

    let activated_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let rescan_recv = boundary.recv_count(&rescan_control_route);
        let roots_recv = boundary.recv_count(&roots_control_route);
        if rescan_recv > 0 && roots_recv > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < activated_deadline,
            "source control streams must begin receiving after route activation; rescan_recv={} roots_recv={} recv_counts={:?}",
            rescan_recv,
            roots_recv,
            boundary.recv_counts_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source.close().await.expect("close source");
}

#[tokio::test]
async fn owner_scoped_roots_control_stream_stays_gated_until_route_activation() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);
    let boundary = Arc::new(RouteCountingTimeoutBoundary::default());

    source
        .start_runtime_endpoints(boundary.clone())
        .await
        .expect("start runtime endpoints");

    let roots_control_route =
        crate::runtime::routes::source_roots_control_stream_route_for("node-a").0;
    let pre_activation_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < pre_activation_deadline {
        let roots_recv = boundary.recv_count(&roots_control_route);
        assert_eq!(
            roots_recv,
            0,
            "source owner-scoped logical-roots control stream must remain gated before runtime route activation; recv_counts={:?}",
            boundary.recv_counts_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source
        .on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key: roots_control_route.clone(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-a::nfs1".to_string()],
                }],
            },
        ))
        .expect("encode scoped source roots-control activate")])
        .await
        .expect("activate owner-scoped source roots-control route");

    let activated_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let roots_recv = boundary.recv_count(&roots_control_route);
        if roots_recv > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < activated_deadline,
            "source owner-scoped logical-roots control stream must begin receiving after route activation; recv_counts={:?}",
            boundary.recv_counts_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source.close().await.expect("close source");
}

#[tokio::test]
async fn source_state_authority_log_records_runtime_mutations() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);
    let before = source.state_cell.authority_log_len();
    assert!(before >= 1, "bootstrap should append authority record");

    source
        .update_logical_roots(vec![root("nfs1", "/mnt/nfs1"), root("nfs3", "/mnt/nfs3")])
        .await
        .expect("update logical roots");
    let after_update = source.state_cell.authority_log_len();
    assert!(
        after_update > before,
        "logical root update should append authority record (before={}, after={})",
        before,
        after_update
    );

    let envelope = encode_runtime_host_grant_change(&RuntimeHostGrantChange {
        version: 1,
        grants: vec![route_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )],
    })
    .expect("encode runtime host object grants changed");
    source
        .on_control_frame(&[envelope])
        .await
        .expect("apply runtime host object grants changed frame");
    let after_control = source.state_cell.authority_log_len();
    assert!(
        after_control > after_update,
        "runtime host object grants changed should append authority record (update={}, control={})",
        after_update,
        after_control
    );
}

#[tokio::test]
async fn logical_roots_survive_restart_on_shared_state_boundary_after_update() {
    let boundary = in_memory_state_boundary();
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        root("nfs2", "/mnt/nfs2"),
        root("nfs3", "/mnt/nfs3"),
        root("nfs4", "/mnt/nfs4"),
    ];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs2", "node-a", "10.0.0.11", "/mnt/nfs2", true),
        test_export("node-b::nfs3", "node-b", "10.0.0.12", "/mnt/nfs3", true),
        test_export("node-c::nfs4", "node-c", "10.0.0.13", "/mnt/nfs4", true),
    ];

    let source = FSMetaSource::with_boundaries_and_state(
        cfg.clone(),
        NodeId("node-a".to_string()),
        None,
        boundary.clone(),
    )
    .expect("build source");
    source
        .update_logical_roots(vec![root("nfs2", "/mnt/nfs2"), root("nfs4", "/mnt/nfs4")])
        .await
        .expect("update logical roots");
    source.close().await.expect("close source");

    let restarted =
        FSMetaSource::with_boundaries_and_state(cfg, NodeId("node-a".to_string()), None, boundary)
            .expect("restart source");
    let restarted_root_ids = restarted
        .logical_roots_snapshot()
        .into_iter()
        .map(|root| root.id)
        .collect::<Vec<_>>();
    assert_eq!(
        restarted_root_ids,
        vec!["nfs2".to_string(), "nfs4".to_string()],
        "restart on the same state boundary must preserve the updated authoritative roots instead of reintroducing retired roots",
    );
}

#[tokio::test]
async fn authoritative_logical_roots_sync_recovers_local_runtime_after_missed_online_second_wave() {
    let boundary = in_memory_state_boundary();
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-a::nfs2", "node-a", "10.0.0.11", "/mnt/nfs2", true),
    ];

    let source = FSMetaSource::with_boundaries_and_state(
        cfg.clone(),
        NodeId("node-a".to_string()),
        None,
        boundary.clone(),
    )
    .expect("build source");

    let authoritative =
        LogicalRootsCell::from_state_boundary(SOURCE_RUNTIME_UNIT_ID, cfg.roots.clone(), boundary)
            .expect("authoritative logical roots cell");
    authoritative
        .replace(vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")])
        .await
        .expect("replace authoritative logical roots");

    let changed = source
        .sync_logical_roots_from_authoritative_cell_if_changed()
        .await
        .expect("sync logical roots from authority");
    assert!(
        changed,
        "source should detect authoritative logical-roots drift"
    );

    let root_ids = source
        .logical_roots_snapshot()
        .into_iter()
        .map(|root| root.id)
        .collect::<Vec<_>>();
    assert_eq!(
        root_ids,
        vec!["nfs1".to_string(), "nfs2".to_string()],
        "authoritative sync must refresh the local logical-roots view after a missed online second wave",
    );
    let concrete_root_ids = source
        .status_snapshot()
        .concrete_roots
        .into_iter()
        .map(|root| root.logical_root_id)
        .collect::<Vec<_>>();
    assert!(
        concrete_root_ids.contains(&"nfs2".to_string()),
        "authoritative sync must rebuild local concrete roots for newly-authoritative logical roots",
    );
}

#[tokio::test]
async fn roots_control_stream_persists_authoritative_roots_for_followup_status_and_force_find() {
    let state_boundary = in_memory_state_boundary();
    let mut cfg = SourceConfig::default();
    cfg.roots = Vec::new();
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-a::nfs2", "node-a", "10.0.0.11", "/mnt/nfs2", true),
    ];

    let source = FSMetaSource::with_boundaries_and_state(
        cfg,
        NodeId("node-a".to_string()),
        None,
        state_boundary.clone(),
    )
    .expect("build source");

    let route_key = format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL);
    let payload = encode_logical_roots_control_payload(&vec![
        root("nfs1", "/mnt/nfs1"),
        root("nfs2", "/mnt/nfs2"),
    ])
    .expect("encode logical-roots control payload");
    let boundary = Arc::new(SingleRootsControlEventBoundary::new(
        route_key.clone(),
        payload,
    ));

    source
        .start_runtime_endpoints(boundary.clone())
        .await
        .expect("start runtime endpoints");
    source
        .on_control_frame(&[encode_runtime_exec_control(&RuntimeExecControl::Activate(
            RuntimeExecActivate {
                route_key,
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            },
        ))
        .expect("encode source roots-control activate")])
        .await
        .expect("activate source roots-control route");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let root_ids = source
            .logical_roots_snapshot()
            .into_iter()
            .map(|root| root.id)
            .collect::<Vec<_>>();
        if boundary.delivered() && root_ids == vec!["nfs1".to_string(), "nfs2".to_string()] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "roots-control stream should update local logical roots before timeout; delivered={} logical_roots={root_ids:?}",
            boundary.delivered(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let authoritative =
        LogicalRootsCell::from_state_boundary(SOURCE_RUNTIME_UNIT_ID, Vec::new(), state_boundary)
            .expect("authoritative logical roots cell");
    let authoritative_root_ids = authoritative
        .snapshot()
        .into_iter()
        .map(|root| root.id)
        .collect::<Vec<_>>();
    assert_eq!(
        authoritative_root_ids,
        vec!["nfs1".to_string(), "nfs2".to_string()],
        "roots-control stream must persist authoritative logical roots so later status/force-find reads do not revert peers back to empty roots",
    );

    let changed = source
        .sync_logical_roots_from_authoritative_cell_if_changed()
        .await
        .expect("sync logical roots from authoritative cell");
    assert!(
        !changed,
        "once roots-control applies the authoritative declaration locally, followup status/force-find sync should not observe stale logical-root drift",
    );
}

#[tokio::test]
async fn runtime_host_object_grants_changed_survives_restart_on_shared_state_boundary_for_runtime_managed_schedule_recovery()
 {
    let boundary = in_memory_state_boundary();
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = Vec::new();

    let node_id = NodeId("node-c-29775351239650530581020673".to_string());
    let source = FSMetaSource::with_boundaries_and_state(
        cfg.clone(),
        node_id.clone(),
        Some(Arc::new(NoopBoundary)),
        boundary.clone(),
    )
    .expect("build runtime-managed source");

    source
        .on_control_frame(&[
            encode_runtime_host_grant_change(&RuntimeHostGrantChange {
                version: 1,
                grants: vec![
                    route_export("node-c::nfs1", "node-c", "10.0.0.31", "/mnt/nfs1", true),
                    route_export("node-c::nfs2", "node-c", "10.0.0.32", "/mnt/nfs2", true),
                ],
            })
            .expect("encode runtime host grants changed"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-c::nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-c::nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_RESCAN.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-c::nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-c::nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply initial grants+source control");

    let expected_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = source
            .scheduled_source_group_ids()
            .expect("initial source groups")
            .unwrap_or_default();
        let scan_groups = source
            .scheduled_scan_group_ids()
            .expect("initial scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < initial_deadline,
            "timed out waiting for initial runtime-managed source schedule convergence: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source.close().await.expect("close source");

    let restarted = FSMetaSource::with_boundaries_and_state(
        cfg,
        node_id,
        Some(Arc::new(NoopBoundary)),
        boundary,
    )
    .expect("restart runtime-managed source");

    restarted
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-c::nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-c::nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode restarted source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_RESCAN.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["node-c::nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["node-c::nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode restarted scan activate"),
        ])
        .await
        .expect("apply restarted source control");

    let restart_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = restarted
            .scheduled_source_group_ids()
            .expect("restarted source groups")
            .unwrap_or_default();
        let scan_groups = restarted
            .scheduled_scan_group_ids()
            .expect("restarted scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < restart_deadline,
            "restart on the same state boundary must preserve runtime host grants for runtime-managed schedule recovery: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn runtime_host_object_grants_changed_ignores_stale_versions() {
    let source = build_source(vec![test_export(
        "node-a",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);

    let first = encode_runtime_host_grant_change(&RuntimeHostGrantChange {
        version: 1,
        grants: vec![route_export(
            "node-a",
            "node-a",
            "10.0.0.11",
            "/mnt/nfs1",
            true,
        )],
    })
    .expect("encode first frame");
    source
        .on_control_frame(&[first])
        .await
        .expect("apply first frame");

    let stale = encode_runtime_host_grant_change(&RuntimeHostGrantChange {
        version: 1,
        grants: vec![route_export(
            "node-b",
            "node-b",
            "10.0.0.12",
            "/mnt/nfs2",
            true,
        )],
    })
    .expect("encode stale frame");
    source
        .on_control_frame(&[stale])
        .await
        .expect("stale frame should be ignored");

    assert_eq!(source.host_object_grants_version.load(Ordering::Relaxed), 1);
    let grants = lock_or_recover(
        &source.state_cell.host_object_grants,
        "test.host_object_grants",
    )
    .clone();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].object_ref, "node-a");
    assert_eq!(grants[0].mount_point, std::path::PathBuf::from("/mnt/nfs1"));
}

#[tokio::test]
async fn unit_tick_control_frame_is_accepted() {
    let source = build_source(vec![]);
    let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
        route_key: ROUTE_KEY_QUERY.to_string(),
        unit_id: "runtime.exec.scan".to_string(),
        generation: 1,
        at_ms: 1,
    })
    .expect("encode unit tick");

    source
        .on_control_frame(&[envelope])
        .await
        .expect("source should accept unit tick control frame");
}

#[tokio::test]
async fn unit_tick_with_unknown_unit_id_is_rejected() {
    let source = build_source(vec![]);
    let envelope = encode_runtime_unit_tick(&RuntimeUnitTick {
        route_key: ROUTE_KEY_QUERY.to_string(),
        unit_id: "runtime.exec.unknown".to_string(),
        generation: 1,
        at_ms: 1,
    })
    .expect("encode unit tick");

    let err = source
        .on_control_frame(&[envelope])
        .await
        .expect_err("unknown unit id must be rejected");
    assert!(matches!(err, CnxError::NotSupported(_)));
    assert!(err.to_string().contains("unsupported unit_id"));
}

#[tokio::test]
async fn exec_activate_with_unknown_unit_id_is_rejected() {
    let source = build_source(vec![]);
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

    let err = source
        .on_control_frame(&[envelope])
        .await
        .expect_err("unknown unit id must be rejected");
    assert!(matches!(err, CnxError::NotSupported(_)));
    assert!(err.to_string().contains("unsupported unit_id"));
}

#[tokio::test]
async fn stale_deactivate_generation_is_ignored() {
    let source = build_source(vec![]);
    let activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate");
    source
        .on_control_frame(&[activate])
        .await
        .expect("activate should pass");

    let stale_deactivate =
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 4,
            reason: "test".to_string(),
        }))
        .expect("encode deactivate");
    source
        .on_control_frame(&[stale_deactivate])
        .await
        .expect("stale deactivate should be ignored");

    let state = source.unit_control.snapshot("runtime.exec.scan");
    assert_eq!(state, Some((5, true)));
}

#[tokio::test]
async fn stale_activate_generation_is_ignored() {
    let source = build_source(vec![]);
    let activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate");
    source
        .on_control_frame(&[activate])
        .await
        .expect("activate should pass");

    let deactivate =
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 5,
            reason: "test".to_string(),
        }))
        .expect("encode deactivate");
    source
        .on_control_frame(&[deactivate])
        .await
        .expect("deactivate should pass");

    let stale_activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 4,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode stale activate");
    source
        .on_control_frame(&[stale_activate])
        .await
        .expect("stale activate should be ignored");

    let state = source.unit_control.snapshot("runtime.exec.scan");
    assert_eq!(state, Some((5, false)));
}

#[test]
fn build_root_runtimes_treats_local_object_ref_as_local_host() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::nfs1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtimes = lock_or_recover(&source.state_cell.roots, "test.local_composite_member").clone();
    assert_eq!(runtimes.len(), 1);
    assert_eq!(runtimes[0].object_ref, "node-a::nfs1");
}

#[test]
fn build_root_runtimes_marks_only_group_primary_for_periodic_loop() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![
        test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-a::exp2", "node-a", "10.0.0.12", "/mnt/nfs1", true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtimes = lock_or_recover(&source.state_cell.roots, "test.group_primary_flags").clone();
    assert_eq!(runtimes.len(), 2);
    let primary = runtimes
        .iter()
        .find(|root| root.is_group_primary)
        .expect("one runtime must be selected as group primary");
    assert_eq!(primary.object_ref, "node-a::exp1");
    let non_primary = runtimes
        .iter()
        .find(|root| !root.is_group_primary)
        .expect("one runtime must be non-primary");
    assert_eq!(non_primary.object_ref, "node-a::exp2");
}

#[test]
fn build_root_runtimes_only_instantiates_local_members() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![
        test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-b::exp2", "node-b", "10.0.0.12", "/mnt/nfs1", true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtimes = lock_or_recover(&source.state_cell.roots, "test.local_member_filter").clone();
    assert_eq!(runtimes.len(), 1);
    assert_eq!(runtimes[0].object_ref, "node-a::exp1");
    assert!(runtimes[0].is_group_primary);
}

#[test]
fn build_root_runtimes_treats_instance_suffixed_node_id_as_local_member() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = vec![
        test_export("node-b::nfs1", "node-b", "10.0.0.21", "/mnt/nfs1", true),
        test_export("node-b::nfs2", "node-b", "10.0.0.22", "/mnt/nfs2", true),
    ];

    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-b-29775277610492238759985153".to_string()),
        None,
    )
    .expect("init source");
    let runtimes = lock_or_recover(
        &source.state_cell.roots,
        "test.instance_suffix_local_member",
    )
    .clone();
    assert_eq!(
        runtimes
            .iter()
            .map(|root| root.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b::nfs1", "node-b::nfs2"],
        "instance-suffixed worker node ids must still match bare host_ref grants for local source scheduling"
    );
}

#[test]
fn build_root_runtimes_treats_cluster_scoped_node_id_as_local_member() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = vec![
        test_export("node-b::nfs1", "node-b", "10.0.0.21", "/mnt/nfs1", true),
        test_export("node-b::nfs2", "node-b", "10.0.0.22", "/mnt/nfs2", true),
    ];

    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("cluster-node-b-29775277610492238759985153".to_string()),
        None,
    )
    .expect("init source");
    let runtimes =
        lock_or_recover(&source.state_cell.roots, "test.cluster_scope_local_member").clone();
    assert_eq!(
        runtimes
            .iter()
            .map(|root| root.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b::nfs1", "node-b::nfs2"],
        "cluster-scoped runtime node ids must still match bare host_ref grants for local source scheduling"
    );
}

#[tokio::test]
async fn scheduled_groups_publish_only_runnable_local_roots_after_mixed_scope_activate() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = vec![
        test_export("node-b::nfs1", "node-b", "10.0.0.21", "/mnt/nfs1", true),
        test_export("node-d::nfs2", "node-d", "10.0.0.41", "/mnt/nfs2", true),
    ];
    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-b-29775332298115179545100289".to_string()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init source");

    let envelopes: Vec<ControlEnvelope> = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["node-d::nfs2".to_string()],
                },
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_RESCAN.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-b::nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["node-d::nfs2".to_string()],
                },
            ],
        }))
        .expect("encode scan activate"),
    ];

    source
        .on_control_frame(&envelopes)
        .await
        .expect("apply mixed local+remote control");

    assert_eq!(
        source.scheduled_source_group_ids().expect("source groups"),
        Some(BTreeSet::from(["nfs1".to_string()])),
        "scheduled source groups must reflect runnable local roots, not every activated remote scope",
    );
    assert_eq!(
        source.scheduled_scan_group_ids().expect("scan groups"),
        Some(BTreeSet::from(["nfs1".to_string()])),
        "scheduled scan groups must reflect runnable local roots, not every activated remote scope",
    );
}

#[tokio::test]
async fn scheduled_groups_with_bare_scope_ids_only_publish_local_granted_roots_under_mixed_cluster_grants()
 {
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";
    let node_id = NodeId("node-a-29799407896396737569357825".to_string());

    let cfg = SourceConfig {
        roots: vec![
            RootSpec {
                id: "nfs1".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs1_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
            RootSpec {
                id: "nfs2".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs2_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
            RootSpec {
                id: "nfs3".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs3_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: true,
                scan: true,
                audit_interval_ms: None,
            },
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg, node_id.clone(), Some(Arc::new(NoopBoundary)))
        .expect("init mixed-grant runtime-managed source");

    let control = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                RuntimeHostGrant {
                    object_ref: format!("{}::nfs1", node_id.0),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: node_id.0.clone(),
                        host_ip: "10.0.0.11".to_string(),
                        host_name: Some("node-a".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-a/nfs1".to_string(),
                        fs_source: nfs1_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: format!("{}::nfs2", node_id.0),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: node_id.0.clone(),
                        host_ip: "10.0.0.12".to_string(),
                        host_name: Some("node-a".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-a/nfs2".to_string(),
                        fs_source: nfs2_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-b-29799407896396737569357825::nfs1".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-b-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.21".to_string(),
                        host_name: Some("node-b".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-b/nfs1".to_string(),
                        fs_source: nfs1_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-c-29799407896396737569357825::nfs1".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-c-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.31".to_string(),
                        host_name: Some("node-c".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-c/nfs1".to_string(),
                        fs_source: nfs1_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-c-29799407896396737569357825::nfs2".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-c-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.32".to_string(),
                        host_name: Some("node-c".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-c/nfs2".to_string(),
                        fs_source: nfs2_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-d-29799407896396737569357825::nfs2".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-d-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.41".to_string(),
                        host_name: Some("node-d".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-d/nfs2".to_string(),
                        fs_source: nfs2_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-b-29799407896396737569357825::nfs3".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-b-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.23".to_string(),
                        host_name: Some("node-b".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-b/nfs3".to_string(),
                        fs_source: nfs3_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-d-29799407896396737569357825::nfs3".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-d-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.43".to_string(),
                        host_name: Some("node-d".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-d/nfs3".to_string(),
                        fs_source: nfs3_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
                RuntimeHostGrant {
                    object_ref: "node-e-29799407896396737569357825::nfs3".to_string(),
                    object_type: RuntimeHostObjectType::MountRoot,
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    host: RuntimeHostDescriptor {
                        host_ref: "node-e-29799407896396737569357825".to_string(),
                        host_ip: "10.0.0.53".to_string(),
                        host_name: Some("node-e".to_string()),
                        site: None,
                        zone: None,
                        host_labels: Default::default(),
                    },
                    object: RuntimeObjectDescriptor {
                        mount_point: "/mnt/node-e/nfs3".to_string(),
                        fs_source: nfs3_source.to_string(),
                        fs_type: "nfs".to_string(),
                        mount_options: Vec::new(),
                    },
                    grant_state: RuntimeHostGrantState::Active,
                },
            ],
        })
        .expect("encode host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["nfs2".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs3".to_string(),
                    resource_ids: vec!["nfs3".to_string()],
                },
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_RESCAN.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["nfs2".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs3".to_string(),
                    resource_ids: vec!["nfs3".to_string()],
                },
            ],
        }))
        .expect("encode scan activate"),
    ];

    source
        .on_control_frame(&control)
        .await
        .expect("apply mixed-grant logical-id control");

    assert_eq!(
        source.scheduled_source_group_ids().expect("source groups"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
        "logical-id scopes must only schedule local granted source groups when cluster grants already identify the runnable members",
    );
    assert_eq!(
        source.scheduled_scan_group_ids().expect("scan groups"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
        "logical-id scopes must only schedule local granted scan groups when cluster grants already identify the runnable members",
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let local_roots = lock_or_recover(
            &source.state_cell.roots,
            "test.mixed_grant_bare_scope_ids.local_roots",
        )
        .clone();
        let local_root_ids = local_roots
            .iter()
            .map(|root| root.logical_root_id.as_str())
            .collect::<Vec<_>>();
        if local_root_ids == vec!["nfs1", "nfs2"] {
            break;
        }
        let status = source.status_snapshot();
        assert!(
            tokio::time::Instant::now() < deadline,
            "mixed cluster grants plus bare logical scopes must eventually materialize only local nfs1/nfs2 runtimes on the elected node-a worker: local_roots={local_root_ids:?} concrete_roots={:?}",
            status
                .concrete_roots
                .iter()
                .map(|root| (
                    root.logical_root_id.clone(),
                    root.object_ref.clone(),
                    root.is_group_primary,
                    root.status.clone(),
                ))
                .collect::<Vec<_>>(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn runtime_managed_schedule_preserves_non_empty_scopes_across_empty_followup_activate() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")];
    cfg.host_object_grants = Vec::new();
    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-c-29776082501061843024347137".to_string()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init runtime-managed source");

    let initial = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                route_export("node-c::nfs1", "node-c", "10.0.0.31", "/mnt/nfs1", true),
                route_export("node-c::nfs2", "node-c", "10.0.0.32", "/mnt/nfs2", true),
            ],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-c::nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["node-c::nfs2".to_string()],
                },
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_RESCAN.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                RuntimeBoundScope {
                    scope_id: "nfs1".to_string(),
                    resource_ids: vec!["node-c::nfs1".to_string()],
                },
                RuntimeBoundScope {
                    scope_id: "nfs2".to_string(),
                    resource_ids: vec!["node-c::nfs2".to_string()],
                },
            ],
        }))
        .expect("encode scan activate"),
    ];
    source
        .on_control_frame(&initial)
        .await
        .expect("apply initial runtime-managed source wave");
    assert_eq!(
        source
            .scheduled_source_group_ids()
            .expect("source groups after initial wave"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );
    assert_eq!(
        source
            .scheduled_scan_group_ids()
            .expect("scan groups after initial wave"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );

    let followup = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode empty source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: METHOD_SOURCE_RESCAN.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: Vec::new(),
        }))
        .expect("encode empty scan activate"),
    ];
    source
        .on_control_frame(&followup)
        .await
        .expect("apply followup empty-scope activate wave");

    assert_eq!(
        source
            .scheduled_source_group_ids()
            .expect("source groups after empty-scope followup"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
        "runtime-managed source scheduling must preserve the last non-empty active scopes across followup activate rows that omit bound scopes",
    );
    assert_eq!(
        source
            .scheduled_scan_group_ids()
            .expect("scan groups after empty-scope followup"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
        "runtime-managed scan scheduling must preserve the last non-empty active scopes across followup activate rows that omit bound scopes",
    );
}

#[tokio::test]
async fn runtime_managed_local_resource_ids_without_grant_change_build_primary_scan_roots_for_watch_scan_refresh()
 {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!(
        "fs-meta-runtime-managed-watch-scan-zero-grant-{unique}"
    ));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", nfs1.clone()),
        RootSpec::new("nfs2", nfs2.clone()),
    ];
    cfg.host_object_grants = Vec::new();
    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-c-29776225407437800789245953".to_string()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init runtime-managed source");

    source
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_RESCAN.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply runtime-managed watch-scan activate without host grant change");

    let expected_groups = BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let expected_primary_scan_roots = BTreeSet::from([
        ("nfs1".to_string(), "nfs1".to_string(), nfs1.clone()),
        ("nfs2".to_string(), "nfs2".to_string(), nfs2.clone()),
    ]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_active = match source
            .unit_control
            .unit_state(SOURCE_RUNTIME_UNIT_ID)
            .expect("source unit state")
        {
            Some((true, rows)) => rows
                .into_iter()
                .map(|row| row.scope_id)
                .collect::<BTreeSet<_>>(),
            Some((false, _)) | None => BTreeSet::new(),
        };
        let scan_active = match source
            .unit_control
            .unit_state(SOURCE_SCAN_RUNTIME_UNIT_ID)
            .expect("scan unit state")
        {
            Some((true, rows)) => rows
                .into_iter()
                .map(|row| row.scope_id)
                .collect::<BTreeSet<_>>(),
            Some((false, _)) | None => BTreeSet::new(),
        };
        let primary_scan_roots = lock_or_recover(
                &source.state_cell.roots,
                "test.runtime_managed_local_resource_ids_without_grant_change_build_primary_scan_roots.roots",
            )
            .iter()
            .filter(|root| root.is_group_primary && root.spec.scan)
            .map(|root| {
                (
                    root.logical_root_id.clone(),
                    root.object_ref.clone(),
                    root.monitor_path.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        if source_active == expected_groups
            && scan_active == expected_groups
            && primary_scan_roots == expected_primary_scan_roots
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "runtime-managed zero-grant watch-scan activate should still build local primary scan roots from active local resource ids once unit scopes converge: source={source_active:?} scan={scan_active:?} primary_scan_roots={primary_scan_roots:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    source.close().await.expect("close source");
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn runtime_managed_source_logical_root_update_with_empty_active_scopes_uses_local_grants() {
    let mut cfg = SourceConfig::default();
    cfg.roots = Vec::new();
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.1", "/mnt/nfs1", true),
        test_export("node-b::nfs2", "node-b", "10.0.0.2", "/mnt/nfs2", true),
    ];
    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-a".to_string()),
        Some(Arc::new(NoopBoundary)),
    )
    .expect("init runtime-managed source");

    source
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_ROOTS_CONTROL.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: METHOD_SOURCE_RESCAN.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: Vec::new(),
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply empty-scope activate wave");

    source
        .update_logical_roots(vec![root("nfs1", "/mnt/nfs1"), root("nfs2", "/mnt/nfs2")])
        .await
        .expect("update logical roots");

    assert_eq!(
        source
            .scheduled_source_group_ids()
            .expect("source groups after logical root update"),
        Some(BTreeSet::from(["nfs1".to_string()])),
        "runtime-managed logical root updates must preserve bare active scopes so local grants can schedule the elected source group",
    );
    assert_eq!(
        source
            .scheduled_scan_group_ids()
            .expect("scan groups after logical root update"),
        Some(BTreeSet::from(["nfs1".to_string()])),
        "runtime-managed logical root updates must preserve bare active scopes so local grants can schedule the elected scan group",
    );
}
#[tokio::test]
async fn manual_rescan_publishes_baseline_for_each_split_primary_under_mixed_cluster_grants() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let node_a_nfs1 = tmp.path().join("node-a-nfs1");
    let node_a_nfs2 = tmp.path().join("node-a-nfs2");
    let node_b_nfs1 = tmp.path().join("node-b-nfs1");
    let node_c_nfs1 = tmp.path().join("node-c-nfs1");
    let node_c_nfs2 = tmp.path().join("node-c-nfs2");
    let node_d_nfs2 = tmp.path().join("node-d-nfs2");
    let node_b_nfs3 = tmp.path().join("node-b-nfs3");
    let node_d_nfs3 = tmp.path().join("node-d-nfs3");
    let node_e_nfs3 = tmp.path().join("node-e-nfs3");
    for path in [
        &node_a_nfs1,
        &node_a_nfs2,
        &node_b_nfs1,
        &node_c_nfs1,
        &node_c_nfs2,
        &node_d_nfs2,
        &node_b_nfs3,
        &node_d_nfs3,
        &node_e_nfs3,
    ] {
        std::fs::create_dir_all(path.join("data")).expect("create mount data dir");
    }
    std::fs::write(node_a_nfs1.join("data").join("a.txt"), b"a").expect("seed node-a nfs1");
    std::fs::write(node_a_nfs2.join("data").join("b.txt"), b"b").expect("seed node-a nfs2");
    std::fs::write(node_b_nfs3.join("data").join("c.txt"), b"c").expect("seed node-b nfs3");

    let node_a_id = "node-a-29799407896396737569357825";
    let node_b_id = "node-b-29799407896396737569357825";
    let node_c_id = "node-c-29799407896396737569357825";
    let node_d_id = "node-d-29799407896396737569357825";
    let node_e_id = "node-e-29799407896396737569357825";
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";

    let grant = |object_ref: String,
                 host_ref: &str,
                 host_ip: &str,
                 mount_point: std::path::PathBuf,
                 fs_source: &str|
     -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref,
            host_ref: host_ref.to_string(),
            host_ip: host_ip.to_string(),
            host_name: Some(host_ref.to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point,
            fs_source: fs_source.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active: true,
        }
    };
    let cfg = SourceConfig {
        roots: vec![
            RootSpec {
                id: "nfs1".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs1_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: false,
                scan: true,
                audit_interval_ms: None,
            },
            RootSpec {
                id: "nfs2".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs2_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: false,
                scan: true,
                audit_interval_ms: None,
            },
            RootSpec {
                id: "nfs3".to_string(),
                selector: crate::source::config::RootSelector {
                    mount_point: None,
                    fs_source: Some(nfs3_source.to_string()),
                    fs_type: None,
                    host_ip: None,
                    host_ref: None,
                },
                subpath_scope: std::path::PathBuf::from("/"),
                watch: false,
                scan: true,
                audit_interval_ms: None,
            },
        ],
        host_object_grants: vec![
            grant(
                format!("{node_a_id}::nfs1"),
                node_a_id,
                "10.0.0.11",
                node_a_nfs1.clone(),
                nfs1_source,
            ),
            grant(
                format!("{node_a_id}::nfs2"),
                node_a_id,
                "10.0.0.12",
                node_a_nfs2.clone(),
                nfs2_source,
            ),
            grant(
                format!("{node_b_id}::nfs1"),
                node_b_id,
                "10.0.0.21",
                node_b_nfs1.clone(),
                nfs1_source,
            ),
            grant(
                format!("{node_c_id}::nfs1"),
                node_c_id,
                "10.0.0.31",
                node_c_nfs1.clone(),
                nfs1_source,
            ),
            grant(
                format!("{node_c_id}::nfs2"),
                node_c_id,
                "10.0.0.32",
                node_c_nfs2.clone(),
                nfs2_source,
            ),
            grant(
                format!("{node_d_id}::nfs2"),
                node_d_id,
                "10.0.0.41",
                node_d_nfs2.clone(),
                nfs2_source,
            ),
            grant(
                format!("{node_b_id}::nfs3"),
                node_b_id,
                "10.0.0.23",
                node_b_nfs3.clone(),
                nfs3_source,
            ),
            grant(
                format!("{node_d_id}::nfs3"),
                node_d_id,
                "10.0.0.43",
                node_d_nfs3.clone(),
                nfs3_source,
            ),
            grant(
                format!("{node_e_id}::nfs3"),
                node_e_id,
                "10.0.0.53",
                node_e_nfs3.clone(),
                nfs3_source,
            ),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg, NodeId(node_a_id.to_string()), None)
        .expect("init mixed-cluster source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs3".to_string(),
                        resource_ids: vec!["nfs3".to_string()],
                    },
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs3".to_string(),
                        resource_ids: vec!["nfs3".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs3".to_string(),
                        resource_ids: vec!["nfs3".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs3".to_string(),
                        resource_ids: vec!["nfs3".to_string()],
                    },
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    source
        .on_control_frame(&source_wave(2))
        .await
        .expect("mixed-cluster source wave should succeed");

    let expected_primaries = std::collections::BTreeMap::from([
        ("nfs1".to_string(), format!("{node_a_id}::nfs1")),
        ("nfs2".to_string(), format!("{node_a_id}::nfs2")),
        ("nfs3".to_string(), format!("{node_b_id}::nfs3")),
    ]);
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if source.source_primary_by_group_snapshot() == expected_primaries {
            break;
        }
        assert!(
            tokio::time::Instant::now() < ready_deadline,
            "mixed-cluster source should elect split primaries before manual rescan"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < drain_deadline {
        let Ok(next) = tokio::time::timeout(Duration::from_millis(50), stream.next()).await else {
            continue;
        };
        if next.is_none() {
            break;
        }
    }

    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan signal");

    let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < republish_deadline {
        let Ok(next) = tokio::time::timeout(Duration::from_millis(250), stream.next()).await else {
            continue;
        };
        let Some(batch) = next else {
            continue;
        };
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<crate::FileMetaRecord>(event.payload_bytes())
            else {
                continue;
            };
            if is_under_query_path(&record.path, b"/data") {
                *baseline_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
            }
        }
        if ["nfs1", "nfs2"].iter().all(|group_id| {
            baseline_counts
                .get(&format!("{node_a_id}::{group_id}"))
                .copied()
                .unwrap_or(0)
                > 0
        }) {
            break;
        }
    }

    source.close().await.expect("close source");

    assert!(
        baseline_counts
            .get(&format!("{node_a_id}::nfs1"))
            .copied()
            .unwrap_or(0)
            > 0
            && baseline_counts
                .get(&format!("{node_a_id}::nfs2"))
                .copied()
                .unwrap_or(0)
                > 0,
        "manual rescan should publish baseline /data for node-a split-primary roots under mixed cluster grants: {baseline_counts:?}",
    );
}

#[tokio::test]
async fn runtime_managed_watch_scan_zero_grant_trigger_rescan_when_ready_publishes_baseline_data() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!(
        "fs-meta-runtime-managed-watch-scan-publish-{unique}"
    ));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
    std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
    std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", nfs1.clone()),
        RootSpec::new("nfs2", nfs2.clone()),
    ];
    cfg.host_object_grants = Vec::new();
    let source =
        FSMetaSource::with_boundaries(cfg, NodeId("node-c-local-sink-status-helper".into()), None)
            .expect("init runtime-managed source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    source
        .on_control_frame(&source_wave(2))
        .await
        .expect("apply runtime-managed watch-scan activate without host grant change");
    source.trigger_rescan_when_ready().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut counts = BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
        let Ok(Some(batch)) = next else {
            continue;
        };
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<crate::FileMetaRecord>(event.payload_bytes())
            else {
                continue;
            };
            if is_under_query_path(&record.path, b"/data") {
                *counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
            }
        }
        if counts.len() >= 2 {
            break;
        }
    }

    source.close().await.expect("close source");
    let _ = std::fs::remove_dir_all(base);

    assert!(
        counts.len() >= 2,
        "runtime-managed zero-grant watch-scan trigger_rescan_when_ready must publish baseline /data for both local roots after runtime-managed activate scopes converge: {counts:?}",
    );
}

#[tokio::test]
async fn runtime_managed_watch_scan_zero_grant_republishes_baseline_after_cleanup_tail() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!(
        "fs-meta-runtime-managed-watch-scan-republish-{unique}"
    ));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
    std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
    std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs1", nfs1.clone()),
        RootSpec::new("nfs2", nfs2.clone()),
    ];
    cfg.host_object_grants = Vec::new();
    let source = FSMetaSource::with_boundaries(
        cfg,
        NodeId("node-c-peer-cleanup-tail-sink-status".into()),
        None,
    )
    .expect("init runtime-managed source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    RuntimeBoundScope {
                        scope_id: "nfs1".to_string(),
                        resource_ids: vec!["nfs1".to_string()],
                    },
                    RuntimeBoundScope {
                        scope_id: "nfs2".to_string(),
                        resource_ids: vec!["nfs2".to_string()],
                    },
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let cleanup_tail = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 3,
            reason: "restart_deferred_retire_pending".to_string(),
        }))
        .expect("encode source roots deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 3,
            reason: "restart_deferred_retire_pending".to_string(),
        }))
        .expect("encode source rescan-control deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 3,
            reason: "restart_deferred_retire_pending".to_string(),
        }))
        .expect("encode source rescan deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 3,
            reason: "restart_deferred_retire_pending".to_string(),
        }))
        .expect("encode source scan deactivate"),
    ];

    source
        .on_control_frame(&source_wave(2))
        .await
        .expect("apply initial runtime-managed watch-scan activate");
    assert_eq!(
        source
            .scheduled_source_group_ids()
            .expect("source groups after initial activate"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );
    assert_eq!(
        source
            .scheduled_scan_group_ids()
            .expect("scan groups after initial activate"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );
    source.trigger_rescan_when_ready().await;

    let baseline_target = b"/data";
    let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut baseline_counts = BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < baseline_deadline {
        let next = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
        let Ok(Some(batch)) = next else {
            continue;
        };
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<crate::FileMetaRecord>(event.payload_bytes())
            else {
                continue;
            };
            if is_under_query_path(&record.path, baseline_target) {
                *baseline_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
            }
        }
        if baseline_counts.len() >= 2 {
            break;
        }
    }
    assert!(
        baseline_counts.len() >= 2,
        "initial runtime-managed watch-scan trigger_rescan_when_ready must publish baseline /data for both local roots before cleanup tail: {baseline_counts:?}",
    );
    while tokio::time::timeout(Duration::from_millis(50), stream.next())
        .await
        .ok()
        .flatten()
        .is_some()
    {}

    source
        .on_control_frame(&cleanup_tail)
        .await
        .expect("apply cleanup-only watch-scan tail");
    source
        .on_control_frame(&source_wave(4))
        .await
        .expect("apply later runtime-managed watch-scan recovery");
    assert_eq!(
        source
            .scheduled_source_group_ids()
            .expect("source groups after later recovery"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );
    assert_eq!(
        source
            .scheduled_scan_group_ids()
            .expect("scan groups after later recovery"),
        Some(BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])),
    );
    source.trigger_rescan_when_ready().await;

    let republish_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut republish_counts = BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < republish_deadline {
        let next = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
        let Ok(Some(batch)) = next else {
            continue;
        };
        for event in batch {
            let Ok(record) = rmp_serde::from_slice::<crate::FileMetaRecord>(event.payload_bytes())
            else {
                continue;
            };
            if is_under_query_path(&record.path, baseline_target) {
                *republish_counts
                    .entry(event.metadata().origin_id.0.clone())
                    .or_default() += 1;
            }
        }
        if republish_counts.len() >= 2 {
            break;
        }
    }

    source.close().await.expect("close source");
    let _ = std::fs::remove_dir_all(base);

    assert!(
        republish_counts.len() >= 2,
        "later runtime-managed watch-scan recovery must republish baseline /data for both local roots after cleanup tail instead of leaving post-recovery publishes empty: baseline_counts={baseline_counts:?} republish_counts={republish_counts:?}",
    );
}

#[test]
fn logical_root_fanout_can_group_by_host_ip_descriptor() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec {
            id: "east".to_string(),
            selector: crate::source::config::RootSelector {
                mount_point: Some(std::path::PathBuf::from("/mnt/shared")),
                host_ip: Some("10.0.0.11".to_string()),
                ..Default::default()
            },
            subpath_scope: std::path::PathBuf::from("/"),
            watch: true,
            scan: true,
            audit_interval_ms: None,
        },
        RootSpec {
            id: "west".to_string(),
            selector: crate::source::config::RootSelector {
                mount_point: Some(std::path::PathBuf::from("/mnt/shared")),
                host_ip: Some("10.0.0.12".to_string()),
                ..Default::default()
            },
            subpath_scope: std::path::PathBuf::from("/"),
            watch: true,
            scan: true,
            audit_interval_ms: None,
        },
    ];
    cfg.host_object_grants = vec![
        test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/shared", true),
        test_export("node-b::exp2", "node-b", "10.0.0.12", "/mnt/shared", true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let fanout = source.logical_root_fanout_snapshot();
    assert_eq!(fanout.get("east").map(|rows| rows.len()), Some(1));
    assert_eq!(fanout.get("west").map(|rows| rows.len()), Some(1));
    assert_eq!(
        fanout
            .get("east")
            .and_then(|rows| rows.first())
            .map(|row| row.object_ref.as_str()),
        Some("node-a::exp1")
    );
    assert_eq!(
        fanout
            .get("west")
            .and_then(|rows| rows.first())
            .map(|row| row.object_ref.as_str()),
        Some("node-b::exp2")
    );
}

#[test]
fn trigger_rescan_targets_group_primary_only() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![
        test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-a::exp2", "node-a", "10.0.0.12", "/mnt/nfs1", true),
    ];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtimes =
        lock_or_recover(&source.state_cell.roots, "test.trigger_rescan_primary_only").clone();
    let mut primary_rx = None;
    let mut non_primary_rx = None;
    for root in runtimes {
        let rx = root.rescan_tx.subscribe();
        if root.is_group_primary {
            primary_rx = Some(rx);
        } else {
            non_primary_rx = Some(rx);
        }
    }

    source.trigger_rescan();

    let primary_reason = primary_rx
        .expect("primary receiver")
        .try_recv()
        .expect("primary should receive manual rescan");
    assert!(matches!(primary_reason, RescanReason::Manual));
    assert!(matches!(
        non_primary_rx.expect("non-primary receiver").try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn manual_rescan_requests_are_coalesced_while_one_is_pending() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let primary_root = lock_or_recover(
        &source.state_cell.roots,
        "test.manual_rescan_requests_are_coalesced.roots",
    )
    .iter()
    .find(|root| root.is_group_primary)
    .cloned()
    .expect("primary root exists");
    let root_key = FSMetaSource::root_runtime_key(&primary_root);
    let mut rx = primary_root.rescan_tx.subscribe();

    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        Some(&source.state_cell.manual_rescan_intents),
        "manual",
    );
    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        Some(&source.state_cell.manual_rescan_intents),
        "manual",
    );

    assert!(matches!(
        rx.try_recv().expect("first signal should be sent"),
        RescanReason::Manual
    ));
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    {
        let mut intents = lock_or_recover(
            &source.state_cell.manual_rescan_intents,
            "test.manual_rescan_requests_are_coalesced.intents",
        );
        let entry = intents.get_mut(&root_key).expect("intent exists");
        assert_eq!(entry.requested, 2);
        assert_eq!(entry.completed, 0);
        entry.completed = entry.requested;
    }

    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        Some(&source.state_cell.manual_rescan_intents),
        "manual",
    );
    assert!(matches!(
        rx.try_recv()
            .expect("next signal should be re-armed after completion"),
        RescanReason::Manual
    ));
}

#[test]
fn topology_rescan_is_not_coalesced_behind_pending_manual_rescan() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let primary_root = lock_or_recover(
        &source.state_cell.roots,
        "test.topology_rescan_not_coalesced.roots",
    )
    .iter()
    .find(|root| root.is_group_primary)
    .cloned()
    .expect("primary root exists");
    let mut rx = primary_root.rescan_tx.subscribe();

    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        Some(&source.state_cell.manual_rescan_intents),
        "manual",
    );
    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        Some(&source.state_cell.manual_rescan_intents),
        "manual",
    );
    FSMetaSource::request_rescan_on_primary_roots(
        std::slice::from_ref(&primary_root),
        None,
        None,
        "topology",
    );

    assert!(matches!(
        rx.try_recv().expect("manual signal should be sent"),
        RescanReason::Manual
    ));
    assert!(matches!(
        rx.try_recv()
            .expect("topology signal must not be suppressed by pending manual intent"),
        RescanReason::Manual
    ));
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn source_primary_snapshot_reports_cluster_primary_assignment() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![
        test_export("node-a::exp1", "node-a", "10.0.0.11", "/mnt/nfs1", true),
        test_export("node-z::exp2", "node-z", "10.0.0.12", "/mnt/nfs1", true),
    ];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-z".to_string()), None)
        .expect("init source");

    let local_roots = lock_or_recover(
        &source.state_cell.roots,
        "test.source_primary_snapshot.local_roots",
    )
    .clone();
    assert_eq!(
        local_roots.len(),
        1,
        "local node should only materialize local member"
    );
    assert!(
        !local_roots[0].is_group_primary,
        "local member is not the lexical cluster primary in this setup"
    );

    let snapshot = source.source_primary_by_group_snapshot();
    assert_eq!(
        snapshot.get("nfs1").map(String::as_str),
        Some("node-a::exp1"),
        "snapshot must report authoritative cluster primary, not local-only primary state"
    );
}

#[tokio::test]
async fn trigger_rescan_when_ready_waits_for_primary_root_running() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let primary_root = lock_or_recover(
        &source.state_cell.roots,
        "test.trigger_rescan_when_ready.roots",
    )
    .iter()
    .find(|root| root.is_group_primary)
    .cloned()
    .expect("primary root exists");
    let root_key = FSMetaSource::root_runtime_key(&primary_root);
    let mut rx = primary_root.rescan_tx.subscribe();
    let fanout_health = source.state_cell.fanout_health.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(75)).await;
        lock_or_recover(&fanout_health, "test.trigger_rescan_when_ready.health")
            .object_ref
            .insert(root_key, "running".to_string());
    });

    let started = tokio::time::Instant::now();
    source.trigger_rescan_when_ready().await;
    assert!(started.elapsed() >= Duration::from_millis(75));
    let reason = rx.try_recv().expect("primary should receive manual rescan");
    assert!(matches!(reason, RescanReason::Manual));
}

#[tokio::test]
async fn trigger_rescan_when_ready_does_not_fire_before_primary_root_running() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let primary_root = lock_or_recover(
        &source.state_cell.roots,
        "test.trigger_rescan_when_ready_timeout.roots",
    )
    .iter()
    .find(|root| root.is_group_primary)
    .cloned()
    .expect("primary root exists");
    let mut rx = primary_root.rescan_tx.subscribe();

    tokio::time::timeout(Duration::from_secs(6), source.trigger_rescan_when_ready())
        .await
        .expect(
            "trigger_rescan_when_ready should settle even if primary root never becomes running",
        );

    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "trigger_rescan_when_ready must not fire a manual rescan before the primary scan root is running"
    );
}

#[test]
fn root_task_signature_ignores_group_primary_flag_changes() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtime = lock_or_recover(&source.state_cell.roots, "test.root_task_signature")
        .first()
        .cloned()
        .expect("runtime exists");
    let mut flipped = runtime.clone();
    flipped.is_group_primary = !runtime.is_group_primary;

    let before = FSMetaSource::root_task_signature(&runtime);
    let after = FSMetaSource::root_task_signature(&flipped);
    assert_eq!(before, after);
}

#[test]
fn status_snapshot_tracks_same_key_candidate_handoff() {
    let mut cfg = SourceConfig::default();
    cfg.roots = vec![root("nfs1", "/mnt/nfs1")];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let runtime = lock_or_recover(&source.state_cell.roots, "test.status_snapshot_handoff")
        .first()
        .cloned()
        .expect("runtime exists");
    let root_key = FSMetaSource::root_runtime_key(&runtime);
    let health = source.state_cell.fanout_health_handle();

    FSMetaSource::update_root_task_slot_health(
        &health,
        &root_key,
        1,
        7,
        RootTaskRole::Active,
        "running",
    );
    FSMetaSource::update_root_task_slot_health(
        &health,
        &root_key,
        2,
        8,
        RootTaskRole::Candidate,
        "warming",
    );

    let snapshot = source.status_snapshot();
    let entry = snapshot
        .concrete_roots
        .iter()
        .find(|entry| entry.root_key == root_key)
        .expect("concrete root health exists");
    assert_eq!(entry.current_revision, Some(1));
    assert_eq!(entry.current_stream_generation, Some(7));
    assert_eq!(entry.candidate_revision, Some(2));
    assert_eq!(entry.candidate_stream_generation, Some(8));
    assert_eq!(entry.candidate_status.as_deref(), Some("warming"));

    FSMetaSource::promote_root_task_candidate_health(&health, &root_key, 2, 8);
    FSMetaSource::mark_root_task_draining(&health, &root_key, 1, 7, "draining");
    FSMetaSource::finish_root_task_draining(&health, &root_key, 1, 7, true);

    let snapshot = source.status_snapshot();
    let entry = snapshot
        .concrete_roots
        .iter()
        .find(|entry| entry.root_key == root_key)
        .expect("concrete root health exists after promote");
    assert_eq!(entry.current_revision, Some(2));
    assert_eq!(entry.current_stream_generation, Some(8));
    assert_eq!(entry.candidate_revision, None);
    assert_eq!(entry.candidate_stream_generation, None);
    assert_eq!(entry.draining_revision, Some(1));
    assert_eq!(entry.draining_stream_generation, Some(7));
    assert_eq!(entry.draining_status.as_deref(), Some("retired"));
}

#[tokio::test]
async fn wait_for_task_handles_ready_blocks_until_all_receivers_flip() {
    let (tx_a, rx_a) = tokio::sync::watch::channel(false);
    let (tx_b, rx_b) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = tx_a.send(true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = tx_b.send(true);
    });
    let started = tokio::time::Instant::now();
    let ready =
        FSMetaSource::wait_for_task_handles_ready(&mut [rx_a, rx_b], Duration::from_secs(1)).await;
    assert!(ready);
    assert!(started.elapsed() >= Duration::from_millis(80));
}

#[tokio::test]
async fn pub_initial_scan_preserves_epoch_boundaries_for_each_primary_root() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-pub-initial-scan-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::write(nfs1.join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("b.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < deadline {
        let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("initial scan batch wait should not time out")
            .expect("pub stream should yield initial scan batch");
        for event in batch {
            let origin = event.metadata().origin_id.0.clone();
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                *control_counts.entry(origin).or_insert(0) += 1;
            } else {
                *data_counts.entry(origin).or_insert(0) += 1;
            }
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        if complete {
            break;
        }
    }

    source.close().await.expect("close source");

    assert_eq!(control_counts.get("node-a::nfs1").copied(), Some(2));
    assert_eq!(control_counts.get("node-a::nfs2").copied(), Some(2));
    assert!(
        data_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "nfs1 should publish at least one materialized record"
    );
    assert!(
        data_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "nfs2 should publish at least one materialized record"
    );

    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn manual_rescan_replays_epoch_and_data_for_each_primary_root() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-manual-rescan-all-roots-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::write(nfs1.join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("b.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("initial scan batch wait should not time out")
            .expect("pub stream should yield initial scan batch");
        for event in batch {
            let origin = event.metadata().origin_id.0.clone();
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                *control_counts.entry(origin).or_insert(0) += 1;
            } else {
                *data_counts.entry(origin).or_insert(0) += 1;
            }
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        if complete {
            break;
        }
    }

    let nfs1_initial_data = data_counts.get("node-a::nfs1").copied().unwrap_or(0);
    let nfs2_initial_data = data_counts.get("node-a::nfs2").copied().unwrap_or(0);
    assert_eq!(control_counts.get("node-a::nfs1").copied(), Some(2));
    assert_eq!(control_counts.get("node-a::nfs2").copied(), Some(2));
    assert!(nfs1_initial_data > 0, "nfs1 initial scan should emit data");
    assert!(nfs2_initial_data > 0, "nfs2 initial scan should emit data");

    std::fs::write(nfs1.join("after-rescan.txt"), b"aa").expect("append nfs1 file");
    std::fs::write(nfs2.join("after-rescan.txt"), b"bb").expect("append nfs2 file");
    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan signal");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("manual rescan batch wait should not time out")
            .expect("pub stream should yield manual rescan batch");
        for event in batch {
            let origin = event.metadata().origin_id.0.clone();
            if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                *control_counts.entry(origin).or_insert(0) += 1;
            } else {
                *data_counts.entry(origin).or_insert(0) += 1;
            }
        }
        let complete = control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4
            && control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4
            && data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data
            && data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data;
        if complete {
            break;
        }
    }

    source.close().await.expect("close source");

    assert!(
        control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4,
        "nfs1 should emit a second epoch after manual rescan: {control_counts:?}"
    );
    assert!(
        control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4,
        "nfs2 should emit a second epoch after manual rescan: {control_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data,
        "nfs1 should emit additional data after manual rescan: {data_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data,
        "nfs2 should emit additional data after manual rescan: {data_counts:?}"
    );

    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn manual_rescan_source_to_fresh_sink_replays_baseline_entries_for_each_primary_root() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-manual-rescan-fresh-sink-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    let subdir = "force-find-stress";
    std::fs::create_dir_all(nfs1.join(subdir)).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join(subdir)).expect("create nfs2 dir");
    std::fs::write(nfs1.join(subdir).join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join(subdir).join("seed.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
        .expect("init sink");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let query_dir = b"/force-find-stress";
    let query_root = b"/force-find-stress";

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink
                .send_with_failure(&batch)
                .await
                .expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root);
        let nfs2_ready = selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root),
        "nfs1 initial materialization should exist"
    );
    assert!(
        selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root),
        "nfs2 initial materialization should exist"
    );

    let fresh_sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
        .expect("init fresh sink");

    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan signal");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => fresh_sink
                .send_with_failure(&batch)
                .await
                .expect("apply rescan batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready =
            selected_group_tree_contains_path(&fresh_sink, "nfs1", query_dir, query_root);
        let nfs2_ready =
            selected_group_tree_contains_path(&fresh_sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        selected_group_tree_contains_path(&fresh_sink, "nfs1", query_dir, query_root),
        "manual rescan should rematerialize baseline nfs1 entries into a fresh sink after sink state loss"
    );
    assert!(
        selected_group_tree_contains_path(&fresh_sink, "nfs2", query_dir, query_root),
        "manual rescan should rematerialize baseline nfs2 entries into a fresh sink after sink state loss"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close original sink");
    fresh_sink.close().await.expect("close fresh sink");
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn manual_rescan_source_to_sink_materializes_new_entries_for_each_primary_root() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-manual-rescan-source-sink-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    let subdir = "force-find-stress";
    std::fs::create_dir_all(nfs1.join(subdir)).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join(subdir)).expect("create nfs2 dir");
    std::fs::write(nfs1.join(subdir).join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join(subdir).join("seed.txt"), b"b").expect("seed nfs2");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let sink =
        SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg).expect("init sink");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let query_dir = b"/force-find-stress";
    let query_root = b"/force-find-stress";
    let query_new = b"/force-find-stress/after-rescan.txt";

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink
                .send_with_failure(&batch)
                .await
                .expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root);
        let nfs2_ready = selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root),
        "nfs1 initial materialization should exist"
    );
    assert!(
        selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root),
        "nfs2 initial materialization should exist"
    );

    std::fs::write(nfs1.join(subdir).join("after-rescan.txt"), b"aa").expect("append nfs1 file");
    std::fs::write(nfs2.join(subdir).join("after-rescan.txt"), b"bb").expect("append nfs2 file");
    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan signal");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink
                .send_with_failure(&batch)
                .await
                .expect("apply rescan batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_done = selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_new);
        let nfs2_done = selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_new);
        if nfs1_done && nfs2_done {
            break;
        }
    }

    assert!(
        selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_new),
        "nfs1 should materialize its post-rescan file"
    );
    assert!(
        selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_new),
        "nfs2 should materialize its post-rescan file"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink");
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn pub_stream_yielded_path_counts_include_newly_seeded_subtree_for_each_primary_root() {
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-pub-yielded-path-counts-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
        .expect("seed nfs1 subtree");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
        .expect("seed nfs2 subtree");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(_batch)) => {}
            Ok(None) => break,
            Err(_) => continue,
        }
        let counts = source.yielded_path_origin_counts_snapshot();
        let ready = ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| counts.get(*origin).copied().unwrap_or(0) > 0);
        if ready {
            break;
        }
    }

    source.close().await.expect("close source");

    let counts = source.yielded_path_origin_counts_snapshot();
    assert!(
        counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "yielded path counts should include newly seeded subtree for nfs1: {counts:?}"
    );
    assert!(
        counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "yielded path counts should include newly seeded subtree for nfs2: {counts:?}"
    );

    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn current_pub_stream_preserves_mixed_origin_target_membership_once_batch_is_dequeued() {
    let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let source =
        FSMetaSource::with_boundaries(SourceConfig::default(), NodeId("node-a".to_string()), None)
            .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let stream_binding = lock_or_recover(
        &source.state_cell.stream_binding,
        "test.current_pub_stream_preserves_mixed_origin.stream_binding",
    )
    .as_ref()
    .expect("stream binding present")
    .clone();
    let batch = vec![
        mk_source_record_event(
            "node-a::nfs1",
            b"/force-find-stress/nfs1.txt",
            b"nfs1.txt",
            1,
        ),
        mk_source_record_event(
            "node-a::nfs2",
            b"/force-find-stress/nfs2.txt",
            b"nfs2.txt",
            2,
        ),
    ];
    let enqueued_counts = summarize_event_path_origins(&batch, b"/force-find-stress");
    FSMetaSource::record_current_stream_enqueued_path_counts(
        &source.enqueued_path_origin_counts,
        stream_binding.generation,
        &enqueued_counts,
    );
    stream_binding
        .tx
        .send(batch)
        .expect("enqueue mixed-origin batch");

    let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
        .await
        .expect("dequeued batch wait should not time out")
        .expect("pub stream should yield injected batch");
    let enqueued_counts = source.enqueued_path_origin_counts_snapshot();
    let counts = source.yielded_path_origin_counts_snapshot();
    let pending_counts = source.pending_path_origin_counts_snapshot();

    source.close().await.expect("close source");

    if let Some(value) = previous {
        unsafe {
            std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value);
        }
    } else {
        unsafe {
            std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE");
        }
    }

    assert_eq!(
        event_origin_ids(&batch),
        BTreeSet::from(["node-a::nfs1".to_string(), "node-a::nfs2".to_string()])
    );
    assert_eq!(enqueued_counts.get("node-a::nfs1").copied(), Some(1));
    assert_eq!(enqueued_counts.get("node-a::nfs2").copied(), Some(1));
    assert_eq!(counts.get("node-a::nfs1").copied(), Some(1));
    assert_eq!(counts.get("node-a::nfs2").copied(), Some(1));
    assert!(
        pending_counts.is_empty(),
        "once the mixed batch is dequeued, no target-path backlog should remain: {pending_counts:?}"
    );
}

#[tokio::test]
async fn current_pub_stream_surfaces_ready_later_origin_within_bounded_fairness_window() {
    let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let source =
        FSMetaSource::with_boundaries(SourceConfig::default(), NodeId("node-a".to_string()), None)
            .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let stream_binding = lock_or_recover(
        &source.state_cell.stream_binding,
        "test.current_pub_stream_fifo.stream_binding",
    )
    .as_ref()
    .expect("stream binding present")
    .clone();

    let nfs1_backlog_batches = 128u64;
    for ts in 1..=nfs1_backlog_batches {
        let batch = vec![mk_source_record_event(
            "node-a::nfs1",
            b"/force-find-stress/nfs1.txt",
            b"nfs1.txt",
            ts,
        )];
        FSMetaSource::record_current_stream_enqueued_path_counts(
            &source.enqueued_path_origin_counts,
            stream_binding.generation,
            &summarize_event_path_origins(&batch, b"/force-find-stress"),
        );
        stream_binding
            .tx
            .send(batch)
            .expect("enqueue nfs1 backlog batch");
    }
    let nfs2_batch = vec![mk_source_record_event(
        "node-a::nfs2",
        b"/force-find-stress/nfs2.txt",
        b"nfs2.txt",
        nfs1_backlog_batches + 1,
    )];
    FSMetaSource::record_current_stream_enqueued_path_counts(
        &source.enqueued_path_origin_counts,
        stream_binding.generation,
        &summarize_event_path_origins(&nfs2_batch, b"/force-find-stress"),
    );
    stream_binding
        .tx
        .send(nfs2_batch)
        .expect("enqueue nfs2 batch behind backlog");

    let fairness_window = 8usize;
    let mut seen_origins = BTreeSet::<String>::new();
    for _ in 0..fairness_window {
        let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("fairness-window batch wait should not time out")
            .expect("pub stream should yield a fairness-window batch");
        seen_origins.extend(event_origin_ids(&batch));
        if seen_origins.contains("node-a::nfs2") {
            break;
        }
    }

    let enqueued_counts = source.enqueued_path_origin_counts_snapshot();
    let yielded_counts = source.yielded_path_origin_counts_snapshot();
    let pending_counts = source.pending_path_origin_counts_snapshot();

    source.close().await.expect("close source");

    if let Some(value) = previous {
        unsafe {
            std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value);
        }
    } else {
        unsafe {
            std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE");
        }
    }

    assert_eq!(
        enqueued_counts.get("node-a::nfs1").copied(),
        Some(nfs1_backlog_batches)
    );
    assert_eq!(enqueued_counts.get("node-a::nfs2").copied(), Some(1));
    assert!(
        seen_origins.contains("node-a::nfs2"),
        "an already-enqueued second root should surface within the bounded fairness window instead of remaining fully buried behind historical backlog: seen={seen_origins:?} yielded={yielded_counts:?} pending={pending_counts:?}"
    );
}

#[tokio::test]
async fn second_pub_stream_after_partial_root_respawn_still_yields_all_primary_roots() {
    let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-second-pub-stream-{unique}"));
    let nfs1 = base.join("nfs1");
    let nfs2 = base.join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
        .expect("seed nfs1 subtree");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
        .expect("seed nfs2 subtree");

    let mut cfg = SourceConfig::default();
    let mut root_a = RootSpec::new("nfs1", nfs1.clone());
    root_a.watch = false;
    let mut root_b = RootSpec::new("nfs2", nfs2.clone());
    root_b.watch = false;
    cfg.roots = vec![root_a, root_b];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone(), true),
        test_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone(), true),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut first_stream = source.pub_().await.expect("start first source pub stream");
    let target = b"/force-find-stress";

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut initial_counts = BTreeMap::<String, u64>::new();
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), first_stream.next()).await {
            Ok(Some(batch)) => {
                for (origin, count) in summarize_event_path_origins(&batch, target) {
                    *initial_counts.entry(origin).or_default() += count;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| initial_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }
    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| initial_counts.get(*origin).copied().unwrap_or(0) > 0),
        "first stream should receive initial subtree data for both roots: {initial_counts:?}"
    );

    let initial_nfs2_forwarded = source
        .status_snapshot()
        .concrete_roots
        .into_iter()
        .find(|entry| entry.object_ref == "node-a::nfs2")
        .expect("nfs2 concrete root exists")
        .forwarded_path_event_count;

    let mut second_stream = source.pub_().await.expect("start second source pub stream");
    let mut desired_roots =
        lock_or_recover(&source.state_cell.roots, "test.second_pub_stream.roots").clone();
    desired_roots
        .iter_mut()
        .find(|root| root.object_ref == "node-a::nfs1")
        .expect("nfs1 desired root exists")
        .spec
        .audit_interval_ms = Some(1_234);
    *lock_or_recover(
        &source.state_cell.roots,
        "test.second_pub_stream.roots.replace",
    ) = desired_roots.clone();
    source.reconcile_root_tasks(&desired_roots).await;

    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let mut second_stream_counts = BTreeMap::<String, u64>::new();
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), second_stream.next()).await {
            Ok(Some(batch)) => {
                for (origin, count) in summarize_event_path_origins(&batch, target) {
                    *second_stream_counts.entry(origin).or_default() += count;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| second_stream_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }

    let final_snapshot = source.status_snapshot();
    let final_nfs2_forwarded = final_snapshot
        .concrete_roots
        .iter()
        .find(|entry| entry.object_ref == "node-a::nfs2")
        .expect("nfs2 concrete root exists after rescan")
        .forwarded_path_event_count;
    let final_generations = final_snapshot
        .concrete_roots
        .iter()
        .map(|entry| (entry.object_ref.clone(), entry.current_stream_generation))
        .collect::<BTreeMap<_, _>>();

    source.close().await.expect("close source");

    if let Some(value) = previous {
        unsafe {
            std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value);
        }
    } else {
        unsafe {
            std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE");
        }
    }

    assert!(
        final_nfs2_forwarded > initial_nfs2_forwarded,
        "nfs2 should still forward subtree events during the manual-rescan window: initial={initial_nfs2_forwarded} final={final_nfs2_forwarded}"
    );
    assert!(
        second_stream_counts
            .get("node-a::nfs1")
            .copied()
            .unwrap_or(0)
            > 0,
        "second stream should receive nfs1 subtree data after the partial respawn: {second_stream_counts:?}"
    );
    assert!(
        second_stream_counts
            .get("node-a::nfs2")
            .copied()
            .unwrap_or(0)
            > 0,
        "second stream should keep receiving nfs2 subtree data after the partial respawn instead of leaving nfs2 on a stale sender: {second_stream_counts:?}; initial_nfs2_forwarded={initial_nfs2_forwarded} final_nfs2_forwarded={final_nfs2_forwarded}"
    );
    assert_eq!(
        final_snapshot.current_stream_generation,
        Some(2),
        "second pub stream should advance the bound stream generation"
    );
    assert_eq!(
        final_generations.get("node-a::nfs1"),
        Some(&Some(2)),
        "nfs1 should be bound to the second pub stream generation: {final_generations:?}"
    );
    assert_eq!(
        final_generations.get("node-a::nfs2"),
        Some(&Some(2)),
        "nfs2 should be rebound onto the second pub stream generation instead of staying on a stale sender: {final_generations:?}"
    );

    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn close_rescan_channels_disables_periodic_and_rescan() {
    let mut rescan_open = true;
    let mut periodic_open = true;
    FSMetaSource::close_rescan_channels(&mut rescan_open, &mut periodic_open);
    assert!(!rescan_open);
    assert!(!periodic_open);
}

#[tokio::test]
async fn reconcile_root_tasks_falls_back_to_controlled_replace_when_candidate_never_ready() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-fallback-replace-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        root_dir,
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");

    let desired_root = lock_or_recover(&source.state_cell.roots, "test.fallback.roots")
        .first()
        .cloned()
        .expect("desired root exists");
    let root_key = FSMetaSource::root_runtime_key(&desired_root);
    let desired_signature = FSMetaSource::root_task_signature(&desired_root);
    let mut stale_signature = desired_signature.clone();
    stale_signature.watch = !desired_signature.watch;

    let (out_tx, out_rx) = mpsc::unbounded_channel::<Vec<Event>>();
    drop(out_rx);
    *lock_or_recover(
        &source.state_cell.stream_binding,
        "test.fallback.stream_binding",
    ) = Some(SourceStreamBinding {
        generation: 4,
        tx: out_tx,
    });
    source
        .state_cell
        .root_task_revision
        .store(100, Ordering::Relaxed);
    lock_or_recover(&source.state_cell.root_tasks, "test.fallback.root_tasks").insert(
        root_key.clone(),
        RootTaskEntry {
            active: RootTaskSlot {
                revision: 1,
                stream_generation: 3,
                signature: stale_signature,
                handle: pending_root_task_handle(),
            },
            candidate: Some(RootTaskSlot {
                revision: 2,
                stream_generation: 3,
                signature: desired_signature.clone(),
                handle: pending_root_task_handle(),
            }),
        },
    );
    FSMetaSource::update_root_task_slot_health(
        &source.state_cell.fanout_health_handle(),
        &root_key,
        1,
        3,
        RootTaskRole::Active,
        "running",
    );
    FSMetaSource::update_root_task_slot_health(
        &source.state_cell.fanout_health_handle(),
        &root_key,
        2,
        3,
        RootTaskRole::Candidate,
        "warming",
    );

    let started = tokio::time::Instant::now();
    source.reconcile_root_tasks(&[desired_root.clone()]).await;
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "candidate fallback must wait for bounded ready timeout"
    );

    let tasks = lock_or_recover(
        &source.state_cell.root_tasks,
        "test.fallback.root_tasks.after",
    );
    let entry = tasks.get(&root_key).expect("root task still present");
    assert_eq!(entry.active.signature, desired_signature);
    assert_eq!(entry.active.revision, 100);
    assert!(
        entry.candidate.is_none(),
        "timed out candidate must be removed"
    );
    drop(tasks);

    let health = lock_or_recover(&source.state_cell.fanout_health, "test.fallback.health");
    let detail = health
        .object_ref_detail
        .get(&root_key)
        .expect("concrete root detail exists");
    assert_eq!(detail.candidate_revision, None);
    assert_eq!(detail.draining_revision, Some(1));
    assert_eq!(detail.draining_status.as_deref(), Some("retired"));

    source.close().await.expect("close source");
}

#[tokio::test]
async fn runtime_host_object_grants_changed_reassigns_group_primary_after_failover() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-primary-failover-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
    cfg.host_object_grants = vec![
        test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            root_dir.clone(),
            true,
        ),
        test_export(
            "node-a::exp2",
            "node-a",
            "10.0.0.12",
            root_dir.clone(),
            true,
        ),
    ];

    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let initial = lock_or_recover(&source.state_cell.roots, "test.primary_before").clone();
    let initial_primary = initial
        .iter()
        .find(|root| root.is_group_primary)
        .expect("initial primary exists")
        .object_ref
        .clone();
    assert_eq!(initial_primary, "node-a::exp1");

    let envelope = encode_runtime_host_grant_change(&RuntimeHostGrantChange {
        version: 1,
        grants: vec![
            route_export(
                "node-a::exp1",
                "node-a",
                "10.0.0.11",
                &root_dir.display().to_string(),
                false,
            ),
            route_export(
                "node-a::exp2",
                "node-a",
                "10.0.0.12",
                &root_dir.display().to_string(),
                true,
            ),
        ],
    })
    .expect("encode grants changed");

    source
        .on_control_frame(&[envelope])
        .await
        .expect("apply grants changed frame");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let updated = loop {
        let snapshot = lock_or_recover(&source.state_cell.roots, "test.primary_after").clone();
        let current_primary = snapshot
            .iter()
            .find(|root| root.is_group_primary)
            .map(|root| root.object_ref.clone());
        if current_primary.as_deref() == Some("node-a::exp2") {
            break snapshot;
        }
        if tokio::time::Instant::now() >= deadline {
            let object_refs = snapshot
                .iter()
                .map(|root| {
                    format!(
                        "{}:active={}:primary={}",
                        root.object_ref, root.active, root.is_group_primary
                    )
                })
                .collect::<Vec<_>>();
            panic!(
                "timed out waiting for primary failover; roots={}",
                object_refs.join(", ")
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let updated_primary = updated
        .iter()
        .find(|root| root.is_group_primary)
        .expect("updated primary exists")
        .object_ref
        .clone();
    assert_eq!(updated_primary, "node-a::exp2");
    assert!(updated.iter().any(|root| {
        root.object_ref == "node-a::exp1" && !root.is_group_primary && !root.active
    }));

    let mut primary_rx = None;
    let mut non_primary_rx = None;
    for root in updated {
        let rx = root.rescan_tx.subscribe();
        if root.is_group_primary {
            primary_rx = Some(rx);
        } else if root.object_ref == "node-a::exp1" {
            non_primary_rx = Some(rx);
        }
    }
    source.trigger_rescan();
    let primary_reason = primary_rx
        .expect("primary receiver")
        .try_recv()
        .expect("new primary should receive manual rescan");
    assert!(matches!(primary_reason, RescanReason::Manual));
    assert!(matches!(
        non_primary_rx.expect("non-primary receiver").try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn force_find_round_robins_across_group_sources() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-force-find-rr-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
    cfg.host_object_grants = vec![
        test_export(
            "node-a::exp1",
            "node-a",
            "10.0.0.11",
            root_dir.clone(),
            true,
        ),
        test_export(
            "node-a::exp2",
            "node-a",
            "10.0.0.12",
            root_dir.clone(),
            true,
        ),
    ];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");

    let first = source
        .force_find(&force_find_params())
        .expect("first force-find");
    let second = source
        .force_find(&force_find_params())
        .expect("second force-find");
    let third = source
        .force_find(&force_find_params())
        .expect("third force-find");

    assert_eq!(
        event_origin_ids(&first),
        BTreeSet::from(["nfs1".to_string()])
    );
    assert_eq!(
        event_origin_ids(&second),
        BTreeSet::from(["nfs1".to_string()])
    );
    assert_eq!(
        event_origin_ids(&third),
        BTreeSet::from(["nfs1".to_string()])
    );
    let last_runner = lock_or_recover(
        &source.force_find_last_runner,
        "test.force_find.last_runner.round_robin",
    );
    assert_eq!(
        last_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1")
    );
    drop(last_runner);
    let rr = lock_or_recover(&source.force_find_rr, "test.force_find.rr.round_robin");
    assert_eq!(rr.get("nfs1").copied(), Some(1));
}

#[test]
fn force_find_rejects_when_group_is_already_inflight() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-force-find-lock-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", root_dir.clone())];
    cfg.host_object_grants = vec![test_export(
        "node-a::exp1",
        "node-a",
        "10.0.0.11",
        root_dir,
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");

    lock_or_recover(&source.force_find_inflight, "test.force_find.inflight")
        .insert("nfs1".to_string());
    let err = source
        .force_find(&force_find_params())
        .expect_err("in-flight force-find must fail closed");
    assert!(matches!(err, CnxError::NotReady(_)));
    assert!(err.to_string().contains("already running for group: nfs1"));
}

#[test]
fn force_find_returns_error_when_selected_group_matches_no_runtime() {
    let source = build_source(vec![test_export(
        "node-a::nfs1",
        "node-a",
        "10.0.0.11",
        "/mnt/nfs1",
        true,
    )]);

    let params = crate::query::request::InternalQueryRequest::force_find(
        crate::query::request::QueryOp::Tree,
        crate::query::request::QueryScope {
            selected_group: Some("node-z::nfs9".to_string()),
            ..Default::default()
        },
    );
    let err = source
        .force_find(&params)
        .expect_err("unknown target origin must fail");
    assert!(matches!(err, CnxError::PeerError(_)));
    assert!(err.to_string().contains("matched no group"));
}

#[test]
fn req_skips_unreachable_roots() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-req-reachable-{unique}"));
    let missing = std::env::temp_dir().join(format!("fs-meta-req-unreachable-{unique}"));
    std::fs::create_dir_all(&base).expect("create base root");
    std::fs::write(base.join("ok.txt"), b"ok").expect("seed base file");
    let _ = std::fs::remove_dir_all(&missing);

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![
        RootSpec::new("nfs-ok", base.clone()),
        RootSpec::new("nfs-unreachable", missing.clone()),
    ];
    cfg.host_object_grants = vec![
        test_export("node-a::nfs-ok", "node-a", "node-a", base.clone(), true),
        test_export(
            "node-a::nfs-unreachable",
            "node-a",
            "node-a",
            missing.clone(),
            false,
        ),
    ];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let events = source
        .query_scan(&crate::query::request::LiveScanRequest::default())
        .expect("reachable roots should still query successfully");
    assert!(!events.is_empty());
}

#[test]
fn force_find_reports_unreachable_target_root() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let missing_root = std::env::temp_dir().join(format!("fs-meta-target-unreachable-{unique}"));
    let _ = std::fs::remove_dir_all(&missing_root);

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs-unreachable", missing_root.clone())];
    cfg.host_object_grants = vec![test_export(
        "node-a::nfs-unreachable",
        "node-a",
        "node-a",
        missing_root,
        false,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");
    let params = crate::query::request::InternalQueryRequest::force_find(
        crate::query::request::QueryOp::Tree,
        crate::query::request::QueryScope {
            selected_group: Some("nfs-unreachable".to_string()),
            ..Default::default()
        },
    );
    let err = source
        .force_find(&params)
        .expect_err("unreachable target root must fail explicitly");
    assert!(matches!(err, CnxError::PeerError(_)));
    assert!(err.to_string().contains("inactive"));
}

#[test]
fn force_find_returns_error_when_all_targeted_roots_fail() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let missing_root = std::env::temp_dir().join(format!("fs-meta-missing-root-{unique}"));
    let _ = std::fs::remove_dir_all(&missing_root);

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs-missing", missing_root.clone())];
    cfg.host_object_grants = vec![test_export(
        "node-a::nfs-missing",
        "node-a",
        "10.0.0.99",
        missing_root,
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");

    let err = source
        .force_find(&crate::query::request::InternalQueryRequest::force_find(
            crate::query::request::QueryOp::Tree,
            crate::query::request::QueryScope::default(),
        ))
        .expect_err("all-root failures must fail force-find");
    assert!(matches!(err, CnxError::PeerError(_)));
    assert!(err.to_string().contains("failed on all targeted roots"));
    assert!(err.to_string().contains("node-a::nfs-missing"));
}

#[test]
fn force_find_missing_subpath_returns_empty_group_payload_instead_of_error() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let base = std::env::temp_dir().join(format!("fs-meta-force-find-missing-subpath-{unique}"));
    std::fs::create_dir_all(&base).expect("create base root");

    let mut cfg = SourceConfig::default();
    cfg.roots = vec![RootSpec::new("nfs1", base.clone())];
    cfg.host_object_grants = vec![test_export(
        "node-a::nfs1",
        "node-a",
        "10.0.0.11",
        base,
        true,
    )];
    let source = FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
        .expect("init source");

    let events = source
        .force_find(&crate::query::request::InternalQueryRequest::force_find(
            crate::query::request::QueryOp::Tree,
            crate::query::request::QueryScope {
                path: b"/missing-subpath".to_vec(),
                ..Default::default()
            },
        ))
        .expect("missing subpath should degrade to empty payload");
    assert_eq!(events.len(), 1);
}

#[test]
fn sentinel_trigger_rescan_bridges_into_rescan_channel() {
    let (tx, mut rx) = tokio::sync::broadcast::channel(2);
    FSMetaSource::execute_sentinel_actions(
        &[SentinelAction::TriggerRescan {
            root_key: "nfs1@node-a".to_string(),
        }],
        "nfs1@node-a",
        Some(&tx),
        None,
    );
    let reason = rx
        .try_recv()
        .expect("sentinel trigger_rescan must publish a rescan signal");
    assert!(matches!(reason, RescanReason::Manual));
}

#[test]
fn manual_rescan_is_processed_even_when_primary_flag_is_false() {
    assert!(should_process_rescan_reason(false, RescanReason::Manual));
}

#[test]
fn periodic_and_overflow_rescan_still_require_primary() {
    assert!(!should_process_rescan_reason(false, RescanReason::Periodic));
    assert!(!should_process_rescan_reason(false, RescanReason::Overflow));
    assert!(should_process_rescan_reason(true, RescanReason::Periodic));
    assert!(should_process_rescan_reason(true, RescanReason::Overflow));
}

#[test]
fn sentinel_degraded_updates_concrete_health_projection() {
    let health = Arc::new(Mutex::new(FanoutHealthState::default()));
    FSMetaSource::execute_sentinel_actions(
        &[SentinelAction::ReportDegraded {
            root_key: "nfs1@node-a@/mnt/nfs1".to_string(),
            reason: "overflow".to_string(),
        }],
        "nfs1@node-a@/mnt/nfs1",
        None,
        Some(&health),
    );
    let guard = lock_or_recover(&health, "test.sentinel_degraded_health");
    assert_eq!(
        guard
            .object_ref
            .get("nfs1@node-a@/mnt/nfs1")
            .map(String::as_str),
        Some("degraded: overflow")
    );
}

#[test]
fn sentinel_actions_ignore_mismatched_root_key() {
    let (tx, mut rx) = tokio::sync::broadcast::channel(2);
    let health = Arc::new(Mutex::new(FanoutHealthState::default()));
    FSMetaSource::execute_sentinel_actions(
        &[SentinelAction::TriggerRescan {
            root_key: "nfs1@node-a@/mnt/nfs1".to_string(),
        }],
        "nfs2@node-b@/mnt/nfs2",
        Some(&tx),
        Some(&health),
    );
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn epoch0_scan_delay_parser_respects_defaults_and_bounds() {
    assert_eq!(epoch0_scan_delay_ms_from_raw(None, 0), 0);
    assert_eq!(epoch0_scan_delay_ms_from_raw(Some("250"), 0), 250);
    assert_eq!(epoch0_scan_delay_ms_from_raw(Some("bad"), 100), 100);
    assert_eq!(
        epoch0_scan_delay_ms_from_raw(Some("999999999"), 100),
        EPOCH0_SCAN_DELAY_MAX_MS
    );
}
