use super::*;
use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::runtime::{
    EventMetadata, LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
    in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_host_fs_types::UnixStat;
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeUnitTick, encode_runtime_exec_control, encode_runtime_unit_tick,
};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex as StdMutex, OnceLock};
use tempfile::tempdir;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::Duration;

use crate::runtime::routes::{
    METHOD_SINK_QUERY, ROUTE_KEY_EVENTS, ROUTE_KEY_QUERY, ROUTE_KEY_SINK_ROOTS_CONTROL,
    ROUTE_TOKEN_FS_META_INTERNAL, default_route_bindings,
};
use crate::source::FSMetaSource;
use crate::source::config::RootSpec;
use crate::{ControlEvent, EpochType, EventKind, FileMetaRecord};

#[derive(Default)]
struct LoopbackWorkerBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: StdMutex<HashSet<String>>,
    send_batches_by_channel: StdMutex<HashMap<String, usize>>,
    recv_batches_by_channel: StdMutex<HashMap<String, usize>>,
    changed: Notify,
}

impl LoopbackWorkerBoundary {
    fn send_batch_count(&self, channel: &str) -> usize {
        *self
            .send_batches_by_channel
            .lock()
            .expect("loopback send batches lock")
            .get(channel)
            .unwrap_or(&0)
    }

    fn recv_batch_count(&self, channel: &str) -> usize {
        *self
            .recv_batches_by_channel
            .lock()
            .expect("loopback recv batches lock")
            .get(channel)
            .unwrap_or(&0)
    }
}

#[async_trait]
impl ChannelIoSubset for LoopbackWorkerBoundary {
    async fn channel_send(&self, _ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        {
            let mut send_batches = self
                .send_batches_by_channel
                .lock()
                .expect("loopback send batches lock");
            *send_batches
                .entry(request.channel_key.0.clone())
                .or_default() += 1;
        }
        {
            let mut channels = self.channels.lock().await;
            channels
                .entry(request.channel_key.0)
                .or_default()
                .extend(request.events);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn channel_recv(
        &self,
        _ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        let deadline = request
            .timeout_ms
            .map(Duration::from_millis)
            .map(|timeout| tokio::time::Instant::now() + timeout);
        loop {
            {
                let mut channels = self.channels.lock().await;
                if let Some(events) = channels.remove(&request.channel_key.0)
                    && !events.is_empty()
                {
                    let mut recv_batches = self
                        .recv_batches_by_channel
                        .lock()
                        .expect("loopback recv batches lock");
                    *recv_batches
                        .entry(request.channel_key.0.clone())
                        .or_default() += 1;
                    return Ok(events);
                }
            }
            {
                let closed = self.closed.lock().expect("loopback closed lock");
                if closed.contains(&request.channel_key.0) {
                    return Err(CnxError::ChannelClosed);
                }
            }
            let notified = self.changed.notified();
            if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, notified).await {
                    Ok(()) => {}
                    Err(_) => return Err(CnxError::Timeout),
                }
            } else {
                notified.await;
            }
        }
    }

    fn channel_close(&self, _ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.closed
            .lock()
            .expect("loopback closed lock")
            .insert(channel.0);
        self.changed.notify_waiters();
        Ok(())
    }
}

fn mk_worker_sink_source_event(origin: &str, record: FileMetaRecord) -> Event {
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

fn mk_worker_sink_record(
    path: &[u8],
    file_name: &str,
    ts: u64,
    event_kind: EventKind,
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
        crate::SyncTrack::Scan,
        b"/".to_vec(),
        ts,
        false,
    )
}

fn mk_worker_sink_control_event(origin: &str, control: ControlEvent, ts: u64) -> Event {
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

impl ChannelBoundary for LoopbackWorkerBoundary {
    fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
}

impl StateBoundary for LoopbackWorkerBoundary {}

fn fs_meta_runtime_lib_filename() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libfs_meta_runtime.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "fs_meta_runtime.dll"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "libfs_meta_runtime.so"
    }
}

fn fs_meta_runtime_workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn fs_meta_worker_module_path() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
            if let Ok(path) = std::env::var(name) {
                let resolved = PathBuf::from(path);
                if resolved.exists() {
                    return resolved;
                }
            }
        }
        let root = fs_meta_runtime_workspace_root();
        let lib_name = fs_meta_runtime_lib_filename();
        for candidate in [
            root.join("target/debug").join(lib_name),
            root.join("target/debug/deps").join(lib_name),
            root.join(".target/debug").join(lib_name),
            root.join(".target/debug/deps").join(lib_name),
        ] {
            if candidate.exists() {
                return candidate;
            }
        }
        panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY");
    })
    .clone()
}

fn external_sink_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: "sink".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(fs_meta_worker_module_path()),
        socket_dir: Some(socket_dir.to_path_buf()),
    }
}

fn sink_worker_root(id: &str, path: &Path) -> RootSpec {
    let mut root = RootSpec::new(id, path);
    root.watch = false;
    root.scan = true;
    root
}

fn sink_worker_export(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: PathBuf,
) -> GrantedMountRoot {
    GrantedMountRoot {
        object_ref: object_ref.to_string(),
        host_ref: host_ref.to_string(),
        host_ip: host_ip.to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point,
        fs_source: object_ref.to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }
}

fn test_worker_control_route_key_for(role_id: &str, node_id: &str) -> String {
    let normalize = |raw: &str| -> String {
        raw.chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!(
        "capanix.worker.{}.{}.rpc:v1",
        normalize(role_id),
        normalize(node_id)
    )
}

fn selected_group_request(path: &[u8], group_id: &str) -> InternalQueryRequest {
    InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: path.to_vec(),
            recursive: false,
            max_depth: Some(0),
            selected_group: Some(group_id.to_string()),
        },
        None,
    )
}

fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
    RuntimeBoundScope {
        scope_id: scope_id.to_string(),
        resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_external_sink_worker_handles_for_same_binding_keep_one_worker_lane() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let binding = external_sink_worker_binding(worker_socket_dir.path());
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let first = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct first sink worker client"),
    );
    let second = Arc::new(
        SinkWorkerClientHandle::new(NodeId("node-a".to_string()), cfg, binding, factory)
            .expect("construct second sink worker client"),
    );
    let envelopes = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ];

    let first_task = {
        let first = first.clone();
        let envelopes = envelopes.clone();
        tokio::spawn(async move { first.on_control_frame(envelopes).await })
    };
    let second_task = {
        let second = second.clone();
        let envelopes = envelopes.clone();
        tokio::spawn(async move { second.on_control_frame(envelopes).await })
    };

    tokio::time::timeout(Duration::from_secs(20), async {
        first_task.await.expect("join first activation task")
    })
    .await
    .expect("first activation timed out")
    .expect("first activation should succeed without spawning a conflicting worker");
    tokio::time::timeout(Duration::from_secs(20), async {
        second_task.await.expect("join second activation task")
    })
    .await
    .expect("second activation timed out")
    .expect("second activation should share the same worker lane");

    let first_status = first.status_snapshot().await.expect("first status");
    let second_status = second.status_snapshot().await.expect("second status");
    assert_eq!(
        first_status.scheduled_groups_by_node, second_status.scheduled_groups_by_node,
        "same-binding sink worker handles should observe one shared external worker state",
    );

    first.close().await.expect("close first sink worker handle");
    second
        .close()
        .await
        .expect("close second sink worker handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_materializes_each_local_primary_root_from_source_batches() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "nfs1 initial materialization should exist",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "nfs2 initial materialization should exist",
    );

    std::fs::write(
        nfs1.join("force-find-stress").join("after-rescan.txt"),
        b"aa",
    )
    .expect("append nfs1 file");
    std::fs::write(
        nfs2.join("force-find-stress").join("after-rescan.txt"),
        b"bb",
    )
    .expect("append nfs2 file");
    source
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply rescan batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_done = decode_exact_query_node(
            sink.materialized_query(selected_group_request(
                b"/force-find-stress/after-rescan.txt",
                "nfs1",
            ))
            .await
            .expect("query nfs1 rescan"),
            b"/force-find-stress/after-rescan.txt",
        )
        .expect("decode nfs1 rescan")
        .is_some();
        let nfs2_done = decode_exact_query_node(
            sink.materialized_query(selected_group_request(
                b"/force-find-stress/after-rescan.txt",
                "nfs2",
            ))
            .await
            .expect("query nfs2 rescan"),
            b"/force-find-stress/after-rescan.txt",
        )
        .expect("decode nfs2 rescan")
        .is_some();
        if nfs1_done && nfs2_done {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(
                b"/force-find-stress/after-rescan.txt",
                "nfs1",
            ))
            .await
            .expect("query nfs1 final"),
            b"/force-find-stress/after-rescan.txt",
        )
        .expect("decode nfs1 final")
        .is_some(),
        "nfs1 should materialize its post-rescan file",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(
                b"/force-find-stress/after-rescan.txt",
                "nfs2",
            ))
            .await
            .expect("query nfs2 final"),
            b"/force-find-stress/after-rescan.txt",
        )
        .expect("decode nfs2 final")
        .is_some(),
        "nfs2 should materialize its post-rescan file",
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_materializes_third_local_primary_root_non_root_subtree_after_source_batches()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    for dir in [&nfs1, &nfs2, &nfs3] {
        std::fs::create_dir_all(dir.join("data")).expect("create data dir");
        std::fs::write(dir.join("data").join("a.txt"), b"a").expect("seed a");
        std::fs::write(dir.join("data").join("b.txt"), b"b").expect("seed b");
    }

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
            sink_worker_root("nfs3", &nfs3),
        ],
        host_object_grants: vec![
            sink_worker_export("node-b::nfs1", "node-b", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-b::nfs2", "node-b", "10.0.0.12", nfs2.clone()),
            sink_worker_export("node-b::nfs3", "node-b", "10.0.0.13", nfs3.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-b".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                bound_scope_with_resources("nfs3", &["node-b::nfs3"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink groups");

    let selected_dir = b"/data";
    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_dir,
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs2"))
                .await
                .expect("query nfs2"),
            selected_dir,
        )
        .expect("decode nfs2")
        .is_some();
        let nfs3_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs3"))
                .await
                .expect("query nfs3"),
            selected_dir,
        )
        .expect("decode nfs3")
        .is_some();
        if nfs1_ready && nfs2_ready && nfs3_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                .await
                .expect("query nfs1 final"),
            selected_dir,
        )
        .expect("decode nfs1 final")
        .is_some(),
        "nfs1 should materialize /data",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs2"))
                .await
                .expect("query nfs2 final"),
            selected_dir,
        )
        .expect("decode nfs2 final")
        .is_some(),
        "nfs2 should materialize /data",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs3"))
                .await
                .expect("query nfs3 final"),
            selected_dir,
        )
        .expect("decode nfs3 final")
        .is_some(),
        "third local primary root should materialize /data after source batches",
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_third_group_non_root_subtree_recovers_after_first_batch_was_dropped()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    for dir in [&nfs1, &nfs2, &nfs3] {
        std::fs::create_dir_all(dir.join("data")).expect("create data dir");
        std::fs::write(dir.join("data").join("a.txt"), b"a").expect("seed a");
        std::fs::write(dir.join("data").join("b.txt"), b"b").expect("seed b");
    }

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
            sink_worker_root("nfs3", &nfs3),
        ],
        host_object_grants: vec![
            sink_worker_export("node-b::nfs1", "node-b", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-b::nfs2", "node-b", "10.0.0.12", nfs2.clone()),
            sink_worker_export("node-b::nfs3", "node-b", "10.0.0.13", nfs3.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-b".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                bound_scope_with_resources("nfs3", &["node-b::nfs3"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink groups");

    let selected_dir = b"/data";
    let mut dropped_first_nfs3_batch = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => {
                let mut forward = Vec::new();
                let mut batch_has_nfs3 = false;
                for event in batch {
                    if event.metadata().origin_id.0 == "node-b::nfs3" {
                        batch_has_nfs3 = true;
                    }
                    forward.push(event);
                }
                if batch_has_nfs3 && !dropped_first_nfs3_batch {
                    dropped_first_nfs3_batch = true;
                    continue;
                }
                sink.send(forward).await.expect("apply source batch");
            }
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_dir,
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs2"))
                .await
                .expect("query nfs2"),
            selected_dir,
        )
        .expect("decode nfs2")
        .is_some();
        let nfs3_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs3"))
                .await
                .expect("query nfs3"),
            selected_dir,
        )
        .expect("decode nfs3")
        .is_some();
        if nfs1_ready && nfs2_ready && nfs3_ready {
            break;
        }
    }

    assert!(
        dropped_first_nfs3_batch,
        "precondition: one nfs3 batch should be dropped"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs3"))
                .await
                .expect("query nfs3 final"),
            selected_dir,
        )
        .expect("decode nfs3 final")
        .is_some(),
        "third group should still materialize /data after later batches even if its first batch was dropped",
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_schedules_group_before_it_materializes_until_batches_arrive() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");
    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink groups");

    let selected_file = b"/force-find-stress/seed.txt";
    let mut deferred_nfs2_batches = Vec::<Vec<Event>>::new();
    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => {
                let mut nfs1_batch = Vec::new();
                let mut nfs2_batch = Vec::new();
                for event in batch {
                    match event.metadata().origin_id.0.as_str() {
                        "node-a::nfs1" => nfs1_batch.push(event),
                        "node-a::nfs2" => nfs2_batch.push(event),
                        _ => {}
                    }
                }
                if !nfs2_batch.is_empty() {
                    deferred_nfs2_batches.push(nfs2_batch);
                }
                if !nfs1_batch.is_empty() {
                    sink.send(nfs1_batch).await.expect("apply nfs1-only batch");
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1 while nfs2 withheld"),
            selected_file,
        )
        .expect("decode nfs1 while nfs2 withheld")
        .is_some();
        if nfs1_ready {
            break;
        }
    }

    let status = sink
        .status_snapshot()
        .await
        .expect("status snapshot after nfs1-only send");
    assert_eq!(
        status
            .scheduled_groups_by_node
            .get("node-a")
            .cloned()
            .unwrap_or_default(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
        "scheduled group coverage should reflect both roots before nfs2 materializes",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1 after nfs1-only send"),
            selected_file,
        )
        .expect("decode nfs1 after nfs1-only send")
        .is_some(),
        "nfs1 should materialize once its batches are sent",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2 before batches arrive"),
            selected_file,
        )
        .expect("decode nfs2 before batches arrive")
        .is_none(),
        "nfs2 should stay empty until its own batches are sent",
    );

    for batch in deferred_nfs2_batches.drain(..) {
        sink.send(batch).await.expect("apply deferred nfs2 batch");
    }
    let nfs2_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < nfs2_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => {
                let nfs2_batch = batch
                    .into_iter()
                    .filter(|event| event.metadata().origin_id.0 == "node-a::nfs2")
                    .collect::<Vec<_>>();
                if !nfs2_batch.is_empty() {
                    sink.send(nfs2_batch)
                        .await
                        .expect("apply remaining nfs2 batch");
                }
            }
            Ok(None) => {}
            Err(_) => continue,
        }
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2 after deferred batches"),
            selected_file,
        )
        .expect("decode nfs2 after deferred batches")
        .is_some();
        if nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2 final"),
            selected_file,
        )
        .expect("decode nfs2 final")
        .is_some(),
        "nfs2 should materialize on the same sink-worker seam once its batches arrive",
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_internal_materialized_route_delivers_sequential_same_owner_queries_twice()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            sink_worker_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");
    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink groups");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let selected_dir = b"/force-find-stress";
    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_dir,
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs2"))
                .await
                .expect("query nfs2"),
            selected_dir,
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    let adapter = exchange_host_adapter_from_channel_boundary(
        boundary.clone(),
        NodeId("node-d".to_string()),
        default_route_bindings(),
    );
    let route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
        .expect("resolve internal sink query route");
    let reply_route = format!("{}:reply", route.0);
    let first_events = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(
                rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                    .expect("encode nfs1 internal query"),
            ),
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await;
    let second_events = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(
                rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs2"))
                    .expect("encode nfs2 internal query"),
            ),
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await;

    assert!(
        first_events.is_ok(),
        "first direct internal sink-query route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        first_events.as_ref().err(),
    );
    assert!(
        second_events.is_ok(),
        "second direct internal sink-query route should complete; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={} err={:?}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
        second_events.as_ref().err(),
    );

    let first_node = decode_exact_query_node(
        first_events.expect("first direct internal query result"),
        selected_dir,
    )
    .expect("decode first direct internal query");
    let second_node = decode_exact_query_node(
        second_events.expect("second direct internal query result"),
        selected_dir,
    )
    .expect("decode second direct internal query");

    assert!(
        first_node.is_some(),
        "first direct internal sink-query route should return the owner materialized subtree"
    );
    assert!(
        second_node.is_some(),
        "second direct internal sink-query route should return the owner materialized subtree"
    );
    assert_eq!(
        boundary.send_batch_count(&route.0),
        2,
        "both sequential internal sink-query calls should send one request batch each"
    );
    assert_eq!(
        boundary.recv_batch_count(&route.0),
        2,
        "owner runtime endpoint should receive both internal sink-query request batches"
    );
    assert_eq!(
        boundary.send_batch_count(&reply_route),
        2,
        "owner runtime endpoint should send one reply batch for each internal sink-query call"
    );
    assert_eq!(
        boundary.recv_batch_count(&reply_route),
        2,
        "caller should receive one reply batch for each internal sink-query call"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_sink_worker_internal_materialized_route_serves_local_owner_payload_while_control_inflight()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-a".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");
    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-a::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink group");

    let selected_dir = b"/force-find-stress";
    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_dir, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_dir,
        )
        .expect("decode nfs1")
        .is_some();
        if ready {
            break;
        }
    }

    let adapter = exchange_host_adapter_from_channel_boundary(
        boundary.clone(),
        NodeId("node-d".to_string()),
        default_route_bindings(),
    );
    let route = default_route_bindings()
        .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_QUERY)
        .expect("resolve internal sink query route");
    let reply_route = format!("{}:reply", route.0);
    let baseline = adapter
        .call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(
                rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                    .expect("encode baseline internal query"),
            ),
            Duration::from_secs(5),
            Duration::from_millis(250),
        )
        .await
        .expect("baseline direct internal sink-query route should complete");
    assert!(
        decode_exact_query_node(baseline, selected_dir)
            .expect("decode baseline internal query")
            .is_some(),
        "baseline direct internal sink-query route should return the owner materialized subtree"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let inflight_control = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![bound_scope_with_resources(
                                "nfs1",
                                &["node-a::nfs1"],
                            )],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ],
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("sink control should reach pause point");

    let inflight_events = tokio::time::timeout(
        Duration::from_millis(800),
        adapter.call_collect(
            ROUTE_TOKEN_FS_META_INTERNAL,
            METHOD_SINK_QUERY,
            Bytes::from(
                rmp_serde::to_vec(&selected_group_request(selected_dir, "nfs1"))
                    .expect("encode inflight internal query"),
            ),
            Duration::from_secs(5),
            Duration::from_millis(250),
        ),
    )
    .await
    .expect("direct internal sink-query route should still settle while sink control is in flight")
    .expect("inflight direct internal sink-query route");

    assert!(
        decode_exact_query_node(inflight_events, selected_dir)
            .expect("decode inflight internal query")
            .is_some(),
        "direct internal sink-query route during sink control inflight must still return the owner materialized subtree; request_send_batches={} request_recv_batches={} reply_send_batches={} reply_recv_batches={}",
        boundary.send_batch_count(&route.0),
        boundary.recv_batch_count(&route.0),
        boundary.send_batch_count(&reply_route),
        boundary.recv_batch_count(&reply_route),
    );

    release.notify_waiters();
    let _ = inflight_control.await.expect("join inflight control");

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

struct SinkWorkerUpdateRootsHookReset;

impl Drop for SinkWorkerUpdateRootsHookReset {
    fn drop(&mut self) {
        clear_sink_worker_update_roots_hook();
    }
}

struct SinkWorkerControlFrameErrorHookReset;

impl Drop for SinkWorkerControlFrameErrorHookReset {
    fn drop(&mut self) {
        clear_sink_worker_control_frame_error_hook();
    }
}

struct SinkWorkerControlFramePauseHookReset;

impl Drop for SinkWorkerControlFramePauseHookReset {
    fn drop(&mut self) {
        clear_sink_worker_control_frame_pause_hook();
    }
}

struct SinkWorkerStatusErrorHookReset;

impl Drop for SinkWorkerStatusErrorHookReset {
    fn drop(&mut self) {
        clear_sink_worker_status_error_hook();
    }
}

struct SinkWorkerStatusSnapshotHookReset;

impl Drop for SinkWorkerStatusSnapshotHookReset {
    fn drop(&mut self) {
        clear_sink_worker_status_snapshot_hook();
    }
}

struct SinkWorkerScheduledGroupsErrorHookReset;

impl Drop for SinkWorkerScheduledGroupsErrorHookReset {
    fn drop(&mut self) {
        clear_sink_worker_scheduled_groups_error_hook();
    }
}

struct SinkWorkerRetryResetHookReset;

impl Drop for SinkWorkerRetryResetHookReset {
    fn drop(&mut self) {
        clear_sink_worker_retry_reset_hook();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_query_nonblocking_does_not_dispatch_worker_rpc_while_control_inflight() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink query route");

    let worker_route = test_worker_control_route_key_for("sink", "node-d");
    let baseline_send_batches = boundary.send_batch_count(&worker_route);
    let _inflight = sink.begin_control_op();

    let events = sink
            .materialized_query_nonblocking(selected_group_request(b"/", "nfs1"))
            .await
            .expect(
                "materialized_query_nonblocking should fail closed from the local nonblocking path while sink worker control is already in flight",
            );
    assert!(
        events.is_empty(),
        "nonblocking materialized query during control inflight should fail closed with an empty result instead of dispatching another worker rpc: {events:?}"
    );
    assert_eq!(
        boundary.send_batch_count(&worker_route),
        baseline_send_batches,
        "materialized_query_nonblocking must not dispatch a worker rpc while sink worker control is already in flight"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_stale_empty_cache_after_materialization_when_control_is_marked_inflight()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let primed = sink
        .status_snapshot()
        .await
        .expect("prime initial empty cached status snapshot");
    assert!(
        primed.scheduled_groups_by_node.is_empty(),
        "precondition: initial cached snapshot should start empty"
    );

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink query route");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        if ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after materialization"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after materialization")
        .is_some(),
        "precondition: owner-local materialized query should already see the ready subtree"
    );

    let _inflight = sink.begin_control_op();
    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking during synthetic control inflight");

    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-d"),
        Some(&vec!["nfs1".to_string()]),
        "status_snapshot_nonblocking must not regress to the stale empty pre-activate cache after the sink already materialized nfs1"
    );
    assert!(
        snapshot
            .groups
            .iter()
            .find(|group| group.group_id == "nfs1")
            .is_some_and(|group| group.live_nodes > 0 && group.total_nodes > 0),
        "status_snapshot_nonblocking must keep the materialized group visible instead of regressing to a stale empty cache while control is merely marked inflight: {snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_fails_closed_from_stale_empty_cache_when_control_is_marked_inflight_and_live_status_probe_times_out_after_schedule_convergence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let readiness_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let live_snapshot = loop {
        match sink.status_snapshot().await {
            Ok(snapshot)
                if snapshot
                    .groups
                    .iter()
                    .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                    .all(|group| group.initial_audit_completed && group.live_nodes > 0) =>
            {
                break snapshot;
            }
            Ok(_) | Err(CnxError::Timeout) => {}
            Err(err) => panic!("snapshot after materialization: {err:?}"),
        }
        assert!(
            tokio::time::Instant::now() < readiness_deadline,
            "precondition: both nfs1 and nfs2 must be ready before seeding a split stale cache"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(
        live_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before seeding a stale empty cache: {live_snapshot:?}"
    );

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing the local sink-status fallback seam"
    );

    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("seed stale empty cached status after ready materialization");

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Timeout,
    });

    let _inflight = sink.begin_control_op();
    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed when the cached status summary is empty, scheduled groups are already converged, and the live status probe times out during control inflight",
            );

    assert!(
        matches!(err, CnxError::Timeout),
        "stale empty local sink-status cache during control inflight must fail closed with timeout once scheduled groups are converged: err={err:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_republishes_scheduled_groups_into_cached_summary_when_control_is_marked_inflight_and_live_status_probe_fails_after_schedule_convergence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing the inflight local sink-status fallback seam"
    );

    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("seed stale empty cached status after ready materialization");

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let _inflight = sink.begin_control_op();
    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed when the cached status summary is empty and the live inflight sink status probe failed",
            );
    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "control-inflight live status failure should be propagated instead of publishing an empty cached sink-status summary: {err:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached status summary after control-inflight fail-closed nonblocking status");
    let cached_scheduled = cached_snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        cached_scheduled, scheduled,
        "after schedule convergence, a control-inflight fail-closed nonblocking status probe must still republish the cached scheduled groups instead of leaving the local sink-status summary completely empty: cached={cached_snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

include!("tests/status_snapshot_partially_stale.rs");
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_return_scheduled_zero_uninitialized_cache_when_worker_unavailable()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let live_snapshot = sink
        .status_snapshot()
        .await
        .expect("snapshot after materialization");
    assert!(
        live_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before seeding a scheduled zero cache: {live_snapshot:?}"
    );

    let mut zero_snapshot = live_snapshot.clone();
    zero_snapshot.scheduled_groups_by_node = std::collections::BTreeMap::from([(
        "node-d".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    for group in &mut zero_snapshot.groups {
        if group.group_id == "nfs1" || group.group_id == "nfs2" {
            group.primary_object_ref = "unassigned".to_string();
            group.initial_audit_completed = false;
            group.live_nodes = 0;
            group.total_nodes = 0;
            group.tombstoned_count = 0;
            group.attested_count = 0;
            group.suspect_count = 0;
            group.blind_spot_count = 0;
            group.shadow_time_us = 0;
            group.shadow_lag_us = 0;
            group.overflow_pending_audit = false;
            group.materialized_revision = 1;
            group.estimated_heap_bytes = 0;
        }
    }
    sink.update_cached_status_snapshot(zero_snapshot.clone())
        .expect("seed zero scheduled cache");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err("status_snapshot_nonblocking must not publish a scheduled-but-fully-zero sink snapshot when the live sink worker status call failed");

    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "worker-unavailable path should propagate the live status error instead of returning a scheduled zero/uninitialized snapshot: stale={zero_snapshot:?} err={err:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_republishes_scheduled_groups_into_cached_summary_when_live_status_fails_from_stale_empty_cache_after_schedule_convergence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("snapshot after materialization");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before probing local sink-status republish: {ready_snapshot:?}"
    );

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing stale empty cache publication"
    );

    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("seed stale empty cached status after ready materialization");

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::Internal("synthetic non-retryable sink status failure".to_string()),
    });

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed when the cached status summary is empty and the live sink worker status call failed",
            );
    assert!(
        matches!(err, CnxError::Internal(ref message) if message.contains("synthetic non-retryable sink status failure")),
        "live status failure should be propagated instead of returning an empty cached sink-status summary: {err:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached status summary after fail-closed nonblocking status");
    let cached_scheduled = cached_snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        cached_scheduled, scheduled,
        "after schedule convergence, a fail-closed nonblocking status probe must still republish the cached scheduled groups instead of leaving the local sink-status summary completely empty: ready={ready_snapshot:?} cached={cached_snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_snapshot_nonblocking_does_not_regress_ready_cached_groups_to_live_missing_scheduled_rows_with_stream_evidence()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs3", &nfs3)],
        host_object_grants: vec![sink_worker_export(
            "node-b::nfs3",
            "node-b",
            "10.0.0.43",
            nfs3.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let ready_snapshot = SinkStatusSnapshot {
        groups: vec![crate::sink::SinkGroupStatusSnapshot {
            group_id: "nfs3".to_string(),
            primary_object_ref: "node-b::nfs3".to_string(),
            total_nodes: 6,
            live_nodes: 5,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_audit: false,
            initial_audit_completed: true,
            materialized_revision: 7,
            estimated_heap_bytes: 1,
        }],
        scheduled_groups_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs3".to_string()],
        )]),
        stream_ready_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_applied_batches_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            1,
        )]),
        stream_applied_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            15,
        )]),
        stream_applied_control_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            3,
        )]),
        stream_applied_data_events_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            12,
        )]),
        stream_applied_origin_counts_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["node-b::nfs3=15".to_string()],
        )]),
        stream_last_applied_at_us_by_node: std::collections::BTreeMap::from([(
            "node-b".to_string(),
            42,
        )]),
        ..SinkStatusSnapshot::default()
    };
    sink.update_cached_status_snapshot(ready_snapshot.clone())
        .expect("seed ready cached snapshot");

    let live_snapshot = SinkStatusSnapshot {
        scheduled_groups_by_node: ready_snapshot.scheduled_groups_by_node.clone(),
        stream_ready_origin_counts_by_node: ready_snapshot
            .stream_ready_origin_counts_by_node
            .clone(),
        stream_applied_batches_by_node: ready_snapshot.stream_applied_batches_by_node.clone(),
        stream_applied_events_by_node: ready_snapshot.stream_applied_events_by_node.clone(),
        stream_applied_control_events_by_node: ready_snapshot
            .stream_applied_control_events_by_node
            .clone(),
        stream_applied_data_events_by_node: ready_snapshot
            .stream_applied_data_events_by_node
            .clone(),
        stream_applied_origin_counts_by_node: ready_snapshot
            .stream_applied_origin_counts_by_node
            .clone(),
        stream_last_applied_at_us_by_node: ready_snapshot.stream_last_applied_at_us_by_node.clone(),
        ..SinkStatusSnapshot::default()
    };

    let _reset = SinkWorkerStatusSnapshotHookReset;
    install_sink_worker_status_snapshot_hook(SinkWorkerStatusSnapshotHook {
        snapshot: live_snapshot.clone(),
    });

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking must preserve the ready cached group instead of publishing a live scheduled-with-stream-evidence snapshot that dropped the group row",
            );

    assert!(
        snapshot_has_ready_scheduled_groups(&snapshot),
        "status_snapshot_nonblocking must not regress ready cached groups to a live snapshot that keeps scheduled nfs3 stream evidence but drops the group row: ready={ready_snapshot:?} live={live_snapshot:?} returned={snapshot:?}"
    );

    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached sink status after degraded live snapshot");
    assert!(
        snapshot_has_ready_scheduled_groups(&cached_snapshot),
        "the degraded live snapshot must not overwrite the ready cached status summary: ready={ready_snapshot:?} live={live_snapshot:?} cached={cached_snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_does_not_regress_ready_cached_groups_to_live_replayed_zero_state_after_worker_restart()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = match sink.status_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("live sink snapshot: {err:?}"),
        };
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before restart");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before restarting the sink worker: {ready_snapshot:?}"
    );

    sink.shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown sink worker before replay-only restart");
    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");

    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking after replay-only worker restart");

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must not regress previously ready groups to a replay-only zero-state snapshot after worker restart: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_worker_restart_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before restart");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before restarting the no-grant sink worker: {ready_snapshot:?}"
    );

    sink.shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown sink worker before replay-only restart");
    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");
    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after retained replay worker restart should return a restored snapshot",
            );

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after worker restart and retained replay instead of reopening with scheduled zero groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_retry_reset_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before retry reset");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before retry reset: {ready_snapshot:?}"
    );

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retry-reset retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after retry reset retained replay should return a restored snapshot",
            );

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "retry-reset retained replay should reset the shared sink worker client before publishing status",
    );
    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after retry reset and retained replay instead of reopening with scheduled zero groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_persisted_root_id_ready_state_after_same_instance_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before same-instance retained replay");

    let snapshot = sink
            .status_snapshot_nonblocking()
            .await
            .expect(
                "status_snapshot_nonblocking after same-instance retained replay should return a restored snapshot",
            );

    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "status_snapshot_nonblocking must restore durably persisted root-id ready state after same-instance retained replay instead of leaving the local sink-status summary empty: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_settles_within_runtime_app_probe_budget_after_same_instance_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before same-instance retained replay");

    let result = tokio::time::timeout(
        Duration::from_millis(350),
        sink.status_snapshot_nonblocking(),
    )
    .await;

    let result = result.expect(
            "status_snapshot_nonblocking must settle within the runtime-app local sink-status republish probe budget after same-instance retained replay",
        );
    match result {
        Ok(snapshot) => assert!(
            snapshot
                .groups
                .iter()
                .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                .all(|group| group.initial_audit_completed && group.live_nodes > 0),
            "status_snapshot_nonblocking must either restore ready state within the probe budget or fail-close, not return a degraded snapshot: ready={ready_snapshot:?} returned={snapshot:?}"
        ),
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "status_snapshot_nonblocking within the runtime-app local sink-status republish probe budget must restore ready state or fail-close, got {err}"
        ),
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_settles_within_runtime_app_probe_budget_after_same_instance_replay_completed()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete same-instance retained replay before local republish probe");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after same-instance retained replay completed");

    let result = tokio::time::timeout(
        Duration::from_millis(350),
        sink.status_snapshot_nonblocking(),
    )
    .await;

    let result = result.expect(
            "status_snapshot_nonblocking must settle within the runtime-app local sink-status republish probe budget after same-instance retained replay already completed",
        );
    match result {
        Ok(snapshot) => assert!(
            snapshot
                .groups
                .iter()
                .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
                .all(|group| group.initial_audit_completed && group.live_nodes > 0),
            "status_snapshot_nonblocking after replay completion must restore ready state within the probe budget or fail-close, not return a degraded snapshot: ready={ready_snapshot:?} returned={snapshot:?}"
        ),
        Err(CnxError::Timeout) => {}
        Err(err) => panic!(
            "status_snapshot_nonblocking after replay completion within the runtime-app local sink-status republish probe budget must restore ready state or fail-close, got {err}"
        ),
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_eventually_restores_ready_groups_after_second_same_instance_replay_completed_without_new_source_events()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before second same-instance retained replay");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before second same-instance retained replay: {ready_snapshot:?}"
    );

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete first same-instance retained replay before later recovery");
    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.replay_retained_control_state_if_needed()
        .await
        .expect("complete second same-instance retained replay before local republish wait");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after second same-instance retained replay completed");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after second retained replay")
            .unwrap_or_default();
        assert_eq!(
            scheduled, expected_groups,
            "scheduled groups must stay converged while local sink-status republish waits after second same-instance retained replay"
        );

        match tokio::time::timeout(
            Duration::from_millis(350),
            sink.status_snapshot_nonblocking(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.initial_audit_completed)
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            let cached_snapshot = sink
                .cached_status_snapshot()
                .expect("cached status after second same-instance retained replay");
            let blocking_sink_status =
                match tokio::time::timeout(Duration::from_secs(2), sink.status_snapshot()).await {
                    Ok(Ok(snapshot)) => format!("{snapshot:?}"),
                    Ok(Err(err)) => format!("blocking_status_err={err}"),
                    Err(_) => "blocking_status_timeout".to_string(),
                };
            panic!(
                "second same-instance retained replay should eventually restore ready groups without new source events instead of leaving local sink-status stuck empty: cached={cached_snapshot:?} blocking={blocking_sink_status}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_restores_ready_groups_after_explicit_same_instance_retained_wave_without_new_source_events()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let root_scopes = vec![
        bound_scope_with_resources("nfs1", &["nfs1"]),
        bound_scope_with_resources("nfs2", &["nfs2"]),
    ];
    let explicit_replay_wave = || {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink query activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink events activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: root_scopes.clone(),
            }))
            .expect("encode sink roots-control activate"),
            encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key: format!("{}.stream", ROUTE_KEY_EVENTS),
                unit_id: "runtime.exec.sink".to_string(),
                generation: 2,
                at_ms: 1,
            })
            .expect("encode explicit retained sink replay tick"),
        ]
    };

    sink.on_control_frame(explicit_replay_wave())
        .await
        .expect("apply initial sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before explicit replay wave");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before explicit same-instance replay wave: {ready_snapshot:?}"
    );

    sink.on_control_frame(explicit_replay_wave())
        .await
        .expect("apply explicit same-instance retained sink replay wave");
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status after explicit same-instance replay wave");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after explicit same-instance replay wave")
            .unwrap_or_default();
        assert_eq!(
            scheduled, expected_groups,
            "scheduled groups must stay converged while local sink-status republish waits after the explicit same-instance retained sink replay wave"
        );

        match tokio::time::timeout(
            Duration::from_millis(350),
            sink.status_snapshot_nonblocking(),
        )
        .await
        {
            Ok(Ok(snapshot)) => {
                let ready_groups = snapshot
                    .groups
                    .iter()
                    .filter(|group| group.initial_audit_completed)
                    .map(|group| group.group_id.clone())
                    .collect::<std::collections::BTreeSet<_>>();
                if ready_groups == expected_groups {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            let cached_snapshot = sink
                .cached_status_snapshot()
                .expect("cached status after explicit same-instance replay wave");
            let blocking_sink_status =
                match tokio::time::timeout(Duration::from_secs(2), sink.status_snapshot()).await {
                    Ok(Ok(snapshot)) => format!("{snapshot:?}"),
                    Ok(Err(err)) => format!("blocking_status_err={err}"),
                    Err(_) => "blocking_status_timeout".to_string(),
                };
            panic!(
                "explicit same-instance retained sink replay wave should eventually restore ready groups without new source events instead of leaving local sink-status stuck empty: cached={cached_snapshot:?} blocking={blocking_sink_status}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_republishes_scheduled_groups_into_cached_summary_before_runtime_app_probe_budget_cancels_post_replay_status_refresh()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before runtime-app probe cancellation seam");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before probing post-replay local status cancellation seam: {ready_snapshot:?}"
    );

    let scheduled = sink
        .scheduled_group_ids()
        .await
        .expect("scheduled groups after ready materialization")
        .unwrap_or_default();
    assert_eq!(
        scheduled,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        "precondition: scheduled groups must already be converged before probing the post-replay local sink-status seam"
    );

    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before post-replay local status probe");

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        sink.status_snapshot_nonblocking(),
    )
    .await;
    let cached_snapshot = sink
        .cached_status_snapshot()
        .expect("cached status summary after bounded post-replay local status probe");
    let cached_scheduled = cached_snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        cached_scheduled, scheduled,
        "when the runtime-app probe budget expires during post-replay local status refresh, the cached sink-status summary must still republish the converged scheduled groups instead of staying fully empty: result={result:?} cached={cached_snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_restores_persisted_root_id_ready_state_after_retry_reset_and_retained_replay()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before root-id materialization");

    sink.send(vec![
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            2,
        ),
        mk_worker_sink_source_event(
            "nfs1",
            mk_worker_sink_record(b"/ready-a.txt", "ready-a.txt", 3, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs1",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            4,
        ),
        mk_worker_sink_source_event(
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
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            6,
        ),
        mk_worker_sink_source_event(
            "nfs2",
            mk_worker_sink_record(b"/ready-b.txt", "ready-b.txt", 7, EventKind::Update),
        ),
        mk_worker_sink_control_event(
            "nfs2",
            ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit,
            },
            8,
        ),
    ])
    .await
    .expect("apply root-id ready events");

    let ready_snapshot = sink
        .status_snapshot()
        .await
        .expect("ready snapshot before retry reset");
    assert!(
        ready_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both nfs1 and nfs2 must be ready before retry reset: {ready_snapshot:?}"
    );

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });
    let _status_error_reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    sink.control_state_replay_required
        .store(1, Ordering::Release);
    sink.update_cached_status_snapshot(SinkStatusSnapshot::default())
        .expect("clear cached status before retry-reset retained replay");

    let snapshot = sink
            .status_snapshot()
            .await
            .expect("blocking status_snapshot after retry reset retained replay should return a restored snapshot");

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "blocking retry-reset retained replay should reset the shared sink worker client before publishing status",
    );
    assert!(
        snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "blocking status_snapshot must restore durably persisted root-id ready state after retry reset and retained replay instead of timing out or reopening with not-ready groups: ready={ready_snapshot:?} returned={snapshot:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_report_replay_complete_when_live_snapshot_is_scheduled_zero_uninitialized()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &[]),
                bound_scope_with_resources("nfs2", &[]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before replay-required status");

    sink.control_state_replay_required
        .store(1, Ordering::Release);

    let err = sink
            .status_snapshot()
            .await
            .expect_err("status_snapshot must not report retained replay complete while the live sink status is still a scheduled zero/uninitialized snapshot");

    assert!(
        matches!(err, CnxError::Timeout),
        "replay-required blocking status must fail close instead of reporting a scheduled zero/uninitialized snapshot as success: {err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_publish_scheduled_zero_uninitialized_snapshot_after_replay_cleared()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &[]),
                bound_scope_with_resources("nfs2", &[]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before blocking status");

    sink.control_state_replay_required
        .store(0, Ordering::Release);

    let err = sink
            .status_snapshot()
            .await
            .expect_err("status_snapshot must fail close instead of publishing a scheduled zero/uninitialized snapshot after retained replay has already been cleared");

    assert!(
        matches!(err, CnxError::Timeout),
        "blocking status_snapshot must fail close on scheduled zero/uninitialized groups even after retained replay was already cleared: {err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_fails_closed_when_live_snapshot_is_single_scheduled_zero_uninitialized_with_bound_primary_object_ref()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs3", &nfs3)],
        host_object_grants: vec![sink_worker_export(
            "node-b::nfs3",
            "node-b",
            "10.0.0.43",
            nfs3.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-b-29795712685086500907384833".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs3", &["node-b::nfs3"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before zero-state status probe");

    let scheduled_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups after activate")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs3".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduled_deadline,
            "single-group activate should converge scheduled nfs3 before probing the live zero-state seam: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let raw_client = sink.client().await.expect("typed sink worker client");
    let raw_snapshot = match SinkWorkerClientHandle::call_worker(
        &raw_client,
        SinkWorkerRequest::StatusSnapshot,
        SINK_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect("raw status snapshot RPC should succeed before fail-close wrapper")
    {
        SinkWorkerResponse::StatusSnapshot(snapshot) => snapshot,
        other => panic!("unexpected raw sink worker status response: {other:?}"),
    };
    let raw_group = raw_snapshot
        .groups
        .iter()
        .find(|group| group.group_id == "nfs3")
        .expect("raw live sink snapshot should include scheduled nfs3");
    assert_eq!(
        raw_snapshot
            .scheduled_groups_by_node
            .values()
            .flat_map(|groups| groups.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["nfs3".to_string()]),
        "precondition: raw live sink snapshot must already carry scheduled nfs3"
    );
    assert!(
        !raw_group.initial_audit_completed
            && raw_group.live_nodes == 0
            && raw_group.total_nodes == 0
            && raw_group.materialized_revision <= 1,
        "precondition: raw live sink snapshot must still be a zero/uninitialized scheduled group: {raw_snapshot:?}"
    );
    assert_ne!(
        raw_group.primary_object_ref, "unassigned",
        "precondition: this seam needs a concrete primary object ref so the live zero-state can slip past the current scheduled-zero guard: {raw_snapshot:?}"
    );
    assert_ne!(
        raw_group.primary_object_ref, raw_group.group_id,
        "precondition: this seam needs a bound primary object ref distinct from the group id so the live zero-state can slip past the current scheduled-zero guard: {raw_snapshot:?}"
    );

    let err = sink
            .status_snapshot_nonblocking()
            .await
            .expect_err(
                "status_snapshot_nonblocking must fail closed instead of publishing a single scheduled zero/uninitialized group whose live primary_object_ref is a concrete bound grant ref",
            );

    assert!(
        matches!(err, CnxError::Timeout),
        "single scheduled zero/uninitialized group with a bound primary_object_ref must fail close with timeout instead of reaching runtime-app as an ok empty-root summary: err={err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_does_not_rearm_same_retained_replay_after_zero_uninitialized_reply() {
    struct SinkWorkerControlFramePauseHookReset;

    impl Drop for SinkWorkerControlFramePauseHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_pause_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink events activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!(
                "{}.stream",
                crate::runtime::routes::ROUTE_KEY_SINK_ROOTS_CONTROL
            ),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode sink roots-control activate"),
    ])
    .await
    .expect("apply retained replay control wave before blocking status");

    sink.control_state_replay_required
        .store(1, Ordering::Release);

    let first_err = sink
            .status_snapshot()
            .await
            .expect_err("first blocking status_snapshot must fail close on a zero/uninitialized retained replay snapshot");

    assert!(
        matches!(first_err, CnxError::Timeout),
        "first blocking status_snapshot must fail close on a zero/uninitialized retained replay snapshot: {first_err:?}"
    );
    assert_eq!(
        sink.control_state_replay_required.load(Ordering::Acquire),
        0,
        "blocking status_snapshot must not re-arm the same retained replay after it already replayed the retained three-envelope sink wave and still saw a zero/uninitialized snapshot"
    );

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _pause_reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_status = tokio::spawn({
        let sink = sink.clone();
        async move { sink.status_snapshot().await }
    });

    let pause_entered = tokio::time::timeout(Duration::from_millis(600), entered.notified()).await;
    if pause_entered.is_ok() {
        release.notify_waiters();
        let _ = second_status.await;
        panic!(
            "second blocking status_snapshot must not replay the same retained three-envelope sink wave again after the first zero/uninitialized fail-close"
        );
    }

    let second_err = tokio::time::timeout(Duration::from_secs(2), second_status)
            .await
            .expect("second blocking status_snapshot should settle promptly without replaying retained control")
            .expect("join second blocking status_snapshot")
            .expect_err(
                "second blocking status_snapshot should still fail close on the zero/uninitialized snapshot",
            );
    assert!(
        matches!(second_err, CnxError::Timeout),
        "second blocking status_snapshot should fail close without replaying retained control: {second_err:?}"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_query_still_reads_local_payload_while_control_inflight() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("activate sink query route");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        if ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "initial materialization should exist before sink control pauses"
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let inflight_control = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 3,
                            expires_at_ms: 1,
                            bound_scopes: vec![bound_scope_with_resources(
                                "nfs1",
                                &["node-d::nfs1"],
                            )],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ],
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("sink control should reach pause point");

    let query = tokio::time::timeout(
        Duration::from_millis(800),
        sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1")),
    )
    .await;

    release.notify_waiters();
    let _ = inflight_control.await.expect("join inflight control");

    let events = query
        .expect("blocking materialized_query should still settle while sink control is in flight")
        .expect("blocking materialized_query during control inflight");
    assert!(
        decode_exact_query_node(events, b"/force-find-stress")
            .expect("decode query during inflight")
            .is_some(),
        "blocking materialized_query during sink control inflight must still return the last local materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error and reach the live sink worker",
        );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors_after_first_wave_succeeded() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before stale drained/fenced retry"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before stale drained/fenced retry"
    );

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(RuntimeExecDeactivate {
                route_key: ROUTE_KEY_EVENTS.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 3,
                reason: "test deactivate".to_string(),
            }))
            .expect("encode sink deactivate"),
        ])
        .await
        .expect(
            "on_control_frame should retry a stale drained/fenced pid error after the first sink wave already succeeded",
        );

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after drained/fenced retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after drained/fenced retry")
        .is_some(),
        "stale drained/fenced retry must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after drained/fenced retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after drained/fenced retry")
        .is_some(),
        "stale drained/fenced retry must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_invalid_or_revoked_grant_attachment_tokens_after_first_wave_succeeded()
 {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::AccessDenied("invalid or revoked grant attachment token".to_string()),
            "on_control_frame should retry an invalid or revoked grant attachment token after the first sink wave already succeeded",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_channel_closed_after_first_wave_succeeded() {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::ChannelClosed,
            "on_control_frame should retry channel-closed continuity gaps after the first sink wave already succeeded",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_channel_closed_followup_resets_shared_client_before_retry() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let previous_worker_identity = sink.shared_worker_identity_for_tests().await;

    let reset_count = Arc::new(AtomicUsize::new(0));
    let _reset_hook = SinkWorkerRetryResetHookReset;
    install_sink_worker_retry_reset_hook(SinkWorkerRetryResetHook {
        reset_count: reset_count.clone(),
    });

    let _error_reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook {
        err: CnxError::ChannelClosed,
    });

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("follow-up sink control wave should recover after channel-closed error");

    let next_worker_identity = sink.shared_worker_identity_for_tests().await;

    assert!(
        reset_count.load(Ordering::SeqCst) >= 1,
        "follow-up channel-closed recovery must reset the shared sink worker client before retry"
    );
    assert_ne!(
        next_worker_identity, previous_worker_identity,
        "follow-up channel-closed recovery must replace the shared sink worker handle before retry so later waves cannot inherit the stale bridge session"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_enforces_total_timeout_when_worker_call_stalls_after_first_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode first-wave sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let sink = sink.clone();
        async move {
            sink.on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ],
                Duration::from_millis(150),
                Duration::from_millis(150),
            )
            .await
        }
    });

    entered.notified().await;
    let err = tokio::time::timeout(Duration::from_secs(1), control_task)
        .await
        .expect(
            "stalled sink on_control_frame should resolve within the local total timeout budget",
        )
        .expect("join stalled sink on_control_frame task")
        .expect_err(
            "stalled sink on_control_frame should fail once its local timeout budget is exhausted",
        );
    assert!(matches!(err, CnxError::Timeout), "err={err:?}");

    release.notify_waiters();
    sink.close().await.expect("close sink worker");
}

async fn assert_on_control_frame_retries_bridge_reset_error(err: CnxError, label: &str) {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let _reset = SinkWorkerControlFrameErrorHookReset;
    install_sink_worker_control_frame_error_hook(SinkWorkerControlFrameErrorHook { err });

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode second-wave sink activate"),
    ])
    .await
    .unwrap_or_else(|err| panic!("{label}: {err}"));

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "{label}: scheduled groups should remain converged after retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_internal_peer_early_eof_errors_after_first_wave_succeeded() {
    assert_on_control_frame_retries_bridge_reset_error(
            CnxError::Internal(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
            "on_control_frame should retry an internal early-eof bridge error and reach the live sink worker",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_events_deactivate_fails_fast_after_repeated_bridge_reset_errors()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode initial sink events activate"),
    ])
    .await
    .expect("initial sink events wave should succeed");

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let started = std::time::Instant::now();
    let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "single restart_deferred_retire_pending events deactivate should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset fail-close lane should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "bridge-reset fail-close lane should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_events_deactivate_retries_stale_drained_fenced_pid_error_after_first_wave_succeeded()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 force-find dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 force-find dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("first sink control wave should succeed");

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => sink.send(batch).await.expect("apply initial batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1"),
            b"/force-find-stress",
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2"),
            b"/force-find-stress",
        )
        .expect("decode nfs2")
        .is_some();
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after initial")
        .is_some(),
        "precondition: nfs1 initial materialization should exist before restart_deferred_retire_pending retry"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after initial"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after initial")
        .is_some(),
        "precondition: nfs2 initial materialization should exist before restart_deferred_retire_pending retry"
    );

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(SinkWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::from(vec![CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            )]),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        });

    sink.on_control_frame(vec![
            encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                RuntimeExecDeactivate {
                    route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                    unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 3,
                    reason: "restart_deferred_retire_pending".to_string(),
                },
            ))
            .expect("encode sink events deactivate"),
        ])
        .await
        .expect(
            "single restart_deferred_retire_pending events deactivate should retry a stale drained/fenced pid error and reach the live sink worker",
        );

    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs1"))
                .await
                .expect("query nfs1 after restart_deferred_retire_pending retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs1 after restart_deferred_retire_pending retry")
        .is_some(),
        "restart_deferred_retire_pending retry must preserve nfs1 materialized payload"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(b"/force-find-stress", "nfs2"))
                .await
                .expect("query nfs2 after restart_deferred_retire_pending retry"),
            b"/force-find-stress",
        )
        .expect("decode nfs2 after restart_deferred_retire_pending retry")
        .is_some(),
        "restart_deferred_retire_pending retry must preserve nfs2 materialized payload"
    );

    source.close().await.expect("close source");
    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = sink
            .status_snapshot()
            .await
            .expect("status_snapshot should retry a stale drained/fenced pid error and reach the live sink worker");

    assert_eq!(
        snapshot.groups.len(),
        1,
        "fresh live sink worker snapshot should still decode after stale-pid retry"
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_group_ids_retry_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("initial scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups before stale-pid retry test: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SinkWorkerScheduledGroupsErrorHookReset;
    install_sink_worker_scheduled_groups_error_hook(SinkWorkerScheduledGroupsErrorHook {
            err: CnxError::AccessDenied(
                "sink worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let scheduled = sink
            .scheduled_group_ids()
            .await
            .expect("scheduled_group_ids should retry stale drained/fenced pid errors and reach the live sink worker");

    assert_eq!(scheduled, Some(expected_groups));

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_restart_deferred_retire_pending_cleanup_batch_fails_fast_after_repeated_bridge_reset_errors()
 {
    struct SinkWorkerControlFrameErrorQueueHookReset;

    impl Drop for SinkWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_sink_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode initial sink events activate"),
    ])
    .await
    .expect("initial sink events wave should succeed");

    let _reset = SinkWorkerControlFrameErrorQueueHookReset;
    install_sink_worker_control_frame_error_queue_hook(
            SinkWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::Internal(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: None,
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let started = std::time::Instant::now();
    let err = sink
            .on_control_frame_with_timeouts_for_tests(
                vec![
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_c:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-c query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find.node_d:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode node-d query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "materialized-find:v1.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode aggregate query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "on-demand-force-find:v1.on-demand-force-find.req".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode force-find query deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: "sink-logical-roots-control:v1.stream".to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode sink roots control deactivate"),
                    encode_runtime_exec_control(&capanix_runtime_entry_sdk::control::RuntimeExecControl::Deactivate(
                        RuntimeExecDeactivate {
                            route_key: format!("{}.stream", crate::runtime::routes::ROUTE_KEY_EVENTS),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 3,
                            reason: "restart_deferred_retire_pending".to_string(),
                        },
                    ))
                    .expect("encode events deactivate"),
                ],
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "multi-envelope restart_deferred_retire_pending cleanup batch should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset retained cleanup batch should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "bridge-reset retained cleanup batch should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_sink_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), sink.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let first_snapshot = sink
        .status_snapshot()
        .await
        .expect("prime cached status snapshot");
    assert!(
        first_snapshot.scheduled_groups_by_node.is_empty(),
        "primed cached sink status should start without scheduled groups: {:?}",
        first_snapshot.scheduled_groups_by_node
    );

    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-d::nfs1"])],
        }))
        .expect("encode sink activate"),
    ])
    .await
    .expect("apply sink control before nonblocking status");

    let _reset = SinkWorkerStatusErrorHookReset;
    install_sink_worker_status_error_hook(SinkWorkerStatusErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    let snapshot = sink
        .status_snapshot_nonblocking()
        .await
        .expect("status_snapshot_nonblocking should still return a snapshot");

    assert_eq!(
        snapshot.scheduled_groups_by_node.get("node-d"),
        Some(&vec!["nfs1".to_string()]),
        "status_snapshot_nonblocking should retry a peer bridge-stopped error and reach the live sink worker instead of returning the stale cached pre-activate snapshot: {:?}",
        snapshot.scheduled_groups_by_node
    );

    sink.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _reset = SinkWorkerUpdateRootsHookReset;
    install_sink_worker_update_roots_hook(SinkWorkerUpdateRootsHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let update_task = tokio::spawn({
        let client = client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            client
                .update_logical_roots(
                    vec![
                        sink_worker_root("nfs1", &nfs1),
                        sink_worker_root("nfs2", &nfs2),
                    ],
                    vec![
                        sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                        sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                    ],
                )
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale worker bridge");
    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(4), update_task)
        .await
        .expect("update_logical_roots should reacquire a live sink worker client after handoff")
        .expect("join update_logical_roots task")
        .expect("update_logical_roots after worker restart");

    let roots = client
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after update");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-handoff sink update"
    );

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_does_not_clear_cached_ready_status_for_surviving_groups() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed-a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed-b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            sink_worker_root("nfs1", &nfs1),
            sink_worker_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = FSMetaSource::with_boundaries(cfg.clone(), NodeId("node-d".to_string()), None)
        .expect("init source");
    let mut stream = source.pub_().await.expect("start source pub stream");

    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: "runtime.exec.sink".to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode sink activate"),
        ])
        .await
        .expect("apply sink control");

    let materialized_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < materialized_deadline {
        match tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            Ok(Some(batch)) => client.send(batch).await.expect("apply source batch"),
            Ok(None) => break,
            Err(_) => continue,
        }
        let snapshot = client.status_snapshot().await.expect("live sink snapshot");
        let both_ready = snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0);
        if both_ready {
            break;
        }
    }

    let primed_snapshot = client
        .status_snapshot()
        .await
        .expect("primed live sink status snapshot");
    assert!(
        primed_snapshot
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "precondition: both groups must be ready before worker roots update: {primed_snapshot:?}"
    );

    client
        .update_logical_roots(
            vec![
                sink_worker_root("nfs1", &nfs1),
                sink_worker_root("nfs2", &nfs2),
            ],
            vec![
                sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            ],
        )
        .await
        .expect("worker update_logical_roots");

    let cached = client
        .cached_status_snapshot()
        .expect("cached status after update_logical_roots");
    assert!(
        cached
            .groups
            .iter()
            .filter(|group| group.group_id == "nfs1" || group.group_id == "nfs2")
            .all(|group| group.initial_audit_completed && group.live_nodes > 0),
        "update_logical_roots must not clear the cached ready sink status for surviving groups: before={primed_snapshot:?} after={cached:?}"
    );

    source.close().await.expect("close source");
    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: "runtime.exec.sink".to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode sink activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), control_task)
        .await
        .expect("on_control_frame should reacquire a live sink worker client after handoff")
        .expect("join on_control_frame task")
        .expect("sink on_control_frame after worker restart");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after sink handoff retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_and_transport_closes_mid_handoff()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_wave = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge after first wave");
    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker restart timed out")
        .expect("restart sink worker after first-wave success");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
        .await
        .expect("second sink control wave should reacquire a live worker client after reset")
        .expect("join second sink control wave")
        .expect("second sink control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after second-wave retry: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close sink worker");
}

include!("tests/shared_handle_nine_wave.rs");
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_on_control_frame_reacquires_worker_client_after_first_wave_succeeded_without_external_ensure_started()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave sink activate"),
        ])
        .await
        .expect("first sink control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SinkWorkerControlFramePauseHookReset;
    install_sink_worker_control_frame_pause_hook(SinkWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let second_wave = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: crate::runtime::execution_units::SINK_RUNTIME_UNIT_ID
                                .to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave sink activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests(Duration::from_secs(2))
        .await
        .expect("shutdown stale sink worker bridge after first wave");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
            .await
            .expect("second sink control wave should restart the sink worker client without external ensure_started")
            .expect("join second sink control wave")
            .expect("second sink control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled = client
            .scheduled_group_ids()
            .await
            .expect("scheduled groups")
            .unwrap_or_default();
        if scheduled == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled sink groups should remain converged after automatic second-wave restart: scheduled={scheduled:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_keeps_shared_sink_worker_client_alive_when_another_handle_still_exists() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_sink_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor sink worker client"),
    );
    let successor = Arc::new(
        SinkWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start shared sink worker");

    assert!(
        successor
            .shared_worker_existing_client_for_tests()
            .await
            .expect("existing sink client before predecessor close")
            .is_some(),
        "shared sink worker must have a live client before predecessor close"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor sink worker handle");

    assert!(
        successor
            .shared_worker_existing_client_for_tests()
            .await
            .expect("existing sink client after predecessor close")
            .is_some(),
        "closing one sink handle must not tear down the shared worker client while a successor handle still exists"
    );

    let roots = successor
        .logical_roots_snapshot()
        .await
        .expect("successor sink logical_roots_snapshot after predecessor close");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "shared sink worker should stay usable for the successor after predecessor close"
    );

    successor
        .close()
        .await
        .expect("close successor sink worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_waits_for_inflight_update_logical_roots_control_op_before_shutting_down_worker() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![
            sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_sink_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.ensure_started())
        .await
        .expect("sink worker start timed out")
        .expect("start sink worker");

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let _reset = SinkWorkerUpdateRootsHookReset;
    install_sink_worker_update_roots_hook(SinkWorkerUpdateRootsHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let update_task = tokio::spawn({
        let client = client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            client
                .update_logical_roots(
                    vec![
                        sink_worker_root("nfs1", &nfs1),
                        sink_worker_root("nfs2", &nfs2),
                    ],
                    vec![
                        sink_worker_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
                        sink_worker_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
                    ],
                )
                .await
        }
    });

    entered.notified().await;
    let close_task = tokio::spawn({
        let client = client.clone();
        async move { client.close().await }
    });
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        !close_task.is_finished(),
        "sink worker close must wait for in-flight update_logical_roots before tearing down the worker bridge"
    );

    release.notify_waiters();

    update_task
        .await
        .expect("join sink update task")
        .expect("sink update_logical_roots after close wait");
    close_task
        .await
        .expect("join sink close task")
        .expect("close sink worker after update");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_runtime_factories_do_not_share_started_sink_worker_client_on_same_node() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![sink_worker_root("nfs1", &nfs1)],
        host_object_grants: vec![sink_worker_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let state_boundary_a = in_memory_state_boundary();
    let state_boundary_b = in_memory_state_boundary();
    let boundary_a = Arc::new(LoopbackWorkerBoundary::default());
    let boundary_b = Arc::new(LoopbackWorkerBoundary::default());
    let worker_socket_dir = tempdir().expect("create worker socket dir");
    let binding = external_sink_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            RuntimeWorkerClientFactory::new(
                boundary_a.clone(),
                boundary_a.clone(),
                state_boundary_a,
            ),
        )
        .expect("construct predecessor sink worker client"),
    );
    let successor = Arc::new(
        SinkWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            binding,
            RuntimeWorkerClientFactory::new(
                boundary_b.clone(),
                boundary_b.clone(),
                state_boundary_b,
            ),
        )
        .expect("construct successor sink worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.ensure_started())
        .await
        .expect("predecessor sink worker start timed out")
        .expect("start predecessor sink worker");

    assert!(
        predecessor
            .shared_worker_existing_client_for_tests()
            .await
            .expect("predecessor existing sink client")
            .is_some(),
        "predecessor should have a started sink worker client"
    );
    assert!(
        successor
            .shared_worker_existing_client_for_tests()
            .await
            .expect("successor existing sink client after predecessor start")
            .is_none(),
        "a successor created through a distinct runtime worker factory must not inherit the predecessor's started sink worker client"
    );
    assert!(
        !Arc::ptr_eq(&predecessor._shared, &successor._shared),
        "distinct runtime worker factories must not share one sink worker handle state"
    );

    tokio::time::timeout(Duration::from_secs(8), successor.ensure_started())
        .await
        .expect("successor sink worker start timed out")
        .expect("start successor sink worker");

    assert_ne!(
        predecessor.shared_worker_identity_for_tests().await,
        successor.shared_worker_identity_for_tests().await,
        "distinct runtime worker factories must not share one sink runtime worker client wrapper"
    );
    assert_eq!(
        predecessor
            .logical_roots_snapshot()
            .await
            .expect("predecessor logical_roots_snapshot after successor start")
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "predecessor sink worker should remain independently usable"
    );
    assert_eq!(
        successor
            .logical_roots_snapshot()
            .await
            .expect("successor logical_roots_snapshot after successor start")
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "successor sink worker should be independently usable"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor sink worker");
    successor
        .close()
        .await
        .expect("close successor sink worker");
}
