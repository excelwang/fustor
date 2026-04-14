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

struct SinkWorkerStatusResponseQueueHookReset;

impl Drop for SinkWorkerStatusResponseQueueHookReset {
    fn drop(&mut self) {
        clear_sink_worker_status_response_queue_hook();
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
    match sink.status_snapshot_nonblocking().await {
        Ok(snapshot) => {
            let ready_groups = snapshot
                .groups
                .iter()
                .filter(|group| group.initial_audit_completed)
                .map(|group| group.group_id.clone())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                ready_groups,
                std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
                "status_snapshot_nonblocking during control inflight should prefer a recovered ready snapshot over a stale empty cache when the live probe can recover within the local budget: {snapshot:?}"
            );
        }
        Err(err) => assert!(
            matches!(err, CnxError::Timeout),
            "stale empty local sink-status cache during control inflight must either recover to the ready snapshot or fail closed with timeout once scheduled groups are converged: err={err:?}"
        ),
    }

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
include!("tests/status_snapshot_replay.rs");
include!("tests/control_frame_recovery.rs");
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
