use super::*;
use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelBoundary, ChannelKey, ChannelRecvRequest, ChannelSendRequest,
    StateBoundary,
};
use capanix_app_sdk::route_proto::ExecLeaseMetadata;
use capanix_app_sdk::runtime::{
    ControlFrame, LogLevel, RuntimeWorkerBinding, RuntimeWorkerLauncherKind,
    in_memory_state_boundary,
};
use capanix_app_sdk::worker::WorkerMode;
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, RuntimeExecDeactivate,
    RuntimeHostDescriptor, RuntimeHostGrant, RuntimeHostGrantChange, RuntimeHostGrantState,
    RuntimeHostObjectType, RuntimeObjectDescriptor, RuntimeUnitTick, encode_runtime_exec_control,
    encode_runtime_host_grant_change, encode_runtime_unit_tick,
};
use capanix_runtime_entry_sdk::worker_runtime::{
    RuntimeWorkerClientFactory, TypedWorkerInit, TypedWorkerRpc,
};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::ControlEvent;
use crate::FileMetaRecord;
use crate::query::models::QueryNode;
use crate::query::path::is_under_query_path;
use crate::query::path::root_file_name_bytes;
use crate::query::request::{InternalQueryRequest, MaterializedQueryPayload, QueryOp, QueryScope};
use crate::runtime::execution_units::{
    SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use crate::runtime::routes::{
    ROUTE_KEY_QUERY, ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
    ROUTE_KEY_SOURCE_ROOTS_CONTROL,
};
use crate::sink::SinkFileMeta;
use crate::workers::sink::SinkWorkerClientHandle;
#[cfg(target_os = "linux")]
mod real_nfs_lab {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_app_real_nfs_lab.rs"
    ));
}

#[derive(Default)]
struct LoopbackWorkerBoundary {
    channels: AsyncMutex<HashMap<String, Vec<Event>>>,
    closed: StdMutex<HashSet<String>>,
    changed: Notify,
}

struct SourceWorkerUpdateRootsHookReset;

impl Drop for SourceWorkerUpdateRootsHookReset {
    fn drop(&mut self) {
        clear_source_worker_update_roots_hook();
    }
}

struct SourceWorkerUpdateRootsErrorHookReset;

impl Drop for SourceWorkerUpdateRootsErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_update_roots_error_hook();
    }
}

struct SourceWorkerLogicalRootsSnapshotHookReset;

impl Drop for SourceWorkerLogicalRootsSnapshotHookReset {
    fn drop(&mut self) {
        clear_source_worker_logical_roots_snapshot_hook();
    }
}

struct SourceWorkerControlFrameErrorHookReset;

impl Drop for SourceWorkerControlFrameErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_error_hook();
    }
}

struct SourceWorkerControlFrameErrorQueueHookReset;

impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_error_queue_hook();
    }
}

struct SourceWorkerStatusErrorHookReset;

impl Drop for SourceWorkerStatusErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_status_error_hook();
    }
}

struct SourceWorkerObservabilityErrorHookReset;

impl Drop for SourceWorkerObservabilityErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_observability_error_hook();
    }
}

struct SourceWorkerObservabilityCallCountHookReset;

impl Drop for SourceWorkerObservabilityCallCountHookReset {
    fn drop(&mut self) {
        clear_source_worker_observability_call_count_hook();
    }
}

struct SourceWorkerScheduledGroupsErrorHookReset;

impl Drop for SourceWorkerScheduledGroupsErrorHookReset {
    fn drop(&mut self) {
        clear_source_worker_scheduled_groups_error_hook();
    }
}

struct SourceWorkerControlFramePauseHookReset;

impl Drop for SourceWorkerControlFramePauseHookReset {
    fn drop(&mut self) {
        clear_source_worker_control_frame_pause_hook();
    }
}

struct SourceWorkerStartPauseHookReset;

impl Drop for SourceWorkerStartPauseHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_pause_hook();
    }
}

struct SourceWorkerStartErrorQueueHookReset;

impl Drop for SourceWorkerStartErrorQueueHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_error_queue_hook();
    }
}

struct SourceWorkerStartDelayHookReset;

impl Drop for SourceWorkerStartDelayHookReset {
    fn drop(&mut self) {
        clear_source_worker_start_delay_hook();
    }
}

#[async_trait]
impl ChannelIoSubset for LoopbackWorkerBoundary {
    async fn channel_send(&self, _ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
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

impl ChannelBoundary for LoopbackWorkerBoundary {
    fn log(&self, _ctx: BoundaryContext, _level: LogLevel, _msg: &str) {}
}

impl StateBoundary for LoopbackWorkerBoundary {}

const WORKER_BOOTSTRAP_CONTROL_FRAME_KIND: &str = "capanix.worker.bootstrap:v1";

#[derive(Debug, Clone, serde::Serialize)]
enum TestWorkerBootstrapEnvelope<P> {
    Init { node_id: String, payload: P },
}

fn bootstrap_envelope<P: serde::Serialize>(
    message: &TestWorkerBootstrapEnvelope<P>,
) -> ControlEnvelope {
    ControlEnvelope::Frame(ControlFrame {
        kind: WORKER_BOOTSTRAP_CONTROL_FRAME_KIND.to_string(),
        payload: rmp_serde::to_vec_named(message).expect("encode bootstrap envelope"),
    })
}

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

fn worker_socket_tmp_root() -> PathBuf {
    let repo_root = fs_meta_runtime_workspace_root()
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(fs_meta_runtime_workspace_root);
    let dir = repo_root.join(".tmp").join("w");
    std::fs::create_dir_all(&dir).expect("create worker socket temp root");
    dir
}

fn worker_socket_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("t")
        .tempdir_in(worker_socket_tmp_root())
        .expect("create worker socket dir")
}

fn fs_meta_worker_module_path_candidates(root: &Path, lib_name: &str) -> [PathBuf; 4] {
    [
        root.join("target/debug").join(lib_name),
        root.join("target/debug/deps").join(lib_name),
        root.join(".target/debug").join(lib_name),
        root.join(".target/debug/deps").join(lib_name),
    ]
}

fn newest_existing_worker_module_path(
    candidates: impl IntoIterator<Item = PathBuf>,
) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, usize, PathBuf)> = None;
    for (index, candidate) in candidates.into_iter().enumerate() {
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            continue;
        };
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let replace = match best.as_ref() {
            None => true,
            Some((best_modified, best_index, _)) => {
                modified > *best_modified || (modified == *best_modified && index < *best_index)
            }
        };
        if replace {
            best = Some((modified, index, candidate));
        }
    }
    best.map(|(_, _, path)| path)
}

fn resolve_fs_meta_worker_module_path_from_workspace_root(root: &Path) -> Option<PathBuf> {
    newest_existing_worker_module_path(fs_meta_worker_module_path_candidates(
        root,
        fs_meta_runtime_lib_filename(),
    ))
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
        resolve_fs_meta_worker_module_path_from_workspace_root(&fs_meta_runtime_workspace_root())
            .unwrap_or_else(|| {
                panic!("fs-meta worker module not found; set CAPANIX_FS_META_APP_BINARY")
            })
    })
    .clone()
}

fn external_source_worker_binding(socket_dir: &Path) -> RuntimeWorkerBinding {
    external_source_worker_binding_with_module_path(socket_dir, &fs_meta_worker_module_path())
}

fn external_source_worker_binding_with_module_path(
    socket_dir: &Path,
    module_path: &Path,
) -> RuntimeWorkerBinding {
    RuntimeWorkerBinding {
        role_id: "source".to_string(),
        mode: WorkerMode::External,
        launcher_kind: RuntimeWorkerLauncherKind::WorkerHost,
        module_path: Some(module_path.to_path_buf()),
        socket_dir: Some(socket_dir.to_path_buf()),
    }
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

fn worker_source_root(id: &str, path: &Path) -> RootSpec {
    let mut root = RootSpec::new(id, path);
    root.watch = false;
    root.scan = true;
    root
}

fn worker_watch_scan_root(id: &str, path: &Path) -> RootSpec {
    RootSpec::new(id, path)
}

#[test]
fn fs_meta_worker_module_path_prefers_newer_debug_deps_cdylib_over_stale_top_level_debug_cdylib() {
    let tmp = tempdir().expect("create temp dir");
    let lib_name = fs_meta_runtime_lib_filename();
    let stale = tmp.path().join("target/debug").join(lib_name);
    let fresh = tmp.path().join("target/debug/deps").join(lib_name);
    std::fs::create_dir_all(stale.parent().expect("stale parent")).expect("create stale dir");
    std::fs::create_dir_all(fresh.parent().expect("fresh parent")).expect("create fresh dir");
    std::fs::write(&stale, b"stale").expect("write stale module");
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(&fresh, b"fresh").expect("write fresh module");

    let resolved = resolve_fs_meta_worker_module_path_from_workspace_root(tmp.path())
        .expect("resolve worker module path");

    assert_eq!(
        resolved, fresh,
        "external worker tests must select the freshest built fs-meta cdylib instead of a stale top-level debug artifact"
    );
}

fn worker_fs_source_watch_scan_root(id: &str, fs_source: &str) -> RootSpec {
    RootSpec {
        id: id.to_string(),
        selector: crate::source::config::RootSelector {
            mount_point: None,
            fs_source: Some(fs_source.to_string()),
            fs_type: None,
            host_ip: None,
            host_ref: None,
        },
        subpath_scope: PathBuf::from("/"),
        watch: true,
        scan: true,
        audit_interval_ms: None,
    }
}

fn worker_source_export(
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

fn worker_source_export_with_fs_source(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: PathBuf,
    fs_source: &str,
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
        fs_source: fs_source.to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }
}

fn worker_route_export(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: &Path,
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
            mount_point: mount_point.display().to_string(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
        },
        grant_state: RuntimeHostGrantState::Active,
    }
}

fn worker_route_export_with_fs_source(
    object_ref: &str,
    host_ref: &str,
    host_ip: &str,
    mount_point: &Path,
    fs_source: &str,
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
            mount_point: mount_point.display().to_string(),
            fs_source: fs_source.to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
        },
        grant_state: RuntimeHostGrantState::Active,
    }
}

fn bound_scope_with_resources(scope_id: &str, resource_ids: &[&str]) -> RuntimeBoundScope {
    RuntimeBoundScope {
        scope_id: scope_id.to_string(),
        resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
    }
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

fn selected_group_force_find_request(group_id: &str) -> InternalQueryRequest {
    InternalQueryRequest::force_find(
        QueryOp::Tree,
        QueryScope {
            path: b"/".to_vec(),
            recursive: true,
            max_depth: None,
            selected_group: Some(group_id.to_string()),
        },
    )
}

fn selected_group_tree_contains_path(
    sink: &SinkFileMeta,
    group_id: &str,
    query_path: &[u8],
    expected_path: &[u8],
) -> bool {
    let events = sink
        .materialized_query(&selected_group_request(query_path, group_id))
        .expect("selected-group materialized query");
    events.iter().any(|event| {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .expect("decode selected-group materialized payload");
        let MaterializedQueryPayload::Tree(response) = payload else {
            return false;
        };
        (response.root.exists && response.root.path.as_slice() == expected_path)
            || response
                .entries
                .iter()
                .any(|entry| entry.path.as_slice() == expected_path)
    })
}

fn decode_exact_query_node(events: Vec<Event>, path: &[u8]) -> Result<Option<QueryNode>> {
    let mut selected = None::<QueryNode>;
    for event in &events {
        let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
            .map_err(|e| CnxError::Internal(format!("decode query response failed: {e}")))?;
        let MaterializedQueryPayload::Tree(response) = payload else {
            return Err(CnxError::Internal(
                "unexpected stats payload for query_node".into(),
            ));
        };
        let mut consider = |node: QueryNode| {
            if node.path != path {
                return;
            }
            match selected.as_mut() {
                Some(existing) if existing.modified_time_us > node.modified_time_us => {}
                Some(existing) => *existing = node,
                None => selected = Some(node),
            }
        };
        if response.root.exists {
            consider(QueryNode {
                path: response.root.path.clone(),
                file_name: root_file_name_bytes(&response.root.path),
                size: response.root.size,
                modified_time_us: response.root.modified_time_us,
                is_dir: response.root.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
        for entry in response.entries {
            consider(QueryNode {
                path: entry.path.clone(),
                file_name: root_file_name_bytes(&entry.path),
                size: entry.size,
                modified_time_us: entry.modified_time_us,
                is_dir: entry.is_dir,
                monitoring_attested: response.reliability.reliable,
                is_suspect: false,
                is_blind_spot: false,
            });
        }
    }
    Ok(selected)
}

async fn recv_loopback_events(
    boundary: &LoopbackWorkerBoundary,
    timeout_ms: u64,
) -> Result<Vec<Event>> {
    boundary
        .channel_recv(
            BoundaryContext::default(),
            ChannelRecvRequest {
                channel_key: ChannelKey("fs-meta.events:v1.stream".to_string()),
                timeout_ms: Some(timeout_ms),
            },
        )
        .await
}

fn record_control_and_data_counts(
    control_counts: &mut std::collections::BTreeMap<String, usize>,
    data_counts: &mut std::collections::BTreeMap<String, usize>,
    batch: Vec<Event>,
) {
    for event in batch {
        let origin = event.metadata().origin_id.0.clone();
        if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
            *control_counts.entry(origin).or_insert(0) += 1;
        } else {
            *data_counts.entry(origin).or_insert(0) += 1;
        }
    }
}

fn record_path_data_counts(
    path_counts: &mut std::collections::BTreeMap<String, usize>,
    batch: &[Event],
    target: &[u8],
) {
    for event in batch {
        let Ok(record) = rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) else {
            continue;
        };
        if is_under_query_path(&record.path, target) {
            *path_counts
                .entry(event.metadata().origin_id.0.clone())
                .or_insert(0) += 1;
        }
    }
}

#[test]
fn source_worker_rpc_preserves_invalid_input_response_category() {
    let err = SourceWorkerRpc::into_result(SourceWorkerResponse::InvalidInput(
        "duplicate source root id 'dup'".to_string(),
    ))
    .expect_err("invalid input response must become an error");
    match err {
        CnxError::InvalidInput(message) => assert!(message.contains("duplicate")),
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn degraded_worker_observability_uses_cached_snapshot() {
    let cache = SourceWorkerSnapshotCache {
        lifecycle_state: Some("ready".to_string()),
        last_live_observability_snapshot_at: None,
        host_object_grants_version: Some(7),
        grants: Some(vec![GrantedMountRoot {
            object_ref: "obj-a".to_string(),
            host_ref: "host-a".to_string(),
            host_ip: "10.0.0.1".to_string(),
            host_name: None,
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: PathBuf::from("/mnt/nfs-a"),
            fs_source: "nfs://server/export".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: vec![],
            interfaces: vec![],
            active: true,
        }]),
        logical_roots: Some(vec![RootSpec::new("root-a", "/mnt/nfs-a")]),
        status: Some(SourceStatusSnapshot {
            current_stream_generation: None,
            logical_roots: vec![],
            concrete_roots: vec![],
            degraded_roots: vec![("existing-root".to_string(), "already degraded".to_string())],
        }),
        source_primary_by_group: Some(std::collections::BTreeMap::from([(
            "group-a".to_string(),
            "obj-a".to_string(),
        )])),
        last_force_find_runner_by_group: Some(std::collections::BTreeMap::from([(
            "group-a".to_string(),
            "obj-a".to_string(),
        )])),
        force_find_inflight_groups: Some(vec!["group-a".to_string()]),
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["group-a".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-a".to_string(),
            vec!["group-a".to_string()],
        )])),
        last_control_frame_signals_by_node: Some(std::collections::BTreeMap::new()),
        published_batches_by_node: Some(std::collections::BTreeMap::new()),
        published_events_by_node: Some(std::collections::BTreeMap::new()),
        published_control_events_by_node: Some(std::collections::BTreeMap::new()),
        published_data_events_by_node: Some(std::collections::BTreeMap::new()),
        last_published_at_us_by_node: Some(std::collections::BTreeMap::new()),
        last_published_origins_by_node: Some(std::collections::BTreeMap::new()),
        published_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        pending_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        yielded_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        summarized_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
        published_path_origin_counts_by_node: Some(std::collections::BTreeMap::new()),
    };

    let snapshot =
        build_degraded_worker_observability_snapshot(&cache, "source worker unavailable");

    assert_eq!(snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE);
    assert_eq!(snapshot.host_object_grants_version, 7);
    assert_eq!(snapshot.grants.len(), 1);
    assert_eq!(snapshot.logical_roots.len(), 1);
    assert_eq!(
        snapshot.source_primary_by_group.get("group-a"),
        Some(&"obj-a".to_string())
    );
    assert_eq!(
        snapshot.last_force_find_runner_by_group.get("group-a"),
        Some(&"obj-a".to_string())
    );
    assert_eq!(
        snapshot.force_find_inflight_groups,
        vec!["group-a".to_string()]
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["group-a".to_string()])
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["group-a".to_string()])
    );
    assert_eq!(snapshot.status.degraded_roots.len(), 2);
    assert_eq!(
        snapshot.status.degraded_roots[1],
        (
            SOURCE_WORKER_DEGRADED_ROOT_KEY.to_string(),
            "source worker unavailable".to_string()
        )
    );
}

#[test]
fn successful_refresh_does_not_clear_cached_scheduled_groups_when_latest_publication_is_empty() {
    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    update_cached_scheduled_groups_from_refresh(
        &mut cache,
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::new(),
    );

    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not erase the last known non-empty scheduled source publication when the immediate refresh snapshot is transiently empty",
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not erase the last known non-empty scheduled scan publication when the immediate refresh snapshot is transiently empty",
    );
}

#[test]
fn successful_refresh_does_not_drop_cached_scheduled_groups_when_latest_publication_is_partial() {
    let mut cache = SourceWorkerSnapshotCache {
        scheduled_source_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        scheduled_scan_groups_by_node: Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        ..SourceWorkerSnapshotCache::default()
    };

    update_cached_scheduled_groups_from_refresh(
        &mut cache,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
    );

    assert_eq!(
        cache.scheduled_source_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not drop previously accepted source scopes when the immediate live publication is still a strict subset",
    );
    assert_eq!(
        cache.scheduled_scan_groups_by_node,
        Some(std::collections::BTreeMap::from([(
            "node-b".to_string(),
            vec!["nfs1".to_string(), "nfs2".to_string()],
        )])),
        "a successful control refresh must not drop previously accepted scan scopes when the immediate live publication is still a strict subset",
    );
}

#[test]
fn post_ack_schedule_refresh_is_skipped_for_pure_host_grant_change_retries() {
    let signals = vec![SourceControlSignal::RuntimeHostGrantChange {
        changed: RuntimeHostGrantChange {
            version: 7,
            grants: Vec::new(),
        },
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.host-grant-change".to_string(),
            payload: Vec::new(),
        }),
    }];

    assert!(
        !source_control_signals_require_post_ack_schedule_refresh(&signals),
        "pure host-grant retry batches must stay grant-only and skip post-ack scheduled-group refresh work",
    );
}

#[test]
fn post_ack_schedule_refresh_is_skipped_for_cleanup_only_deactivate_tails() {
    let signals = vec![
        SourceControlSignal::Deactivate {
            unit: SourceRuntimeUnit::Source,
            route_key: ROUTE_KEY_QUERY.to_string(),
            generation: 5,
            envelope: ControlEnvelope::Frame(ControlFrame {
                kind: "test.cleanup-only.source".to_string(),
                payload: Vec::new(),
            }),
        },
        SourceControlSignal::Deactivate {
            unit: SourceRuntimeUnit::Scan,
            route_key: ROUTE_KEY_QUERY.to_string(),
            generation: 5,
            envelope: ControlEnvelope::Frame(ControlFrame {
                kind: "test.cleanup-only.scan".to_string(),
                payload: Vec::new(),
            }),
        },
    ];

    assert!(
        !source_control_signals_require_post_ack_schedule_refresh(&signals),
        "cleanup-only deactivate tails must stay local and skip post-ack scheduled-group refresh work",
    );
}

#[test]
fn post_ack_schedule_refresh_stays_enabled_for_activate_waves() {
    let signals = vec![SourceControlSignal::Activate {
        unit: SourceRuntimeUnit::Source,
        route_key: ROUTE_KEY_QUERY.to_string(),
        generation: 5,
        bound_scopes: vec![RuntimeBoundScope {
            scope_id: "nfs1".to_string(),
            resource_ids: vec!["node-a::nfs1".to_string()],
        }],
        envelope: ControlEnvelope::Frame(ControlFrame {
            kind: "test.activate".to_string(),
            payload: Vec::new(),
        }),
    }];

    assert!(
        source_control_signals_require_post_ack_schedule_refresh(&signals),
        "activate waves must keep driving post-ack scheduled-group refresh so stale shared clients are still discarded on replay-owned continuity gaps",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_start_converges_on_fresh_handle() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fresh external source worker start timed out: stderr={}",
                worker_stderr_excerpt(worker_socket_dir.path())
            )
        })
        .expect("start source worker");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_manual_rescan_signal_succeeds_without_started_worker_and_updates_signal_cell() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-a::nfs1",
            "node-a",
            "10.0.0.11",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary.clone());
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    let signal = SignalCell::from_state_boundary(
        crate::runtime::execution_units::SOURCE_RUNTIME_UNIT_ID,
        "manual_rescan",
        state_boundary,
    )
    .expect("construct manual rescan signal cell");
    let offset = signal.current_seq();

    client
        .publish_manual_rescan_signal()
        .await
        .expect("publish manual rescan signal without started worker");

    let (_next, updates) = signal
        .watch_since(offset)
        .await
        .expect("watch signal updates");
    assert_eq!(
        updates.len(),
        1,
        "manual rescan signal should emit exactly one update"
    );
    assert_eq!(updates[0].requested_by, "node-a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_manual_rescan_replays_baseline_batches_for_fresh_sink() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: {
            let mut root1 = RootSpec::new("nfs1", &nfs1);
            root1.watch = true;
            root1.scan = true;
            let mut root2 = RootSpec::new("nfs2", &nfs2);
            root2.watch = true;
            root2.scan = true;
            vec![root1, root2]
        },
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg.clone(),
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg.clone())
        .expect("init sink");
    let query_dir = b"/force-find-stress";
    let query_root = b"/force-find-stress";

    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => sink.send(&batch).await.expect("apply initial batch"),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        let nfs1_ready = selected_group_tree_contains_path(&sink, "nfs1", query_dir, query_root);
        let nfs2_ready = selected_group_tree_contains_path(&sink, "nfs2", query_dir, query_root);
        if nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(selected_group_tree_contains_path(
        &sink, "nfs1", query_dir, query_root
    ));
    assert!(selected_group_tree_contains_path(
        &sink, "nfs2", query_dir, query_root
    ));

    let fresh_sink = SinkFileMeta::with_boundaries(NodeId("node-a".to_string()), None, cfg)
        .expect("init fresh sink");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let replay_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < replay_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => fresh_sink.send(&batch).await.expect("apply replay batch"),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("baseline replay recv failed: {err}"),
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
        "manual rescan should replay baseline nfs1 entries for a fresh sink"
    );
    assert!(
        selected_group_tree_contains_path(&fresh_sink, "nfs2", query_dir, query_root),
        "manual rescan should replay baseline nfs2 entries for a fresh sink"
    );

    client.close().await.expect("close source worker");
    sink.close().await.expect("close sink");
    fresh_sink.close().await.expect("close fresh sink");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_emits_initial_and_manual_rescan_batches_for_each_primary_root() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: {
            let mut root1 = RootSpec::new("nfs1", &nfs1);
            root1.watch = true;
            root1.scan = true;
            let mut root2 = RootSpec::new("nfs2", &nfs2);
            root2.watch = true;
            root2.scan = true;
            vec![root1, root2]
        },
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    let initial_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < initial_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial batch recv failed: {err}"),
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
    assert!(nfs1_initial_data > 0, "nfs1 should emit initial data");
    assert!(nfs2_initial_data > 0, "nfs2 should emit initial data");

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
    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let rescan_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < rescan_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_control_and_data_counts(&mut control_counts, &mut data_counts, batch)
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan batch recv failed: {err}"),
        }
        let complete = control_counts.get("node-a::nfs1").copied().unwrap_or(0) >= 4
            && control_counts.get("node-a::nfs2").copied().unwrap_or(0) >= 4
            && data_counts.get("node-a::nfs1").copied().unwrap_or(0) > nfs1_initial_data
            && data_counts.get("node-a::nfs2").copied().unwrap_or(0) > nfs2_initial_data;
        if complete {
            break;
        }
    }

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

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_updates_last_runner_snapshot_and_observability() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let params = selected_group_force_find_request("nfs1");
    let first = client
        .force_find(params.clone())
        .await
        .expect("first force-find over worker");
    assert!(
        !first.is_empty(),
        "first worker force-find should return at least one response event"
    );
    let first_runner = client
        .last_force_find_runner_by_group_snapshot()
        .await
        .expect("first last-runner snapshot");
    assert_eq!(
        first_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1")
    );

    let second = client
        .force_find(params.clone())
        .await
        .expect("second force-find over worker");
    assert!(
        !second.is_empty(),
        "second worker force-find should return at least one response event"
    );
    let second_runner = client
        .last_force_find_runner_by_group_snapshot()
        .await
        .expect("second last-runner snapshot");
    assert_eq!(
        second_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp2")
    );

    let third = client
        .force_find(params)
        .await
        .expect("third force-find over worker");
    assert!(
        !third.is_empty(),
        "third worker force-find should return at least one response event"
    );
    let third_runner = client
        .last_force_find_runner_by_group_snapshot()
        .await
        .expect("third last-runner snapshot");
    assert_eq!(
        third_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1")
    );

    let observability = SourceFacade::Worker(client.clone().into())
        .observability_snapshot()
        .await
        .expect("worker observability snapshot after force-find");
    assert_eq!(
        observability
            .last_force_find_runner_by_group
            .get("nfs1")
            .map(String::as_str),
        Some("node-a::exp1")
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_preserves_last_runner_in_degraded_observability_after_worker_shutdown()
 {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-cache-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let events = client
        .force_find(selected_group_force_find_request("nfs1"))
        .await
        .expect("force-find over worker");
    assert!(
        !events.is_empty(),
        "worker force-find should return at least one response event"
    );

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker after force-find");

    let observability = SourceFacade::Worker(client.clone().into())
        .observability_snapshot_nonblocking()
        .await;
    assert_eq!(
        observability
            .last_force_find_runner_by_group
            .get("nfs1")
            .map(String::as_str),
        Some("node-a::exp1"),
        "degraded observability should preserve the last force-find runner captured before the worker became unavailable: {observability:?}"
    );
    assert_eq!(
        observability.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "worker shutdown after force-find should serve a degraded cached observability snapshot: {observability:?}"
    );

    let _ = client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_force_find_retries_unexpected_correlation_protocol_violation() {
    struct SourceWorkerForceFindErrorQueueHookReset;

    impl Drop for SourceWorkerForceFindErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_force_find_error_queue_hook();
        }
    }

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir =
        std::env::temp_dir().join(format!("fs-meta-worker-force-find-correlation-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerForceFindErrorQueueHookReset;
    install_source_worker_force_find_error_queue_hook(SourceWorkerForceFindErrorQueueHook {
        errs: std::collections::VecDeque::from([CnxError::PeerError(
            "bound route protocol violation: unexpected correlation_id in reply batch (78)"
                .to_string(),
        )]),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let events = tokio::time::timeout(
            Duration::from_secs(4),
            client.force_find(selected_group_force_find_request("nfs1")),
        )
        .await
        .expect(
            "force_find unexpected-correlation recovery should settle promptly instead of hanging on the stale worker instance",
        )
        .expect(
            "force_find should recover when a live shared source worker hits a transient unexpected-correlation protocol violation",
        );
    assert!(
        !events.is_empty(),
        "force_find should still return response events after unexpected-correlation recovery"
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_eq!(
        next_instance_id, previous_instance_id,
        "force_find unexpected-correlation recovery should retry on the live shared source worker instance instead of forcing a rebind"
    );

    let runner = client
        .last_force_find_runner_by_group_snapshot()
        .await
        .expect("last runner snapshot after unexpected-correlation recovery");
    assert_eq!(
        runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1"),
        "force_find should still update last-runner state after recovering from unexpected correlation"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_roots_snapshot_uses_cached_roots_when_worker_resets_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_roots = client
        .logical_roots_snapshot()
        .await
        .expect("initial logical roots snapshot");
    assert_eq!(
        initial_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "initial worker snapshot should expose the configured root"
    );

    client.close().await.expect("close source worker");

    let cached_roots = client
        .logical_roots_snapshot()
        .await
        .expect("cached logical roots snapshot after worker reset");
    assert_eq!(
        cached_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "logical_roots_snapshot should fall back to cached roots during restart handoff"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_object_grants_snapshot_uses_cached_grants_when_worker_resets_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-d".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_grants = client
        .host_object_grants_snapshot()
        .await
        .expect("initial host-object grants snapshot");
    assert_eq!(
        initial_grants
            .iter()
            .map(|grant| grant.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-d::nfs1"],
        "initial worker snapshot should expose the configured grant"
    );

    client.close().await.expect("close source worker");

    let cached_grants = client
        .host_object_grants_snapshot()
        .await
        .expect("cached host-object grants snapshot after worker reset");
    assert_eq!(
        cached_grants
            .iter()
            .map(|grant| grant.object_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["node-d::nfs1"],
        "host_object_grants_snapshot should fall back to cached grants during restart handoff"
    );
}

#[test]
fn cached_host_object_grants_snapshot_is_used_for_stale_drained_pid_errors() {
    let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
    assert!(
        can_use_cached_host_object_grants_snapshot(&err),
        "host_object_grants_snapshot should fall back to cached grants when a stale drained/fenced worker pid rejects new grant attachments"
    );
}

#[test]
fn cached_logical_roots_snapshot_is_used_for_stale_drained_pid_errors() {
    let err = CnxError::AccessDenied(
            "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                .to_string(),
        );
    assert!(
        can_use_cached_logical_roots_snapshot(&err),
        "logical_roots_snapshot should fall back to cached roots when a stale drained/fenced worker pid rejects new grant attachments"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_snapshot_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerStatusErrorHookReset;
    install_source_worker_status_error_hook(SourceWorkerStatusErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = client.status_snapshot().await.expect(
        "status_snapshot should retry a stale drained/fenced pid error and reach the live worker",
    );

    assert_eq!(
        snapshot.logical_roots.len(),
        1,
        "status snapshot should come back from the rebound live worker with the configured root"
    );
    assert!(
        snapshot.degraded_roots.is_empty(),
        "fresh live source worker snapshot should still decode after stale-pid retry"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before stale observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let snapshot = client.observability_snapshot_nonblocking().await;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a stale drained/fenced pid error and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after stale-pid retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after stale-pid retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after stale-pid retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_bound_route_unexpected_correlation_protocol_violation()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before correlation-mismatch observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::ProtocolViolation(
            "bound route protocol violation: unexpected correlation_id in reply batch (11)"
                .to_string(),
        ),
    });

    let snapshot = client.observability_snapshot_nonblocking().await;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a transient bound-route correlation mismatch and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after correlation-mismatch retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after correlation-mismatch retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after correlation-mismatch retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_retries_peer_bridge_stopped_errors_after_begin() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for initial source/scan schedule before bridge-stop observability retry: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::PeerError("transport closed: sidecar control bridge stopped".to_string()),
    });

    let snapshot = client.observability_snapshot_nonblocking().await;
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_ne!(
        snapshot.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "observability_snapshot_nonblocking should retry a peer bridge-stopped error after begin and reach the live worker"
    );
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled source groups after bridge-stopped retry: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected),
        "live observability snapshot should preserve scheduled scan groups after bridge-stopped retry: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
    assert!(
        snapshot.status.degraded_roots.is_empty(),
        "live observability snapshot after bridge-stopped retry should not degrade: {:?}",
        snapshot.status.degraded_roots
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_waits_for_inflight_update_logical_roots_control_op_before_shutting_down_worker() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    // update_logical_roots holds this same client-side control-op guard
    // while the worker RPC is in flight; close must not tear down the worker
    // bridge until that guard has drained.
    let inflight = client.begin_control_op();
    let close_task = tokio::spawn({
        let client = client.clone();
        async move { client.close().await }
    });
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        !close_task.is_finished(),
        "source worker close must wait for in-flight update_logical_roots before tearing down the worker bridge"
    );

    drop(inflight);
    close_task
        .await
        .expect("join close task")
        .expect("close source worker after update");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_replays_start_after_runtime_reverts_to_initialized_without_worker_receipt()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let primed_worker = client.client().await.expect("connect source worker");
    primed_worker
        .control_frames(
            &[bootstrap_envelope(&TestWorkerBootstrapEnvelope::Init {
                node_id: client.node_id.0.clone(),
                payload: SourceWorkerRpc::init_payload(&client.node_id, &client.config)
                    .expect("source worker init payload"),
            })],
            Duration::from_secs(5),
        )
        .await
        .expect("reinitialize worker without replaying Start");

    let not_ready = SourceWorkerClientHandle::call_worker(
        &primed_worker,
        SourceWorkerRequest::LogicalRootsSnapshot,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect_err("raw worker should require Start after direct Init replay");
    assert!(
        matches!(&not_ready, CnxError::PeerError(message) if message == "worker not initialized"),
        "direct bootstrap Init should leave the worker runtime unstarted until Start is replayed: {not_ready:?}"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        client.update_logical_roots(vec![
            worker_source_root("nfs1", &nfs1),
            worker_source_root("nfs2", &nfs2),
        ]),
    )
    .await
    .expect("update_logical_roots should not hang after raw Init replay")
    .expect("update_logical_roots should replay Start and reach the live worker");

    let roots = client
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after bootstrap replay");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-bootstrap-replay update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerUpdateRootsHookReset;
    install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let update_task = tokio::spawn({
        let client = client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            client
                .update_logical_roots(vec![
                    worker_source_root("nfs1", &nfs1),
                    worker_source_root("nfs2", &nfs2),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale worker bridge");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(4), update_task)
        .await
        .expect("update_logical_roots should reacquire a live worker client after handoff")
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
        "logical roots should reflect the post-handoff update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_enforces_total_timeout_when_worker_call_stalls_after_first_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode first-wave source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_exec_control(&RuntimeExecControl::Activate(
                            RuntimeExecActivate {
                                route_key: ROUTE_KEY_QUERY.to_string(),
                                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                                lease: None,
                                generation: 2,
                                expires_at_ms: 1,
                                bound_scopes: vec![
                                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                                ],
                            },
                        ))
                        .expect("encode second-wave source activate"),
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
            .expect("stalled source on_control_frame should resolve within the local total timeout budget")
            .expect("join stalled source on_control_frame task")
            .expect_err("stalled source on_control_frame should fail once its local timeout budget is exhausted");
    assert!(matches!(err, CnxError::Timeout), "err={err:?}");

    release.notify_waiters();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_reacquires_worker_client_after_transport_closes_mid_handoff() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
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
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source activate"),
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source-scan activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), control_task)
        .await
        .expect("on_control_frame should reacquire a live source worker client after handoff")
        .expect("join on_control_frame task")
        .expect("source on_control_frame after worker restart");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let scheduled_source = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scheduled_scan = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if scheduled_source == expected_groups && scheduled_scan == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < schedule_deadline,
            "timed out waiting for scheduled groups after source handoff retry: source={scheduled_source:?} scan={scheduled_scan:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
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
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
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
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode second-wave source activate"),
                ])
                .await
        }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge after first wave");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker after first-wave success");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), second_wave)
        .await
        .expect("second source control wave should reacquire a live worker client after reset")
        .expect("join second source control wave")
        .expect("second source control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled source groups should remain converged after second-wave retry: source={source_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn third_on_control_frame_reacquires_worker_client_after_first_and_second_waves_succeeded() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let full_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source-scan activate"),
    ];
    let source_only_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
            ],
        }))
        .expect("encode source-only activate"),
    ];

    client
        .on_control_frame(full_wave.clone())
        .await
        .expect("first source control wave should succeed");
    client
        .on_control_frame(source_only_wave.clone())
        .await
        .expect("second source-only control wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let third_wave = tokio::spawn({
        let client = client.clone();
        async move { client.on_control_frame(source_only_wave).await }
    });

    entered.notified().await;
    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown stale source worker bridge after two successful waves");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker restart timed out")
        .expect("restart source worker after two successful waves");
    release.notify_waiters();

    tokio::time::timeout(Duration::from_secs(8), third_wave)
        .await
        .expect("third source control wave should reacquire a live worker client after reset")
        .expect("join third source control wave")
        .expect("third source control wave after worker restart");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled source groups should remain converged after third-wave retry: source={source_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_retries_retryable_bridge_errors_after_failed_followup_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let _reset = SourceWorkerStartErrorQueueHookReset;
    install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
        errs: std::collections::VecDeque::from(vec![CnxError::PeerError(
            "transport closed: sidecar control bridge stopped".to_string(),
        )]),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect("source worker retry start timed out")
            .expect(
                "source worker start should recover from one retryable bridge reset after a follow-up failure",
            );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode retried source activate"),
        ])
        .await
        .expect("source control should still converge after retryable start recovery");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_retries_after_hung_ensure_started_attempt_from_failed_followup_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
        ])
        .await
        .expect("first source control wave should succeed");

    let _reset = SourceWorkerStartDelayHookReset;
    install_source_worker_start_delay_hook(SourceWorkerStartDelayHook {
        delays: std::collections::VecDeque::from([Duration::from_secs(10)]),
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
            .await
            .expect(
                "source worker start should recover after one hung ensure_started attempt from a follow-up failure",
            )
            .expect("source worker start should recover after one hung ensure_started attempt");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode retried source activate"),
        ])
        .await
        .expect("source control should still converge after hung start recovery");

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_recreates_shared_worker_client_when_retryable_start_resets_never_converge() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let failing_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerStartErrorQueueHookReset;
    install_source_worker_start_error_queue_hook(SourceWorkerStartErrorQueueHook {
        errs: std::collections::VecDeque::new(),
        sticky_worker_instance_id: Some(failing_instance_id),
        sticky_peer_err: Some("transport closed: sidecar control bridge stopped".to_string()),
    });

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start should swap off a permanently resetting shared worker client")
        .expect("source worker start should recover by recreating the shared worker client");

    assert_ne!(
        client.worker_instance_id_for_tests().await,
        failing_instance_id,
        "post-failure worker-ready recovery must recreate the shared worker client instead of retrying forever on the same permanently resetting instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_worker_bindings_share_started_source_worker_client_on_same_node() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let module_path = fs_meta_worker_module_path();
    let module_link = tmp.path().join("worker-module-link");
    std::os::unix::fs::symlink(&module_path, &module_link).expect("create worker module symlink");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let predecessor_socket_dir = worker_socket_tempdir();
    let successor_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            external_source_worker_binding(predecessor_socket_dir.path()),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding_with_module_path(
                successor_socket_dir.path(),
                &module_link,
            ),
            factory,
        )
        .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "predecessor source worker start timed out: predecessor_stderr={} successor_stderr={}",
                    worker_stderr_excerpt(predecessor_socket_dir.path()),
                    worker_stderr_excerpt(successor_socket_dir.path())
                )
            })
            .expect("start predecessor source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("successor existing client after predecessor start")
            .is_some(),
        "same-node successor handle should reuse the already-started source worker client even when socket_dir/module_path differ across runtime instances"
    );

    let roots = successor
        .logical_roots_snapshot()
        .await
        .expect("successor logical_roots_snapshot through shared started worker");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "successor handle should see the shared started worker state after predecessor start"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_runtime_factories_do_not_share_started_source_worker_client_on_same_node() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
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
    let worker_socket_dir = worker_socket_tempdir();
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            RuntimeWorkerClientFactory::new(
                boundary_a.clone(),
                boundary_a.clone(),
                state_boundary_a,
            ),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            binding,
            RuntimeWorkerClientFactory::new(
                boundary_b.clone(),
                boundary_b.clone(),
                state_boundary_b,
            ),
        )
        .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
        .await
        .expect("predecessor source worker start timed out")
        .expect("start predecessor source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("successor existing client after predecessor start")
            .is_none(),
        "a successor created through a distinct runtime worker factory must not inherit the predecessor's started source worker client"
    );
    assert!(
        !Arc::ptr_eq(&predecessor._shared, &successor._shared),
        "distinct runtime worker factories must not share one source worker handle state"
    );

    tokio::time::timeout(Duration::from_secs(8), successor.start())
        .await
        .expect("successor source worker start timed out")
        .expect("start successor source worker");

    assert_ne!(
        predecessor.worker_instance_id_for_tests().await,
        successor.worker_instance_id_for_tests().await,
        "distinct runtime worker factories must drive distinct source worker client instances"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recovery_starts_on_same_shared_source_handle_serialize_worker_ready_recovery() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), predecessor.start())
        .await
        .expect("source worker start timed out")
        .expect("start shared source worker");

    predecessor
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown shared source worker before recovery");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerStartPauseHookReset;
    install_source_worker_start_pause_hook(SourceWorkerStartPauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let predecessor_start = tokio::spawn({
        let predecessor = predecessor.clone();
        async move { predecessor.start().await }
    });

    entered.notified().await;

    let mut successor_start = tokio::spawn({
        let successor = successor.clone();
        async move { successor.start().await }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(600), &mut successor_start)
            .await
            .is_err(),
        "same-node concurrent recovery start must wait while the shared source worker is still mid worker-ready recovery"
    );

    release.notify_waiters();

    predecessor_start
        .await
        .expect("join predecessor recovery start")
        .expect("predecessor recovery start");
    successor_start
        .await
        .expect("join successor recovery start")
        .expect("successor recovery start");

    let roots = successor
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after serialized recovery start");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "shared source worker should still converge after serialized same-node recovery starts"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker");
    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_waits_for_shared_control_frame_handoff_before_dispatching_to_worker()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let control_client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct control source worker client"),
    );
    let update_client = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct update source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), control_client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let control_entered = Arc::new(Notify::new());
    let control_release = Arc::new(Notify::new());
    let _control_reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: control_entered.clone(),
        release: control_release.clone(),
    });

    let update_entered = Arc::new(Notify::new());
    let update_release = Arc::new(Notify::new());
    let _update_reset = SourceWorkerUpdateRootsHookReset;
    install_source_worker_update_roots_hook(SourceWorkerUpdateRootsHook {
        entered: update_entered.clone(),
        release: update_release.clone(),
    });

    let control_task = tokio::spawn({
        let control_client = control_client.clone();
        async move {
            control_client
                .on_control_frame(vec![
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source activate"),
                    encode_runtime_exec_control(&RuntimeExecControl::Activate(
                        RuntimeExecActivate {
                            route_key: ROUTE_KEY_QUERY.to_string(),
                            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                            lease: None,
                            generation: 2,
                            expires_at_ms: 1,
                            bound_scopes: vec![
                                bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                                bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                            ],
                        },
                    ))
                    .expect("encode source-scan activate"),
                ])
                .await
        }
    });

    control_entered.notified().await;

    let update_task = tokio::spawn({
        let update_client = update_client.clone();
        let nfs1 = nfs1.clone();
        let nfs2 = nfs2.clone();
        async move {
            update_client
                .update_logical_roots(vec![
                    worker_source_root("nfs1", &nfs1),
                    worker_source_root("nfs2", &nfs2),
                ])
                .await
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(600), update_entered.notified())
            .await
            .is_err(),
        "shared source update_logical_roots must not start dispatch while another handle is still mid-control-frame handoff on the same worker"
    );

    control_release.notify_waiters();
    control_task
        .await
        .expect("join control task")
        .expect("apply shared source control frame");

    update_entered.notified().await;
    update_release.notify_waiters();
    update_task
        .await
        .expect("join update task")
        .expect("update logical roots after shared control handoff");

    let roots = update_client
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after shared handoff");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-handoff update"
    );

    update_client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_keeps_shared_source_worker_client_alive_when_another_handle_still_exists() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs1",
            "node-d",
            "10.0.0.41",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let predecessor = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct predecessor source worker client"),
    );
    let successor = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct successor source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), successor.start())
        .await
        .expect("source worker start timed out")
        .expect("start shared source worker");

    assert!(
        successor
            .existing_client()
            .await
            .expect("existing client before predecessor close")
            .is_some(),
        "shared source worker must have a live client before predecessor close"
    );

    predecessor
        .close()
        .await
        .expect("close predecessor source worker handle");

    assert!(
        successor
            .existing_client()
            .await
            .expect("existing client after predecessor close")
            .is_some(),
        "closing one source handle must not tear down the shared worker client while a successor handle still exists"
    );

    let roots = successor
        .logical_roots_snapshot()
        .await
        .expect("successor source logical_roots_snapshot after predecessor close");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "shared source worker should stay usable for the successor after predecessor close"
    );

    successor
        .close()
        .await
        .expect("close successor source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerUpdateRootsErrorHookReset;
    install_source_worker_update_roots_error_hook(SourceWorkerUpdateRootsErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .update_logical_roots(vec![
                worker_source_root("nfs1", &nfs1),
                worker_source_root("nfs2", &nfs2),
            ])
            .await
            .expect(
                "update_logical_roots should retry a stale drained/fenced pid error and reach the live worker",
            );

    let roots = client
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after stale-pid retry");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2"],
        "logical roots should reflect the post-retry update"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_logical_roots_contraction_drops_removed_roots_from_snapshot_immediately() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    let nfs3 = tmp.path().join("nfs3");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::create_dir_all(&nfs3).expect("create nfs3 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_source_root("nfs1", &nfs1),
            worker_source_root("nfs2", &nfs2),
            worker_source_root("nfs3", &nfs3),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
            worker_source_export("node-d::nfs3", "node-d", "10.0.0.43", nfs3.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let initial_roots = client
        .logical_roots_snapshot()
        .await
        .expect("initial logical roots snapshot");
    assert_eq!(
        initial_roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1", "nfs2", "nfs3"],
        "fixture must start with all three logical roots present"
    );

    client
        .update_logical_roots(vec![worker_source_root("nfs1", &nfs1)])
        .await
        .expect("shrink logical roots to single root");

    let roots = client
        .logical_roots_snapshot()
        .await
        .expect("logical roots snapshot after contraction");
    assert_eq!(
        roots
            .iter()
            .map(|root| root.id.as_str())
            .collect::<Vec<_>>(),
        vec!["nfs1"],
        "source worker logical_roots_snapshot must drop removed roots immediately after contraction"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_retries_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &nfs1)],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(vec![
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source activate"),
                encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                    route_key: ROUTE_KEY_QUERY.to_string(),
                    unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                    lease: None,
                    generation: 2,
                    expires_at_ms: 1,
                    bound_scopes: vec![
                        bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                        bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                    ],
                }))
                .expect("encode source-scan activate"),
            ])
            .await
            .expect(
                "on_control_frame should retry a stale drained/fenced pid error and reach the live worker",
            );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups
            == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
            && scan_groups
                == std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should reflect the post-retry activation: source={:?} scan={:?}",
            source_groups,
            scan_groups
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_on_current_instance()
 {
    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two stale drained/fenced recv recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after stale drained/fenced recv recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_restart_deferred_retire_pending_source_roots_deactivate_fails_fast_after_repeated_bridge_reset_errors()
 {
    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-cleanup-failfast".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial source control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(
            SourceWorkerControlFrameErrorQueueHook {
                errs: std::collections::VecDeque::from(vec![
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge stopped".to_string(),
                    ),
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                            .to_string(),
                    ),
                    CnxError::PeerError(
                        "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                            .to_string(),
                    ),
                ]),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
            },
        );

    let cleanup_envelopes = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source roots deactivate"),
    ];
    assert!(
        is_restart_deferred_retire_pending_deactivate_batch(&cleanup_envelopes),
        "test precondition: source cleanup batch must be recognized as restart_deferred_retire_pending"
    );

    let started = std::time::Instant::now();
    let err = client
            .on_control_frame_with_timeouts_for_tests(
                cleanup_envelopes,
                Duration::from_millis(250),
                Duration::from_millis(50),
            )
            .await
            .expect_err(
                "single restart_deferred_retire_pending source roots deactivate should fail fast once bridge resets keep repeating",
            );

    assert!(
        !matches!(err, CnxError::Timeout),
        "bridge-reset cleanup lane should return the bridge transport error instead of exhausting the whole local timeout budget: {err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "bridge-reset cleanup lane should fail fast instead of spending the full local timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );
    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "bridge-reset cleanup lane must retire the broken shared source worker client before returning so the next generation-two control wave cannot reuse the same current instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_restart_deferred_retire_pending_source_cleanup_wave_through_source_facade_fails_fast_after_repeated_bridge_reset_errors()
 {
    use std::collections::VecDeque;

    struct SourceWorkerControlFrameErrorQueueHookReset;

    impl Drop for SourceWorkerControlFrameErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_control_frame_error_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-cleanup-wave-failfast".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial source control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(
        SourceWorkerControlFrameErrorQueueHook {
            errs: VecDeque::from(vec![
                CnxError::PeerError(
                    "transport closed: sidecar control bridge stopped".to_string(),
                ),
                CnxError::PeerError(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                        .to_string(),
                ),
                CnxError::PeerError(
                    "transport closed: sidecar control bridge closed: internal error: ipc read len: Connection reset by peer (os error 104)"
                        .to_string(),
                ),
            ]),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        },
    );

    let cleanup_signals = source_control_signals_from_envelopes(&vec![
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source roots deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source rescan-control deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source rescan deactivate"),
        encode_runtime_exec_control(&RuntimeExecControl::Deactivate(
            capanix_runtime_entry_sdk::control::RuntimeExecDeactivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: Some(ExecLeaseMetadata {
                    lease_epoch: None,
                    lease_expires_at_ms: Some(22),
                    drain_started_at_ms: Some(11),
                    drain_deadline_at_ms: Some(22),
                }),
                generation: 3,
                reason: "restart_deferred_retire_pending".to_string(),
            },
        ))
        .expect("encode source scan deactivate"),
    ])
    .expect("decode cleanup signals");

    let started = std::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(2),
        SourceFacade::Worker(client.clone().into()).apply_orchestration_signals_with_total_timeout(
            &cleanup_signals,
            SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        ),
    )
    .await
    .expect("cleanup wave should settle within the short source-owned timeout budget")
    .expect_err(
        "restart_deferred_retire_pending cleanup wave should fail fast once repeated bridge resets prove this lane is not making progress",
    );

    assert!(
        !matches!(err, CnxError::Timeout),
        "restart_deferred_retire_pending cleanup wave should return the bridge reset error instead of collapsing to timeout through the source-owned budget: err={err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "restart_deferred_retire_pending cleanup wave should fail fast instead of spending the full source-owned timeout budget: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_invalid_or_revoked_grant_attachment_token_error_on_current_instance()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
        err: CnxError::AccessDenied("invalid or revoked grant attachment token".to_string()),
    });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on an invalid or revoked grant attachment token",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client on invalid token",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two invalid-token recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after invalid-token recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_channel_closed_on_current_instance()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
        err: CnxError::ChannelClosed,
    });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on channel-closed current-instance errors",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after channel-closed current-instance errors",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two channel-closed recovery must reacquire a fresh shared source worker client instance"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_fs_source_selected_wave_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053506".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client when post-ack schedule refresh stalls on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover when post-ack schedule refresh forces a fresh shared worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after post-ack stale drained/fenced refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_only_followup_replays_retained_fs_source_selected_scopes_after_generation_two_reconnect()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053507".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    client
            .on_control_frame(source_wave(2))
            .await
            .expect("generation-two fs_source-selected control wave should succeed before the tick-only followup");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
        .on_control_frame(vec![
            encode_runtime_unit_tick(&RuntimeUnitTick {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                generation: 3,
                at_ms: 1,
            })
            .expect("encode source-roots tick"),
        ])
        .await
        .expect(
            "tick-only followup should recover after reconnecting the shared source worker client",
        );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "tick-only followup should reconnect to a fresh shared source worker client instance"
    );

    let raw = client
        .client()
        .await
        .expect("connect raw source worker client");
    let snapshot = match SourceWorkerClientHandle::call_worker(
        &raw,
        SourceWorkerRequest::ObservabilitySnapshot,
        SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
    )
    .await
    .expect("raw observability snapshot after tick-only reconnect")
    {
        SourceWorkerResponse::ObservabilitySnapshot(snapshot) => snapshot,
        other => {
            panic!("unexpected source worker response for raw observability snapshot: {other:?}")
        }
    };

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "tick-only followup after reconnect must replay retained source activate scopes into the fresh worker instead of leaving runtime-managed source groups empty"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "tick-only followup after reconnect must replay retained scan activate scopes into the fresh worker instead of leaving runtime-managed scan groups empty"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_only_followup_skips_external_worker_ipc_when_retained_active_routes_already_match_current_generation()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053507".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    client
        .on_control_frame(source_wave(2))
        .await
        .expect("generation-two fs_source-selected control wave should succeed before the tick-only followup");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _reset = SourceWorkerControlFramePauseHookReset;
    install_source_worker_control_frame_pause_hook(SourceWorkerControlFramePauseHook {
        entered: entered.clone(),
        release: release.clone(),
    });

    let control_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .on_control_frame_with_timeouts_for_tests(
                    vec![
                        encode_runtime_unit_tick(&RuntimeUnitTick {
                            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                            generation: 2,
                            at_ms: 1,
                        })
                        .expect("encode source-roots tick"),
                    ],
                    Duration::from_millis(150),
                    Duration::from_millis(150),
                )
                .await
        }
    });

    let entered_result = tokio::time::timeout(Duration::from_millis(250), entered.notified()).await;
    if entered_result.is_ok() {
        release.notify_waiters();
        panic!(
            "tick-only followup should not enter external worker on_control_frame RPC when retained active routes already match the current generation"
        );
    }

    tokio::time::timeout(Duration::from_secs(1), control_task)
        .await
        .expect("tick-only followup task should finish without waiting on external worker IPC")
        .expect("join tick-only followup task")
        .expect(
            "tick-only followup should complete from retained state without reopening worker IPC",
        );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-e-29778840745788278147383297".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-e::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-e::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-e::nfs1", "node-e", "10.0.0.51", &nfs1),
                worker_route_export("node-e::nfs2", "node-e", "10.0.0.52", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after real source route replay: source={source_groups:?} scan={scan_groups:?} grants_version={} grants={:?} logical_roots={:?} stderr={}",
                client
                    .host_object_grants_version_snapshot()
                    .await
                    .unwrap_or_default(),
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                client.logical_roots_snapshot().await.unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two real source route wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_sticky_stale_drained_fenced_pid_error_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29780730718931112664498177".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh stalls on a sticky stale drained/fenced pid recv error",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after real-source post-ack stale drained/fenced refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_timeout_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::collections::VecDeque::from([CnxError::Timeout]),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh times out",
            )
            .expect(
                "generation-two real source route replay should recover after forcing a fresh shared worker client on post-ack refresh timeout",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack timeout recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_four_envelope_wave_early_eof_after_begin_does_not_collapse_to_timeout()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29798506721722971770060801".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave_envelopes = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let source_wave = |generation| {
        source_control_signals_from_envelopes(&source_wave_envelopes(generation))
            .expect("decode source wave signals")
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave_envelopes(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
        errs: std::iter::repeat_with(|| {
            CnxError::PeerError(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            )
        })
        .take(64)
        .collect(),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let err = tokio::time::timeout(
        Duration::from_secs(2),
        SourceFacade::Worker(client.clone().into()).apply_orchestration_signals_with_total_timeout(
            &source_wave(2),
            SOURCE_WORKER_EXISTING_CLIENT_CONTROL_RPC_TIMEOUT,
        ),
    )
    .await
    .expect("generation-two real source route replay should settle within the short source-owned timeout budget")
    .expect_err(
        "generation-two real source route replay should surface a retryable bridge reset instead of succeeding through repeated early-eof bridge failures",
    );
    assert!(
        matches!(
            &err,
            CnxError::PeerError(message)
                if message.contains("transport closed") && message.contains("early eof")
        ),
        "generation-two real source route replay should fail fast with the retryable bridge reset instead of collapsing to timeout: err={err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_four_envelope_wave_timeout_like_reset_fails_fast_before_local_budget_exhaustion()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29798803859008206969765889".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
        errs: std::iter::repeat_with(|| CnxError::Timeout)
            .take(50000)
            .collect(),
        sticky_worker_instance_id: None,
        sticky_peer_err: None,
    });

    let queued_len = {
        let guard = match source_worker_control_frame_error_queue_hook_cell().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.as_ref().map(|hook| hook.errs.len())
    };
    assert_eq!(
        queued_len,
        Some(50000),
        "timeout-like reset hook must be installed before generation-two replay"
    );

    let started = std::time::Instant::now();
    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(1200),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route replay should fail once timeout-like bridge resets keep repeating",
        );

    assert!(
        started.elapsed() < Duration::from_millis(200),
        "generation-two real source route replay should fail fast instead of exhausting the local caller budget after timeout-like resets: elapsed={:?} err={err:?}",
        started.elapsed()
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_does_not_fail_closed_from_primed_cache_after_repeated_timeouts_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-repeated-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should settle after repeated post-ack refresh timeouts",
            )
            .expect(
                "generation-two real source route replay should recover instead of failing closed from a primed schedule cache after repeated post-ack refresh timeouts",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "repeated post-ack refresh timeouts must force a fresh shared source worker client instead of succeeding from a primed schedule cache",
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after repeated-timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_exhaustion_does_not_succeed_from_primed_cache()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-timeout-refresh-exhaustion".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(50_000)
                .collect(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route should not succeed from a primed schedule cache when post-ack scheduled-groups refresh exhausts",
        );

    let CnxError::Internal(message) = err else {
        panic!(
            "post-ack schedule refresh exhaustion should fail closed with a sharp internal error instead of a generic retryable timeout: {err:?}"
        );
    };
    assert!(
        message.contains("post-ack scheduled-groups refresh exhausted"),
        "post-ack schedule refresh exhaustion should surface a sharp internal cause, got: {message}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_schedule_rpc_timeout_fail_closes_with_convergence_timeout()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-schedule-rpc-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-b::nfs1", "node-b", "10.0.0.21", &nfs1),
                worker_route_export("node-b::nfs2", "node-b", "10.0.0.22", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| {
                CnxError::PeerError("operation timed out".to_string())
            })
            .take(50_000)
            .collect(),
            sticky_worker_instance_id: None,
            sticky_peer_err: None,
        },
    );

    let err = client
        .on_control_frame_with_timeouts_for_tests(
            source_wave(2),
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect_err(
            "generation-two real source route should fail closed when post-ack scheduled-groups refresh schedule RPCs keep returning operation timed out",
        );

    let CnxError::Internal(message) = err else {
        panic!(
            "schedule RPC timeouts during post-ack refresh should fail closed with a convergence timeout instead of surfacing a raw transport-shaped error: {err:?}"
        );
    };
    assert!(
        message.contains(
            "post-ack scheduled-groups refresh exhausted before scheduled groups converged (timeout)"
        ),
        "schedule RPC timeouts should classify as the timeout-shaped convergence failure, got: {message}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_replay_required_post_ack_schedule_refresh_reacquires_live_worker_after_repeated_timeouts()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-replay-required-timeout-refresh".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    client
        .reconnect_after_retryable_control_reset()
        .await
        .expect("force replay-required source worker rebind");
    let replay_required_instance_id = client.worker_instance_id_for_tests().await;

    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::iter::repeat_with(|| CnxError::Timeout)
                .take(64)
                .collect(),
            sticky_worker_instance_id: Some(replay_required_instance_id),
            sticky_peer_err: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should settle even when replay-required refresh keeps timing out",
            )
            .expect(
                "replay-required post-ack scheduled-groups refresh must reacquire a fresh live worker instead of exhausting after repeated timeouts",
            );
    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, replay_required_instance_id,
        "replay-required repeated timeout refresh must rotate to a fresh live worker instead of remaining stuck on the stalled replay-required instance",
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should converge after replay-required repeated-timeout refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_missing_route_state_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d-29780951151719537312792577".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-d::nfs1", "node-d", "10.0.0.41", &nfs1),
                worker_route_export("node-d::nfs2", "node-d", "10.0.0.42", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "source worker unavailable: missing route state for channel_buffer ChannelSlotId(4750)"
                        .to_string(),
                ),
            },
        );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh hits missing route state for channel_buffer",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after missing route state",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "real source route post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance after missing route state"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after missing-route-state refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_reacquires_worker_client_after_worker_not_initialized_during_post_ack_schedule_refresh()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29792434514518209017708545".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
        SourceWorkerScheduledGroupsRefreshErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some("worker not initialized".to_string()),
        },
    );

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two real source route replay should reacquire a fresh worker client when post-ack schedule refresh still sees worker not initialized",
            )
            .expect(
                "generation-two real source route replay should recover when post-ack schedule refresh forces a fresh shared worker client after worker-not-initialized",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "worker-not-initialized post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduled groups should remain converged after worker-not-initialized refresh recovery: source={source_groups:?} scan={scan_groups:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_detaches_stale_worker_client() {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29780730718931112664498177".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let stale_worker = client
        .client()
        .await
        .expect("connect stale source worker client");
    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    client.on_control_frame(source_wave(2)).await.expect(
        "generation-two real source route replay should recover after shared-client rebind",
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "post-ack scheduled-groups refresh recovery must reacquire a fresh shared source worker client instance"
    );

    let stale_err = SourceWorkerClientHandle::call_worker(
            &stale_worker,
            SourceWorkerRequest::LogicalRootsSnapshot,
            SOURCE_WORKER_CONTROL_RPC_TIMEOUT,
        )
        .await
        .expect_err(
            "stale pre-rebind source worker client must not keep serving requests after post-ack refresh forced a shared-client rebind",
        );
    assert!(
        matches!(
            stale_err,
            CnxError::TransportClosed(_) | CnxError::AccessDenied(_) | CnxError::PeerError(_)
        ),
        "stale pre-rebind source worker client should fail closed after shared-client rebind, got {stale_err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_two_real_source_route_post_ack_schedule_refresh_aborts_inflight_stale_prerebind_logical_roots_snapshot()
 {
    struct SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;

    impl Drop for SourceWorkerScheduledGroupsRefreshErrorQueueHookReset {
        fn drop(&mut self) {
            clear_source_worker_scheduled_groups_refresh_error_queue_hook();
        }
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29792561029320261914573329".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-a::nfs1", "node-a", "10.0.0.11", &nfs1),
                worker_route_export("node-a::nfs2", "node-a", "10.0.0.12", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial real source route wave should succeed");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let _logical_roots_reset = SourceWorkerLogicalRootsSnapshotHookReset;
    install_source_worker_logical_roots_snapshot_hook(SourceWorkerLogicalRootsSnapshotHook {
        roots: None,
        entered: Some(entered.clone()),
        release: Some(release.clone()),
    });

    let stale_snapshot = tokio::spawn({
        let client = client.clone();
        async move { client.logical_roots_snapshot().await }
    });

    entered.notified().await;

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _refresh_reset = SourceWorkerScheduledGroupsRefreshErrorQueueHookReset;
    install_source_worker_scheduled_groups_refresh_error_queue_hook(
            SourceWorkerScheduledGroupsRefreshErrorQueueHook {
                errs: std::collections::VecDeque::new(),
                sticky_worker_instance_id: Some(previous_instance_id),
                sticky_peer_err: Some(
                    "worker receive request failed: access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                        .to_string(),
                ),
            },
        );

    client.on_control_frame(source_wave(2)).await.expect(
        "generation-two real source route replay should recover after shared-client rebind",
    );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "inflight stale logical_roots_snapshot seam must first prove post-ack scheduled-groups refresh reacquired a fresh shared source worker client instance"
    );

    release.notify_waiters();

    let stale_result = tokio::time::timeout(Duration::from_secs(4), stale_snapshot)
        .await
        .expect("stale logical_roots_snapshot task should settle after rebind release")
        .expect("join stale logical_roots_snapshot task");
    let stale_err = stale_result.expect_err(
            "inflight stale pre-rebind logical_roots_snapshot must fail closed after generation-two shared-client rebind",
        );
    assert!(
        matches!(
            stale_err,
            CnxError::TransportClosed(_) | CnxError::AccessDenied(_) | CnxError::PeerError(_)
        ),
        "inflight stale pre-rebind logical_roots_snapshot should fail closed after shared-client rebind, got {stale_err:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_root_real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for single-root real source route replay after stale-pid retry: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial single-root control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two single-root real source route wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_source_selected_real_source_route_on_control_frame_retries_stale_drained_fenced_pid_errors_after_generation_one()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "nfs://server.example/export/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for fs_source-selected single-root replay after stale-pid retry: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    let _reset = SourceWorkerControlFrameErrorHookReset;
    install_source_worker_control_frame_error_hook(SourceWorkerControlFrameErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    client
            .on_control_frame(source_wave(2))
            .await
            .expect(
                "generation-two fs_source-selected single-root wave should retry a stale drained/fenced pid error and preserve runtime-managed source groups",
            );
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "generation-two stale-pid retry",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fs_source_selected_real_source_route_reacquires_worker_client_after_sticky_peer_early_eof_on_generation_two_wave()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    let nfs1_source = "nfs://server.example/export/nfs1";

    let cfg = SourceConfig {
        roots: vec![worker_fs_source_watch_scan_root("nfs1", nfs1_source)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-window-join".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
                nfs1_source,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(1))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial fs_source-selected single-root control wave should succeed");

    let previous_instance_id = client.worker_instance_id_for_tests().await;
    let _reset = SourceWorkerControlFrameErrorQueueHookReset;
    install_source_worker_control_frame_error_queue_hook(SourceWorkerControlFrameErrorQueueHook {
            errs: std::collections::VecDeque::new(),
            sticky_worker_instance_id: Some(previous_instance_id),
            sticky_peer_err: Some(
                "transport closed: sidecar control bridge closed: internal error: ipc read len: early eof"
                    .to_string(),
            ),
        });

    tokio::time::timeout(Duration::from_secs(4), client.on_control_frame(source_wave(2)))
            .await
            .expect(
                "generation-two fs_source-selected replay should reacquire a fresh worker client instead of stalling on a sticky early-eof bridge error",
            )
            .expect(
                "generation-two fs_source-selected replay should recover after reacquiring a fresh worker client from a sticky early-eof bridge error",
            );

    let next_instance_id = client.worker_instance_id_for_tests().await;
    assert_ne!(
        next_instance_id, previous_instance_id,
        "generation-two sticky early-eof recovery must reacquire a fresh shared source worker client instance"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        if source_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
            && scan_groups == std::collections::BTreeSet::from(["nfs1".to_string()])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "generation-two sticky early-eof recovery should keep single-root groups converged: source={source_groups:?} scan={scan_groups:?} stderr={}",
            worker_stderr_excerpt(worker_socket_dir.path()),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_group_snapshots_retry_stale_drained_fenced_pid_errors() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-d::nfs1", "node-d", "10.0.0.41", nfs1.clone()),
            worker_source_export("node-d::nfs2", "node-d", "10.0.0.42", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-d".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-d::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-d::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("initial scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("initial scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups before stale-pid retry test: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let _reset = SourceWorkerScheduledGroupsErrorHookReset;
    install_source_worker_scheduled_groups_error_hook(SourceWorkerScheduledGroupsErrorHook {
            err: CnxError::AccessDenied(
                "source worker unavailable: pid Pid(1) is drained/fenced and cannot obtain new grant attachments"
                    .to_string(),
            ),
        });

    let source_groups = client
        .scheduled_source_group_ids()
        .await
        .expect("scheduled_source_group_ids should retry stale drained/fenced pid errors");
    let scan_groups = client
        .scheduled_scan_group_ids()
        .await
        .expect("scheduled_scan_group_ids should still reach the live worker after retry");

    assert_eq!(source_groups, Some(expected_groups.clone()));
    assert_eq!(scan_groups, Some(expected_groups));

    client.close().await.expect("close source worker");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_find_via_node_shares_runner_state_with_existing_target_worker_handle() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros();
    let root_dir = std::env::temp_dir().join(format!("fs-meta-worker-force-find-share-{unique}"));
    std::fs::create_dir_all(&root_dir).expect("create root dir");
    std::fs::write(root_dir.join("file.txt"), b"ok").expect("seed file");

    let cfg = SourceConfig {
        roots: vec![worker_source_root("nfs1", &root_dir)],
        host_object_grants: vec![
            worker_source_export("node-a::exp1", "node-a", "10.0.0.11", root_dir.clone()),
            worker_source_export("node-a::exp2", "node-a", "10.0.0.12", root_dir.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let binding = external_source_worker_binding(worker_socket_dir.path());
    let target_client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a".to_string()),
            cfg.clone(),
            binding.clone(),
            factory.clone(),
        )
        .expect("construct target source worker client"),
    );
    let routing_client = Arc::new(
        SourceWorkerClientHandle::new(NodeId("node-d".to_string()), cfg, binding, factory)
            .expect("construct routing source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), target_client.start())
        .await
        .expect("target source worker start timed out")
        .expect("start target source worker");

    let routed = SourceFacade::worker(routing_client);
    let params = selected_group_force_find_request("nfs1");
    let first = routed
        .force_find_via_node(&NodeId("node-a".to_string()), &params)
        .await
        .expect("routed force-find via node");
    assert!(
        !first.is_empty(),
        "routed force-find via target node should return at least one response event"
    );

    let shared_runner = target_client
        .last_force_find_runner_by_group_snapshot()
        .await
        .expect("target handle last-runner snapshot");
    assert_eq!(
        shared_runner.get("nfs1").map(String::as_str),
        Some("node-a::exp1"),
        "force_find_via_node should update runner state visible through the already-started target worker handle"
    );

    target_client
        .close()
        .await
        .expect("close target source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_stream_batches_reach_sink_worker_for_each_scheduled_primary_root() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 dir");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 dir");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let source_factory = RuntimeWorkerClientFactory::new(
        boundary.clone(),
        boundary.clone(),
        in_memory_state_boundary(),
    );
    let sink_factory = RuntimeWorkerClientFactory::new(
        boundary.clone(),
        boundary.clone(),
        in_memory_state_boundary(),
    );
    let worker_socket_root = worker_socket_tempdir();
    let source_socket_dir = worker_socket_root.path().join("source");
    let sink_socket_dir = worker_socket_root.path().join("sink");
    std::fs::create_dir_all(&source_socket_dir).expect("create source socket dir");
    std::fs::create_dir_all(&sink_socket_dir).expect("create sink socket dir");
    let source = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg.clone(),
        external_source_worker_binding(&source_socket_dir),
        source_factory,
    )
    .expect("construct source worker client");
    let sink = SinkWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_sink_worker_binding(&sink_socket_dir),
        sink_factory,
    )
    .expect("construct sink worker client");

    tokio::time::timeout(Duration::from_secs(8), source.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    source
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 1,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source-scan activate"),
        ])
        .await
        .expect("activate source worker");
    sink.on_control_frame(vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: ROUTE_KEY_QUERY.to_string(),
            unit_id: SINK_RUNTIME_UNIT_ID.to_string(),
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
    .expect("activate sink worker");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let scheduling_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = source
            .scheduled_source_group_ids()
            .await
            .expect("source groups")
            .unwrap_or_default();
        let scan_groups = source
            .scheduled_scan_group_ids()
            .await
            .expect("scan groups")
            .unwrap_or_default();
        let sink_groups = sink
            .status_snapshot()
            .await
            .expect("sink status")
            .scheduled_groups_by_node
            .get("node-a")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if source_groups == expected_groups
            && scan_groups == expected_groups
            && sink_groups == expected_groups
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < scheduling_deadline,
            "timed out waiting for source/sink schedule convergence: source={source_groups:?} scan={scan_groups:?} sink={sink_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    let selected_file = b"/force-find-stress/seed.txt";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                let mut control = 0usize;
                for event in &batch {
                    let origin = event.metadata().origin_id.0.clone();
                    if rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).is_ok() {
                        *control_counts.entry(origin).or_insert(0) += 1;
                        control += 1;
                    } else {
                        *data_counts.entry(origin).or_insert(0) += 1;
                    }
                }
                if control < batch.len() {
                    sink.send(batch)
                        .await
                        .expect("forward source batch to sink");
                }
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("stream batch recv failed: {err}"),
        }
        let complete = ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied().unwrap_or(0) > 0
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        });
        let nfs1_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1"),
            selected_file,
        )
        .expect("decode nfs1")
        .is_some();
        let nfs2_ready = decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2"),
            selected_file,
        )
        .expect("decode nfs2")
        .is_some();
        if complete && nfs1_ready && nfs2_ready {
            break;
        }
    }

    assert!(
        control_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "nfs1 should publish at least one control event on the external source stream: {control_counts:?}"
    );
    assert!(
        control_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "nfs2 should publish at least one control event on the external source stream: {control_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs1").copied().unwrap_or(0) > 0,
        "nfs1 should produce at least one data batch on the external source stream: {data_counts:?}"
    );
    assert!(
        data_counts.get("node-a::nfs2").copied().unwrap_or(0) > 0,
        "nfs2 should produce at least one data batch on the external source stream: {data_counts:?}"
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs1"))
                .await
                .expect("query nfs1 final"),
            selected_file,
        )
        .expect("decode nfs1 final")
        .is_some(),
        "nfs1 should materialize after forwarding external source batches into sink.send(...)",
    );
    assert!(
        decode_exact_query_node(
            sink.materialized_query(selected_group_request(selected_file, "nfs2"))
                .await
                .expect("query nfs2 final"),
            selected_file,
        )
        .expect("decode nfs2 final")
        .is_some(),
        "nfs2 should materialize after forwarding external source batches into sink.send(...)",
    );

    source.close().await.expect("close source worker");
    sink.close().await.expect("close sink worker");
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
async fn external_source_worker_real_nfs_manual_rescan_publishes_newly_seeded_subtree_alongside_baseline_path()
 {
    let preflight = real_nfs_lab::RealNfsPreflight::detect();
    if !preflight.enabled {
        eprintln!(
            "skip real-nfs external source worker publish-path test: {}",
            preflight
                .reason
                .unwrap_or_else(|| "real-nfs preflight failed".to_string())
        );
        return;
    }

    let mut lab = real_nfs_lab::NfsLab::start().expect("start NFS lab");
    let nfs1 = lab
        .mount_export("node-a", "nfs1")
        .expect("mount node-a nfs1 export");
    let nfs2 = lab
        .mount_export("node-a", "nfs2")
        .expect("mount node-a nfs2 export");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let baseline_target = b"/data";
    let force_find_target = b"/force-find-stress";
    let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
    let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < baseline_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_path_data_counts(&mut baseline_counts, &batch, baseline_target);
                record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        let baseline_ready = ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0);
        if baseline_ready {
            break;
        }
    }

    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
        "baseline /data should publish for both local primary roots over the external source stream: {baseline_counts:?}"
    );
    assert!(
        force_find_counts.is_empty(),
        "new subtree target should stay absent before it is seeded and manually rescanned: {force_find_counts:?}"
    );

    lab.mkdir("nfs1", "force-find-stress")
        .expect("create nfs1 force-find dir");
    lab.mkdir("nfs2", "force-find-stress")
        .expect("create nfs2 force-find dir");
    lab.write_file("nfs1", "force-find-stress/seed.txt", "a\n")
        .expect("seed nfs1 force-find subtree");
    lab.write_file("nfs2", "force-find-stress/seed.txt", "b\n")
        .expect("seed nfs2 force-find subtree");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < force_find_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => {
                record_path_data_counts(&mut force_find_counts, &batch, force_find_target);
            }
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan publish recv failed: {err}"),
        }
        let subtree_ready = ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0);
        if subtree_ready {
            break;
        }
    }

    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
        "newly seeded /force-find-stress subtree should publish for both local primary roots after manual rescan over the external source stream: {force_find_counts:?}"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_nonblocking_observability_can_serve_stale_published_path_counts_during_control_inflight()
 {
    let previous = std::env::var("FSMETA_DEBUG_STREAM_PATH_CAPTURE").ok();
    unsafe {
        std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", "/force-find-stress");
    }

    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(nfs1.join("data")).expect("create nfs1 data dir");
    std::fs::create_dir_all(nfs2.join("data")).expect("create nfs2 data dir");
    std::fs::write(nfs1.join("data").join("a.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("data").join("b.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let baseline_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let baseline_target = b"/data";
    let mut baseline_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < baseline_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => record_path_data_counts(&mut baseline_counts, &batch, baseline_target),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("initial publish recv failed: {err}"),
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }
    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| baseline_counts.get(*origin).copied().unwrap_or(0) > 0),
        "baseline /data should publish for both local primary roots: {baseline_counts:?}"
    );

    let primed_worker = client.client().await.expect("connect source worker");
    let primed = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime cached observability snapshot");
    assert_eq!(
        primed.published_path_capture_target.as_deref(),
        Some("/force-find-stress"),
        "primed snapshot should carry the configured path target"
    );
    assert!(
        primed
            .published_path_origin_counts_by_node
            .get("node-a")
            .is_none_or(Vec::is_empty),
        "before seeding subtree, cached path counts should be empty: {:?}",
        primed.published_path_origin_counts_by_node
    );

    std::fs::create_dir_all(nfs1.join("force-find-stress")).expect("create nfs1 subtree");
    std::fs::create_dir_all(nfs2.join("force-find-stress")).expect("create nfs2 subtree");
    std::fs::write(nfs1.join("force-find-stress").join("seed.txt"), b"aa")
        .expect("seed nfs1 subtree");
    std::fs::write(nfs2.join("force-find-stress").join("seed.txt"), b"bb")
        .expect("seed nfs2 subtree");

    tokio::time::timeout(
        Duration::from_secs(8),
        client.publish_manual_rescan_signal(),
    )
    .await
    .expect("manual rescan publish timed out")
    .expect("publish manual rescan");

    let force_find_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let force_find_target = b"/force-find-stress";
    let mut force_find_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < force_find_deadline {
        match recv_loopback_events(&boundary, 250).await {
            Ok(batch) => record_path_data_counts(&mut force_find_counts, &batch, force_find_target),
            Err(CnxError::Timeout) => continue,
            Err(err) => panic!("manual rescan publish recv failed: {err}"),
        }
        if ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0)
        {
            break;
        }
    }
    assert!(
        ["node-a::nfs1", "node-a::nfs2"]
            .iter()
            .all(|origin| force_find_counts.get(*origin).copied().unwrap_or(0) > 0),
        "manual rescan should publish /force-find-stress for both roots before stale-observability check: {force_find_counts:?}"
    );

    let inflight = client.begin_control_op();
    let stale = client.observability_snapshot_nonblocking().await;
    drop(inflight);
    assert_eq!(
        stale.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "control-inflight nonblocking snapshot should use cached fallback"
    );
    assert!(
        stale
            .published_path_origin_counts_by_node
            .get("node-a")
            .is_none_or(Vec::is_empty),
        "cached fallback should still reflect stale pre-rescan path counts: {:?}",
        stale.published_path_origin_counts_by_node
    );

    let live_worker = client.client().await.expect("reconnect source worker");
    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("fetch live post-rescan observability snapshot");
    let live_counts = live
        .published_path_origin_counts_by_node
        .get("node-a")
        .cloned()
        .unwrap_or_default();
    assert!(
        live_counts
            .iter()
            .any(|entry| entry.starts_with("node-a::nfs1=")),
        "live worker snapshot should include nfs1 /force-find-stress path counts: {live_counts:?}"
    );
    assert!(
        live_counts
            .iter()
            .any(|entry| entry.starts_with("node-a::nfs2=")),
        "live worker snapshot should include nfs2 /force-find-stress path counts after rescan: {live_counts:?}"
    );

    client.close().await.expect("close source worker");
    match previous {
        Some(value) => unsafe { std::env::set_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE", value) },
        None => unsafe { std::env::remove_var("FSMETA_DEBUG_STREAM_PATH_CAPTURE") },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_nonblocking_observability_preserves_scheduled_groups_after_successful_control_before_next_inflight()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let primed_worker = client.client().await.expect("connect source worker");
    let primed = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime cached observability snapshot");
    assert!(
        primed.scheduled_source_groups_by_node.is_empty()
            && primed.scheduled_scan_groups_by_node.is_empty(),
        "baseline cached observability should start empty: source={:?} scan={:?}",
        primed.scheduled_source_groups_by_node,
        primed.scheduled_scan_groups_by_node
    );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let expected = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    let local_source_groups = client
        .scheduled_source_group_ids()
        .await
        .expect("scheduled source groups")
        .unwrap_or_default();
    let local_scan_groups = client
        .scheduled_scan_group_ids()
        .await
        .expect("scheduled scan groups")
        .unwrap_or_default();
    assert_eq!(
        local_source_groups,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
    );
    assert_eq!(
        local_scan_groups,
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
    );

    let inflight = client.begin_control_op();
    let stale = client.observability_snapshot_nonblocking().await;
    drop(inflight);
    assert_eq!(
        stale.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "control-inflight nonblocking snapshot should still use cached fallback"
    );
    assert_eq!(
        stale.scheduled_source_groups_by_node, expected,
        "cached fallback should preserve latest scheduled source groups after successful control"
    );
    assert_eq!(
        stale.scheduled_scan_groups_by_node, expected,
        "cached fallback should preserve latest scheduled scan groups after successful control"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonblocking_observability_reissues_live_rpc_when_recent_cache_is_active_but_debug_empty()
{
    let _hook_reset = SourceWorkerObservabilityCallCountHookReset;
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    assert_eq!(
        live.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()])
    );
    assert!(
        live
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "live snapshot should carry accepted control summary before cache corruption: {:?}",
        live.last_control_frame_signals_by_node
    );

    let mut incomplete = live.clone();
    incomplete.scheduled_source_groups_by_node.clear();
    incomplete.scheduled_scan_groups_by_node.clear();
    incomplete.last_control_frame_signals_by_node.clear();
    incomplete.published_batches_by_node.clear();
    incomplete.published_events_by_node.clear();
    incomplete.published_control_events_by_node.clear();
    incomplete.published_data_events_by_node.clear();
    incomplete.last_published_at_us_by_node.clear();
    incomplete.last_published_origins_by_node.clear();
    incomplete.published_origin_counts_by_node.clear();
    incomplete.enqueued_path_origin_counts_by_node.clear();
    incomplete.pending_path_origin_counts_by_node.clear();
    incomplete.yielded_path_origin_counts_by_node.clear();
    incomplete.summarized_path_origin_counts_by_node.clear();
    incomplete.published_path_origin_counts_by_node.clear();
    client.update_cached_observability_snapshot(&incomplete);

    let observability_rpc_count = Arc::new(AtomicUsize::new(0));
    install_source_worker_observability_call_count_hook(SourceWorkerObservabilityCallCountHook {
        count: observability_rpc_count.clone(),
    });

    let nonblocking = client.observability_snapshot_nonblocking().await;

    assert_eq!(
        observability_rpc_count.load(Ordering::Relaxed),
        1,
        "active-but-debug-empty recent cache must trigger one live observability RPC before accepting snapshot"
    );
    assert_eq!(
        nonblocking.scheduled_source_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "nonblocking observability must reject active-but-debug-empty recent cache for scheduled source groups"
    );
    assert_eq!(
        nonblocking.scheduled_scan_groups_by_node.get("node-a"),
        Some(&vec!["nfs1".to_string(), "nfs2".to_string()]),
        "nonblocking observability must reject active-but-debug-empty recent cache for scheduled scan groups"
    );
    assert!(
        nonblocking
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "nonblocking observability must reject active-but-debug-empty recent cache for control summary: {:?}",
        nonblocking.last_control_frame_signals_by_node
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_live_cache_preserves_last_control_summary_when_later_active_snapshot_omits_it() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let first_live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime live observability snapshot");
    assert!(
        first_live
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "initial live snapshot must carry control summary before omission preservation is checked: {:?}",
        first_live.last_control_frame_signals_by_node
    );

    let mut later_live = first_live.clone();
    later_live.last_control_frame_signals_by_node.clear();
    later_live.published_batches_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        6,
    )]);
    later_live.published_events_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        6,
    )]);
    later_live.published_control_events_by_node = std::collections::BTreeMap::from([(
        "node-a".to_string(),
        6,
    )]);

    client.update_cached_observability_snapshot(&first_live);
    client.update_cached_observability_snapshot(&later_live);

    let nonblocking = client.observability_snapshot_nonblocking().await;
    assert!(
        nonblocking
            .last_control_frame_signals_by_node
            .get("node-a")
            .is_some_and(|signals| !signals.is_empty()),
        "recent live cache must preserve last control summary when a later active snapshot omits it: {:?}",
        nonblocking.last_control_frame_signals_by_node
    );
    assert_eq!(
        nonblocking.published_batches_by_node.get("node-a"),
        Some(&6),
        "preserving last control summary must not roll back newer publication counters"
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observability_snapshot_nonblocking_fail_closes_incomplete_active_cache_when_worker_unavailable()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let live = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("prime complete live observability snapshot");
    assert!(
        source_observability_snapshot_has_active_state(&live),
        "live snapshot must be active before the fail-closed cache fallback invariant is checked"
    );
    assert!(
        !source_observability_snapshot_debug_maps_absent(&live),
        "live snapshot must carry debug maps before cache corruption: {:?}",
        live
    );

    let mut incomplete = live.clone();
    incomplete.scheduled_source_groups_by_node.clear();
    incomplete.scheduled_scan_groups_by_node.clear();
    incomplete.last_control_frame_signals_by_node.clear();
    incomplete.published_batches_by_node.clear();
    incomplete.published_events_by_node.clear();
    incomplete.published_control_events_by_node.clear();
    incomplete.published_data_events_by_node.clear();
    incomplete.last_published_at_us_by_node.clear();
    incomplete.last_published_origins_by_node.clear();
    incomplete.published_origin_counts_by_node.clear();
    incomplete.enqueued_path_origin_counts_by_node.clear();
    incomplete.pending_path_origin_counts_by_node.clear();
    incomplete.yielded_path_origin_counts_by_node.clear();
    incomplete.summarized_path_origin_counts_by_node.clear();
    incomplete.published_path_origin_counts_by_node.clear();
    client.update_cached_observability_snapshot(&incomplete);

    let _reset = SourceWorkerObservabilityErrorHookReset;
    install_source_worker_observability_error_hook(SourceWorkerObservabilityErrorHook {
        err: CnxError::Internal("synthetic observability failure".to_string()),
    });

    let degraded = client.observability_snapshot_nonblocking().await;
    assert_eq!(
        degraded.lifecycle_state, SOURCE_WORKER_DEGRADED_STATE,
        "worker-unavailable fallback should degrade the snapshot instead of returning a live-looking active cache"
    );
    assert!(
        !source_observability_snapshot_has_active_state(&degraded)
            || !source_observability_snapshot_debug_maps_absent(&degraded),
        "worker-unavailable fallback must not return an active-but-debug-empty source snapshot: {:?}",
        degraded
    );

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_refresh_retries_transient_empty_scheduled_groups_before_publishing_cache()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: vec![worker_source_export(
            "node-b::nfs1",
            "node-b",
            "10.0.0.21",
            nfs1.clone(),
        )],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776102761141088687226881".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::collections::VecDeque::from([(
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            )]),
        },
    );

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply control wave");

    let inflight = client.begin_control_op();
    let stale = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    assert_eq!(
        stale.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()],)]),
        "on_control_frame must retry transient empty scheduled-source refreshes before publishing the next degraded cache snapshot"
    );
    assert_eq!(
        stale.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()],)]),
        "on_control_frame must retry transient empty scheduled-scan refreshes before publishing the next degraded cache snapshot"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_latest_activate_signals()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-c-29776225407437800789245953".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let source_wave = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];
    let control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave)
    .collect::<Vec<_>>();

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-c".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must publish the latest runtime-managed source groups from accepted activate scopes even before live refresh converges"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must publish the latest runtime-managed scan groups from accepted activate scopes even before live refresh converges"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_preserves_latest_control_frame_signals_after_successful_activate_wave()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-c-29776225407437800789245953".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-c-29776225407437800789245953".to_string(),
        vec![
            "activate unit=runtime.exec.source route=source-logical-roots-control:v1.stream generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.source route=source-manual-rescan-control:v1.stream generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.source route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
            "activate unit=runtime.exec.scan route=source-manual-rescan:v1.req generation=2 scopes=[\"nfs1=>node-c::nfs1\", \"nfs2=>node-c::nfs2\"]".to_string(),
        ],
    )]);
    assert_eq!(
        degraded.last_control_frame_signals_by_node, expected,
        "control-inflight nonblocking observability must preserve the latest accepted activate-wave control summary even before live refresh converges"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_local_resource_ids_without_grant_change()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776249969860401661214721".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["node-b::nfs1"]),
                bound_scope_with_resources("nfs2", &["node-b::nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed multi-root control wave without grant change");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must derive latest runtime-managed source groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must derive latest runtime-managed scan groups from local resource ids even when the accepted activate wave carries no fresh grant-change payload"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_limits_bare_logical_scope_ids_to_local_granted_roots_under_mixed_cluster_grants()
 {
    let tmp = tempdir().expect("create temp dir");
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
        std::fs::create_dir_all(path).expect("create mount dir");
    }
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
            worker_fs_source_watch_scan_root("nfs3", nfs3_source),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-a-29799407896396737569357825".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs1",
                    "node-a-29799407896396737569357825",
                    "10.0.0.11",
                    &node_a_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs2",
                    "node-a-29799407896396737569357825",
                    "10.0.0.12",
                    &node_a_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs1",
                    "node-b-29799407896396737569357825",
                    "10.0.0.21",
                    &node_b_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs1",
                    "node-c-29799407896396737569357825",
                    "10.0.0.31",
                    &node_c_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs2",
                    "node-c-29799407896396737569357825",
                    "10.0.0.32",
                    &node_c_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs2",
                    "node-d-29799407896396737569357825",
                    "10.0.0.41",
                    &node_d_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs3",
                    "node-b-29799407896396737569357825",
                    "10.0.0.23",
                    &node_b_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs3",
                    "node-d-29799407896396737569357825",
                    "10.0.0.43",
                    &node_d_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-e-29799407896396737569357825::nfs3",
                    "node-e-29799407896396737569357825",
                    "10.0.0.53",
                    &node_e_nfs3,
                    nfs3_source,
                ),
            ],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
                bound_scope_with_resources("nfs3", &["nfs3"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply mixed-grant fs_source-selected control wave");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-a-29799407896396737569357825".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must not prime a non-local nfs3 schedule from bare logical scope ids when mixed cluster grants already identify node-a as runnable only for nfs1/nfs2"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must not prime a non-local nfs3 scan schedule from bare logical scope ids when mixed cluster grants already identify node-a as runnable only for nfs1/nfs2"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_control_frame_does_not_exhaust_post_ack_schedule_refresh_when_mixed_cluster_grants_leave_no_local_runnable_groups()
{
    let tmp = tempdir().expect("create temp dir");
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
        std::fs::create_dir_all(path).expect("create mount dir");
    }
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";
    let nfs3_source = "127.0.0.1:/exports/nfs3";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
            worker_fs_source_watch_scan_root("nfs3", nfs3_source),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-e-29799407896396737569357825".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs1",
                    "node-a-29799407896396737569357825",
                    "10.0.0.11",
                    &node_a_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-a-29799407896396737569357825::nfs2",
                    "node-a-29799407896396737569357825",
                    "10.0.0.12",
                    &node_a_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs1",
                    "node-b-29799407896396737569357825",
                    "10.0.0.21",
                    &node_b_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs1",
                    "node-c-29799407896396737569357825",
                    "10.0.0.31",
                    &node_c_nfs1,
                    nfs1_source,
                ),
                worker_route_export_with_fs_source(
                    "node-c-29799407896396737569357825::nfs2",
                    "node-c-29799407896396737569357825",
                    "10.0.0.32",
                    &node_c_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs2",
                    "node-d-29799407896396737569357825",
                    "10.0.0.41",
                    &node_d_nfs2,
                    nfs2_source,
                ),
                worker_route_export_with_fs_source(
                    "node-b-29799407896396737569357825::nfs3",
                    "node-b-29799407896396737569357825",
                    "10.0.0.23",
                    &node_b_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-d-29799407896396737569357825::nfs3",
                    "node-d-29799407896396737569357825",
                    "10.0.0.43",
                    &node_d_nfs3,
                    nfs3_source,
                ),
                worker_route_export_with_fs_source(
                    "node-e-29799407896396737569357825::nfs3",
                    "node-e-29799407896396737569357825",
                    "10.0.0.53",
                    &node_e_nfs3,
                    nfs3_source,
                ),
            ],
        })
        .expect("encode runtime host grants changed"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame_with_timeouts_for_tests(
            control,
            Duration::from_millis(240),
            Duration::from_millis(50),
        )
        .await
        .expect(
            "mixed-cluster wave with no local runnable groups should preserve an empty schedule instead of exhausting post-ack schedule refresh",
        );

    let observability = client.observability_snapshot_nonblocking().await;
    assert!(
        observability.scheduled_source_groups_by_node.is_empty(),
        "node-e should preserve an empty source schedule when the mixed-cluster wave carries only nfs1/nfs2 and all refreshed scheduled groups are legitimately empty for the local node"
    );
    assert!(
        observability.scheduled_scan_groups_by_node.is_empty(),
        "node-e should preserve an empty scan schedule when the mixed-cluster wave carries only nfs1/nfs2 and all refreshed scheduled groups are legitimately empty for the local node"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_inflight_nonblocking_observability_primes_runtime_managed_groups_from_scope_ids_for_fs_source_selected_roots()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    let nfs1_source = "127.0.0.1:/exports/nfs1";
    let nfs2_source = "127.0.0.1:/exports/nfs2";

    let cfg = SourceConfig {
        roots: vec![
            worker_fs_source_watch_scan_root("nfs1", nfs1_source),
            worker_fs_source_watch_scan_root("nfs2", nfs2_source),
        ],
        host_object_grants: vec![
            worker_source_export_with_fs_source(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                nfs1.clone(),
                nfs1_source,
            ),
            worker_source_export_with_fs_source(
                "node-b::nfs2",
                "node-b",
                "10.0.0.22",
                nfs2.clone(),
                nfs2_source,
            ),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = SourceWorkerClientHandle::new(
        NodeId("node-b-29776249969860401661214721".to_string()),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    install_source_worker_scheduled_groups_refresh_queue_hook(
        SourceWorkerScheduledGroupsRefreshQueueHook {
            replies: std::iter::repeat((
                Some(std::collections::BTreeSet::new()),
                Some(std::collections::BTreeSet::new()),
            ))
            .take(32)
            .collect(),
        },
    );

    let control = vec![
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source roots activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan-control activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source rescan activate"),
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
            lease: None,
            generation: 2,
            expires_at_ms: 1,
            bound_scopes: vec![
                bound_scope_with_resources("nfs1", &["nfs1"]),
                bound_scope_with_resources("nfs2", &["nfs2"]),
            ],
        }))
        .expect("encode source scan activate"),
    ];

    client
        .on_control_frame(control)
        .await
        .expect("apply runtime-managed fs_source-selected control wave");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = std::collections::BTreeMap::from([(
        "node-b".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);
    assert_eq!(
        degraded.scheduled_source_groups_by_node, expected,
        "control-inflight nonblocking observability must preserve runtime-managed source groups from logical scope ids when fs_source-selected roots already have local bootstrap grants"
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node, expected,
        "control-inflight nonblocking observability must preserve runtime-managed scan groups from logical scope ids when fs_source-selected roots already have local bootstrap grants"
    );

    clear_source_worker_scheduled_groups_refresh_queue_hook();
    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_source_worker_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let node_id = NodeId("node-a-29775285406139598021591041".to_string());
    let client = SourceWorkerClientHandle::new(
        node_id.clone(),
        cfg,
        external_source_worker_binding(worker_socket_dir.path()),
        factory,
    )
    .expect("construct source worker client");

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    client
        .on_control_frame(vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let source_groups = client
            .scheduled_source_group_ids()
            .await
            .expect("scheduled source groups")
            .unwrap_or_default();
        let scan_groups = client
            .scheduled_scan_group_ids()
            .await
            .expect("scheduled scan groups")
            .unwrap_or_default();
        if source_groups == expected_groups && scan_groups == expected_groups {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for scheduled groups: source={source_groups:?} scan={scan_groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let live_worker = client.client().await.expect("connect source worker");
    let snapshot = client
        .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
        .await
        .expect("fetch live snapshot");
    let expected = vec!["nfs1".to_string(), "nfs2".to_string()];
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-a"),
        Some(&expected),
        "scheduled source groups should be keyed by stable host_ref rather than instance-suffixed node id"
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-a"),
        Some(&expected),
        "scheduled scan groups should be keyed by stable host_ref rather than instance-suffixed node id"
    );
    assert!(
        !snapshot
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into scheduled source groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert!(
        !snapshot
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into scheduled scan groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );

    client.close().await.expect("close source worker");
}

#[test]
fn observability_snapshot_normalization_uses_cached_grants_when_live_reply_grants_are_empty() {
    let node_id = NodeId("node-d-29776112502313141518991361".to_string());
    let cached_grants = vec![GrantedMountRoot {
        object_ref: "node-d::nfs2".to_string(),
        host_ref: "node-d".to_string(),
        host_ip: "10.0.0.41".to_string(),
        host_name: None,
        site: None,
        zone: None,
        host_labels: Default::default(),
        mount_point: PathBuf::from("/mnt/nfs2"),
        fs_source: "nfs://server/export2".to_string(),
        fs_type: "nfs".to_string(),
        mount_options: vec![],
        interfaces: vec![],
        active: true,
    }];
    let mut snapshot = SourceObservabilitySnapshot {
        lifecycle_state: "ready".to_string(),
        host_object_grants_version: 2,
        grants: Vec::new(),
        logical_roots: Vec::new(),
        status: SourceStatusSnapshot::default(),
        source_primary_by_group: std::collections::BTreeMap::new(),
        last_force_find_runner_by_group: std::collections::BTreeMap::new(),
        force_find_inflight_groups: Vec::new(),
        scheduled_source_groups_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            vec!["nfs2".to_string()],
        )]),
        scheduled_scan_groups_by_node: std::collections::BTreeMap::from([(
            node_id.0.clone(),
            vec!["nfs2".to_string()],
        )]),
        last_control_frame_signals_by_node: std::collections::BTreeMap::new(),
        published_batches_by_node: std::collections::BTreeMap::new(),
        published_events_by_node: std::collections::BTreeMap::new(),
        published_control_events_by_node: std::collections::BTreeMap::new(),
        published_data_events_by_node: std::collections::BTreeMap::new(),
        last_published_at_us_by_node: std::collections::BTreeMap::new(),
        last_published_origins_by_node: std::collections::BTreeMap::new(),
        published_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_capture_target: None,
        enqueued_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        pending_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        yielded_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        summarized_path_origin_counts_by_node: std::collections::BTreeMap::new(),
        published_path_origin_counts_by_node: std::collections::BTreeMap::new(),
    };

    normalize_observability_snapshot_scheduled_group_keys(
        &mut snapshot,
        &node_id,
        Some(&cached_grants),
    );

    assert_eq!(
        snapshot.scheduled_source_groups_by_node,
        std::collections::BTreeMap::from([("node-d".to_string(), vec!["nfs2".to_string()],)]),
        "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled source groups",
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node,
        std::collections::BTreeMap::from([("node-d".to_string(), vec!["nfs2".to_string()],)]),
        "live observability normalization must fall back to cached grants when the reply omitted grants but still published instance-suffixed scheduled scan groups",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_source_observability_normalizes_instance_suffixed_node_id_to_host_ref_for_scheduled_groups()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs2", &nfs2)],
        host_object_grants: vec![worker_source_export(
            "node-d::nfs2",
            "node-d",
            "10.0.0.41",
            nfs2.clone(),
        )],
        ..SourceConfig::default()
    };
    let node_id = NodeId("node-d-29775443922859927994892289".to_string());
    let source = Arc::new(
        FSMetaSource::with_boundaries(
            cfg,
            node_id.clone(),
            Some(Arc::new(LoopbackWorkerBoundary::default())),
        )
        .expect("init source"),
    );

    source
        .on_control_frame(&[
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
            }))
            .expect("encode source activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: ROUTE_KEY_QUERY.to_string(),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation: 2,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs2", &["node-d::nfs2"])],
            }))
            .expect("encode scan activate"),
        ])
        .await
        .expect("apply source+scan control");

    let snapshot = SourceFacade::Local(source)
        .observability_snapshot()
        .await
        .expect("fetch local observability");
    let expected = vec!["nfs2".to_string()];
    assert_eq!(
        snapshot.scheduled_source_groups_by_node.get("node-d"),
        Some(&expected)
    );
    assert_eq!(
        snapshot.scheduled_scan_groups_by_node.get("node-d"),
        Some(&expected)
    );
    assert!(
        snapshot
            .last_control_frame_signals_by_node
            .get("node-d")
            .is_some_and(|signals| !signals.is_empty()),
        "local source observability must carry the accepted activate-wave control summary instead of source_control=[]: {:?}",
        snapshot.last_control_frame_signals_by_node
    );
    assert!(
        !snapshot
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into local scheduled source groups: {:?}",
        snapshot.scheduled_source_groups_by_node
    );
    assert!(
        !snapshot
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into local scheduled scan groups: {:?}",
        snapshot.scheduled_scan_groups_by_node
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_source_observability_preserves_published_maps_after_local_stream_emits_batches() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");
    std::fs::write(nfs1.join("seed.txt"), b"a").expect("seed nfs1");
    std::fs::write(nfs2.join("seed.txt"), b"b").expect("seed nfs2");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let source = Arc::new(
        FSMetaSource::with_boundaries(cfg, NodeId("node-a".to_string()), None)
            .expect("init source"),
    );
    let mut stream = source.pub_().await.expect("start source pub stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut control_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut data_counts = std::collections::BTreeMap::<String, usize>::new();
    while tokio::time::Instant::now() < deadline {
        let batch = tokio::time::timeout(Duration::from_millis(250), stream.next())
            .await
            .expect("initial local batch wait should not time out")
            .expect("local pub stream should yield initial scan batch");
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

    assert!(
        ["node-a::nfs1", "node-a::nfs2"].iter().all(|origin| {
            control_counts.get(*origin).copied() == Some(2)
                && data_counts.get(*origin).copied().unwrap_or(0) > 0
        }),
        "local source stream should emit baseline control+data for both roots before observability check: control={control_counts:?} data={data_counts:?}"
    );

    let snapshot = SourceFacade::Local(source.clone())
        .observability_snapshot()
        .await
        .expect("fetch local source observability after local stream publish");

    assert!(
        snapshot.published_batches_by_node.get("node-a").copied().unwrap_or(0) > 0,
        "local source observability must not report active scheduling/control truth with published_batches_by_node still empty after the local stream emitted batches: {:?}",
        snapshot
    );
    assert!(
        snapshot.published_events_by_node.get("node-a").copied().unwrap_or(0) > 0,
        "local source observability must preserve published event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .published_control_events_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must preserve published control-event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .published_data_events_by_node
            .get("node-a")
            .copied()
            .unwrap_or(0)
            > 0,
        "local source observability must preserve published data-event counts after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot.last_published_at_us_by_node.contains_key("node-a"),
        "local source observability must preserve a last_published_at_us timestamp after local stream publish: {:?}",
        snapshot
    );
    assert!(
        snapshot
            .last_published_origins_by_node
            .get("node-a")
            .is_some_and(|origins| !origins.is_empty()),
        "local source observability must preserve last_published_origins after local stream publish: {:?}",
        snapshot
    );

    source.close().await.expect("close local source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_active_local_status_yields_schedule_and_control_summary_when_runtime_state_is_empty()
{
    let stable_host_ref = "node-d";
    let status = SourceStatusSnapshot {
        current_stream_generation: Some(7),
        logical_roots: Vec::new(),
        concrete_roots: vec![crate::source::SourceConcreteRootHealthSnapshot {
            root_key: "nfs2@node-d::nfs2@/tmp/nfs2".to_string(),
            logical_root_id: "nfs2".to_string(),
            object_ref: "node-d::nfs2".to_string(),
            status: "running".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary: true,
            active: true,
            watch_lru_capacity: 65_536,
            audit_interval_ms: 300_000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
            emitted_batch_count: 7,
            emitted_event_count: 4051,
            emitted_control_event_count: 2,
            emitted_data_event_count: 4049,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: Some(123456),
            last_emitted_origins: vec!["node-d::nfs2=1".to_string()],
            forwarded_batch_count: 7,
            forwarded_event_count: 4051,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: Some(123456),
            last_forwarded_origins: vec!["node-d::nfs2=1".to_string()],
            current_revision: Some(1),
            current_stream_generation: Some(7),
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }],
        degraded_roots: Vec::new(),
    };

    let source_groups = recovered_scheduled_groups_by_node_from_active_status(
        stable_host_ref,
        &status,
        |entry| entry.watch_enabled,
    );
    let scan_groups = recovered_scheduled_groups_by_node_from_active_status(
        stable_host_ref,
        &status,
        |entry| entry.scan_enabled,
    );
    let control_signals = recovered_control_signals_by_node_from_active_status(
        stable_host_ref,
        &status,
        &source_groups,
        &scan_groups,
    );

    let expected = vec!["nfs2".to_string()];
    assert_eq!(source_groups.get(stable_host_ref), Some(&expected));
    assert_eq!(scan_groups.get(stable_host_ref), Some(&expected));
    assert!(
        control_signals
            .get(stable_host_ref)
            .is_some_and(|signals| !signals.is_empty()),
        "active local status recovery must synthesize a non-empty control summary instead of source_control=[]: {:?}",
        control_signals
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_instance_suffixed_source_worker_recovers_schedule_from_real_source_route_wave() {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: vec![
            worker_source_export("node-a::nfs1", "node-a", "10.0.0.11", nfs1.clone()),
            worker_source_export("node-a::nfs2", "node-a", "10.0.0.12", nfs2.clone()),
        ],
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-a-29775285406139598021591041".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let real_source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-a::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-a::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after real source route wave: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} stderr={}",
                client.logical_roots_snapshot().await.unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(real_source_wave(2))
        .await
        .expect("initial real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(real_source_wave(3))
        .await
        .expect("restarted real source route wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_runtime_host_grants_for_real_source_route_wave()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29775384077525007841886209".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_groups =
        std::collections::BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]);
    async fn assert_scheduled_groups(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            if &source_groups == expected_groups && &scan_groups == expected_groups {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for scheduled groups after restart-preserved host grants: source={source_groups:?} scan={scan_groups:?} logical_roots={:?} grants_version={} grants={:?} stderr={}",
                client.logical_roots_snapshot().await.unwrap_or_default(),
                client
                    .host_object_grants_version_snapshot()
                    .await
                    .unwrap_or_default(),
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_scheduled_groups(
        &client,
        &expected_groups,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_multi_root_observability_after_runtime_managed_upgrade_recovery()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29776120697300046443446273".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_snapshot = std::collections::BTreeMap::from([(
        "node-c".to_string(),
        vec!["nfs1".to_string(), "nfs2".to_string()],
    )]);

    async fn assert_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let worker = client.client().await.expect("connect source worker");
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for multi-root observability after runtime-managed restart recovery: snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_observability(
        &client,
        &expected_snapshot,
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_observability(
        &client,
        &expected_snapshot,
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_observability_preserves_last_control_frame_signals_after_runtime_managed_upgrade_recovery()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    let nfs2 = tmp.path().join("nfs2");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");
    std::fs::create_dir_all(&nfs2).expect("create nfs2 dir");

    let cfg = SourceConfig {
        roots: vec![
            worker_watch_scan_root("nfs1", &nfs1),
            worker_watch_scan_root("nfs2", &nfs2),
        ],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-c-29776120697300046443446273".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
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
                    bound_scope_with_resources("nfs1", &["node-c::nfs1"]),
                    bound_scope_with_resources("nfs2", &["node-c::nfs2"]),
                ],
            }))
            .expect("encode source scan activate"),
        ]
    };

    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![
                worker_route_export("node-c::nfs1", "node-c", "10.0.0.31", &nfs1),
                worker_route_export("node-c::nfs2", "node-c", "10.0.0.32", &nfs2),
            ],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = client
            .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
            .await
            .expect("observability snapshot");
        if matches!(
            snapshot.last_control_frame_signals_by_node.get("node-c"),
            Some(signals) if !signals.is_empty()
        ) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restarted runtime-managed upgrade recovery must preserve last_control_frame_signals_by_node in live observability snapshot: {:?}",
            snapshot.last_control_frame_signals_by_node
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_preserves_single_root_observability_after_runtime_managed_upgrade_recovery()
{
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            NodeId("node-b-29775497172756365788053505".to_string()),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    let expected_groups = std::collections::BTreeSet::from(["nfs1".to_string()]);
    async fn assert_schedule_and_observability(
        client: &Arc<SourceWorkerClientHandle>,
        expected_groups: &std::collections::BTreeSet<String>,
        expected_snapshot: &std::collections::BTreeMap<String, Vec<String>>,
        label: &str,
        worker_socket_dir: &Path,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let source_groups = client
                .scheduled_source_group_ids()
                .await
                .expect("scheduled source groups")
                .unwrap_or_default();
            let scan_groups = client
                .scheduled_scan_group_ids()
                .await
                .expect("scheduled scan groups")
                .unwrap_or_default();
            let worker = client.client().await.expect("connect source worker");
            let snapshot = client
                .observability_snapshot_with_timeout(SOURCE_WORKER_CONTROL_RPC_TIMEOUT)
                .await
                .expect("observability snapshot");
            if &source_groups == expected_groups
                && &scan_groups == expected_groups
                && &snapshot.scheduled_source_groups_by_node == expected_snapshot
                && &snapshot.scheduled_scan_groups_by_node == expected_snapshot
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: timed out waiting for schedule+observability convergence after runtime-managed restart recovery: source={source_groups:?} scan={scan_groups:?} snapshot_source={:?} snapshot_scan={:?} grants={:?} stderr={}",
                snapshot.scheduled_source_groups_by_node,
                snapshot.scheduled_scan_groups_by_node,
                client
                    .host_object_grants_snapshot()
                    .await
                    .unwrap_or_default(),
                worker_stderr_excerpt(worker_socket_dir),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "initial generation",
        worker_socket_dir.path(),
    )
    .await;

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");
    assert_schedule_and_observability(
        &client,
        &expected_groups,
        &std::collections::BTreeMap::from([("node-b".to_string(), vec!["nfs1".to_string()])]),
        "restarted generation",
        worker_socket_dir.path(),
    )
    .await;

    client.close().await.expect("close source worker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_external_source_worker_cache_fallback_preserves_stable_host_ref_after_runtime_managed_upgrade_recovery()
 {
    let tmp = tempdir().expect("create temp dir");
    let nfs1 = tmp.path().join("nfs1");
    std::fs::create_dir_all(&nfs1).expect("create nfs1 dir");

    let cfg = SourceConfig {
        roots: vec![worker_watch_scan_root("nfs1", &nfs1)],
        host_object_grants: Vec::new(),
        ..SourceConfig::default()
    };
    let boundary = Arc::new(LoopbackWorkerBoundary::default());
    let state_boundary = in_memory_state_boundary();
    let worker_socket_dir = worker_socket_tempdir();
    let factory =
        RuntimeWorkerClientFactory::new(boundary.clone(), boundary.clone(), state_boundary);
    let node_id = NodeId("node-b-29775497172756365788053505".to_string());
    let client = Arc::new(
        SourceWorkerClientHandle::new(
            node_id.clone(),
            cfg,
            external_source_worker_binding(worker_socket_dir.path()),
            factory,
        )
        .expect("construct source worker client"),
    );

    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("source worker start timed out")
        .expect("start source worker");

    let source_wave = |generation| {
        vec![
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source roots activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan-control activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source rescan activate"),
            encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
                route_key: format!("{}.req", ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
                unit_id: SOURCE_SCAN_RUNTIME_UNIT_ID.to_string(),
                lease: None,
                generation,
                expires_at_ms: 1,
                bound_scopes: vec![bound_scope_with_resources("nfs1", &["node-b::nfs1"])],
            }))
            .expect("encode source scan activate"),
        ]
    };
    let initial_control = std::iter::once(
        encode_runtime_host_grant_change(&RuntimeHostGrantChange {
            version: 1,
            grants: vec![worker_route_export(
                "node-b::nfs1",
                "node-b",
                "10.0.0.21",
                &nfs1,
            )],
        })
        .expect("encode runtime host grants changed"),
    )
    .chain(source_wave(2))
    .collect::<Vec<_>>();

    client
        .on_control_frame(initial_control)
        .await
        .expect("initial control wave should succeed");

    client
        .shutdown_shared_worker_for_tests()
        .await
        .expect("shutdown source worker for restart");
    tokio::time::timeout(Duration::from_secs(8), client.start())
        .await
        .expect("restarted source worker start timed out")
        .expect("restart source worker");

    client
        .on_control_frame(source_wave(3))
        .await
        .expect("restarted control wave without new host grants should succeed");

    let inflight = client.begin_control_op();
    let degraded = client.observability_snapshot_nonblocking().await;
    drop(inflight);

    let expected = vec!["nfs1".to_string()];
    assert_eq!(
        degraded.scheduled_source_groups_by_node.get("node-b"),
        Some(&expected),
        "cache fallback should preserve stable host_ref-keyed source schedule after runtime-managed upgrade recovery: {:?}",
        degraded.scheduled_source_groups_by_node
    );
    assert_eq!(
        degraded.scheduled_scan_groups_by_node.get("node-b"),
        Some(&expected),
        "cache fallback should preserve stable host_ref-keyed scan schedule after runtime-managed upgrade recovery: {:?}",
        degraded.scheduled_scan_groups_by_node
    );
    assert!(
        !degraded
            .scheduled_source_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into cached degraded source schedule: {:?}",
        degraded.scheduled_source_groups_by_node
    );
    assert!(
        !degraded
            .scheduled_scan_groups_by_node
            .contains_key(&node_id.0),
        "instance-suffixed node id should not leak into cached degraded scan schedule: {:?}",
        degraded.scheduled_scan_groups_by_node
    );

    client.close().await.expect("close source worker");
}

fn worker_stderr_excerpt(socket_dir: &Path) -> String {
    let mut excerpts = std::fs::read_dir(socket_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|row| row.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".stderr.log"))
        })
        .filter_map(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .map(|text| format!("{}:\n{}", path.display(), text))
        })
        .collect::<Vec<_>>();
    excerpts.sort();
    excerpts.join("\n---\n")
}

#[path = "tests/generation_one_local_apply_recovery.rs"]
mod generation_one_local_apply_recovery;
#[path = "tests/recovery.rs"]
mod recovery;
#[path = "tests/trigger_rescan_republish.rs"]
mod trigger_rescan_republish;

trigger_rescan_republish::define_trigger_rescan_republish_tests!();
generation_one_local_apply_recovery::define_generation_one_local_apply_recovery_tests!();
recovery::define_recovery_tests!();
