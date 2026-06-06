//! Parallel scanner for audit traversal and snapshot queries.
//!
//! Implements work-stealing parallel directory walk with mtime-diff pruning,
//! symlink loop prevention, and re-batching.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, UNIX_EPOCH};

use bytes::Bytes;
use crossbeam_channel as cb;

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_host_adapter_fs::{HostFs, HostFsDirEntry, HostFsMetadata};
use capanix_host_fs_types::UnixStat;

use crate::{ControlEvent, EpochType, FileMetaRecord, LogicalClock};

use crate::source::drift::{self, DriftEstimator};
use crate::source::watcher;

fn lock_or_recover<'a, T>(m: &'a Mutex<T>, context: &str) -> MutexGuard<'a, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            log::warn!("{context}: mutex poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MetadataErrorHostFs {
        kind: io::ErrorKind,
        message: &'static str,
    }

    impl HostFs for MetadataErrorHostFs {
        fn metadata(&self, _path: &Path) -> io::Result<HostFsMetadata> {
            Err(io::Error::new(self.kind, self.message))
        }

        fn symlink_metadata(&self, path: &Path) -> io::Result<HostFsMetadata> {
            self.metadata(path)
        }

        fn read_dir(&self, _path: &Path) -> io::Result<Vec<HostFsDirEntry>> {
            Ok(Vec::new())
        }
    }

    fn scanner_with(host_fs: impl HostFs + 'static) -> ParallelScanner {
        ParallelScanner::new(
            PathBuf::from("/root"),
            PathBuf::new(),
            1,
            16,
            1024,
            NodeId("node-a::nfs1".to_string()),
            Arc::new(host_fs),
        )
    }

    #[derive(Clone)]
    struct WideTreeHostFs {
        width: usize,
        read_dir_calls: Arc<Mutex<Vec<PathBuf>>>,
    }

    impl WideTreeHostFs {
        fn new(width: usize) -> Self {
            Self {
                width,
                read_dir_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn read_dir_calls(&self) -> Vec<PathBuf> {
            lock_or_recover(&self.read_dir_calls, "test.wide_tree.read_dir_calls").clone()
        }
    }

    impl HostFs for WideTreeHostFs {
        fn metadata(&self, path: &Path) -> io::Result<HostFsMetadata> {
            Ok(HostFsMetadata {
                is_dir: true,
                len: 0,
                modified: Some(UNIX_EPOCH + Duration::from_secs(1)),
                created: Some(UNIX_EPOCH + Duration::from_secs(1)),
                dev: Some(1),
                ino: Some(derived_ino_for_path(path)),
            })
        }

        fn symlink_metadata(&self, path: &Path) -> io::Result<HostFsMetadata> {
            self.metadata(path)
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<HostFsDirEntry>> {
            lock_or_recover(&self.read_dir_calls, "test.wide_tree.read_dir_calls")
                .push(path.to_path_buf());
            if path == Path::new("/root") {
                Ok((0..self.width)
                    .map(|idx| HostFsDirEntry {
                        path: PathBuf::from(format!("/root/child-{idx}")),
                        is_dir: true,
                    })
                    .collect())
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn drift_estimator() -> Arc<Mutex<DriftEstimator>> {
        Arc::new(Mutex::new(DriftEstimator::new(32, 10, 1_000_000)))
    }

    #[test]
    fn targeted_scan_preserves_host_fs_unavailable_even_when_reported_as_not_found() {
        let scanner = scanner_with(MetadataErrorHostFs {
            kind: io::ErrorKind::NotFound,
            message: "HOST_FS_UNAVAILABLE: bound root is not a mounted root",
        });

        let err = scanner
            .scan_targeted(
                b"/force-find-stress",
                true,
                None,
                &drift_estimator(),
                &Arc::new(LogicalClock::new()),
            )
            .expect_err("root unavailable must not be treated as a missing child path");

        assert!(
            err.to_string().contains("HOST_FS_UNAVAILABLE"),
            "error should preserve unavailable evidence: {err:?}"
        );
    }

    #[test]
    fn targeted_scan_keeps_plain_not_found_as_missing_child_path() {
        let scanner = scanner_with(MetadataErrorHostFs {
            kind: io::ErrorKind::NotFound,
            message: "No such file or directory",
        });

        let events = scanner
            .scan_targeted(
                b"/missing",
                true,
                None,
                &drift_estimator(),
                &Arc::new(LogicalClock::new()),
            )
            .expect("plain missing child should remain an empty targeted scan");

        assert!(events.is_empty());
    }

    #[test]
    fn targeted_scan_root_not_found_still_fails() {
        let scanner = scanner_with(MetadataErrorHostFs {
            kind: io::ErrorKind::NotFound,
            message: "root missing",
        });

        let err = scanner
            .scan_targeted(
                b"/",
                true,
                None,
                &drift_estimator(),
                &Arc::new(LogicalClock::new()),
            )
            .expect_err("root path itself missing is not a missing child result");

        assert!(err.to_string().contains("root missing"));
    }

    #[test]
    fn audit_scan_ignores_targeted_scan_budget_and_walks_full_tree() {
        let host_fs = WideTreeHostFs::new(128);
        let scanner = ParallelScanner::new(
            PathBuf::from("/root"),
            PathBuf::new(),
            2,
            16,
            1,
            NodeId("node-a::nfs1".to_string()),
            Arc::new(host_fs.clone()),
        );

        let batches = scanner.scan_audit(
            7,
            &mut HashMap::new(),
            &drift_estimator(),
            &Arc::new(LogicalClock::new()),
            None,
        );
        let flat = batches.into_iter().flatten().collect::<Vec<_>>();
        let controls = flat
            .iter()
            .filter_map(|event| rmp_serde::from_slice::<ControlEvent>(event.payload_bytes()).ok())
            .collect::<Vec<_>>();
        let records = flat
            .iter()
            .filter_map(|event| rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()).ok())
            .collect::<Vec<_>>();

        assert_eq!(
            controls,
            vec![
                ControlEvent::EpochStart {
                    epoch_id: 7,
                    epoch_type: EpochType::Audit
                },
                ControlEvent::EpochEnd {
                    epoch_id: 7,
                    epoch_type: EpochType::Audit
                }
            ]
        );
        assert_eq!(
            records.len(),
            129,
            "audit scan must not reuse targeted query max_scan_events as a traversal cutoff"
        );
        assert!(
            records.iter().all(|record| !record.audit_skipped),
            "initial full audit must not pretend unvisited subtrees are covered"
        );
        let paths = records
            .iter()
            .map(|record| record.path.clone())
            .collect::<HashSet<_>>();
        assert!(paths.contains(&b"/".to_vec()));
        assert!(paths.contains(&b"/child-127".to_vec()));
        assert!(
            host_fs.read_dir_calls().len() >= 129,
            "audit scan must visit the full directory tree before closing the epoch"
        );
    }

    #[test]
    fn directory_fingerprint_is_order_independent_for_wide_directories() {
        let mut entries = (0..4096)
            .map(|idx| HostFsDirEntry {
                path: PathBuf::from(format!("/root/child-{idx:04}")),
                is_dir: idx % 17 == 0,
            })
            .collect::<Vec<_>>();
        let forward = directory_fingerprint(&entries);
        entries.reverse();
        let reverse = directory_fingerprint(&entries);

        assert_eq!(
            forward, reverse,
            "directory fingerprint must not depend on read_dir entry order"
        );
    }

    #[derive(Clone)]
    struct ConcurrentMetadataHostFs {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl HostFs for ConcurrentMetadataHostFs {
        fn metadata(&self, _path: &Path) -> io::Result<HostFsMetadata> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let mut observed = self.max_active.load(Ordering::SeqCst);
            while active > observed {
                match self.max_active.compare_exchange(
                    observed,
                    active,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(next) => observed = next,
                }
            }
            std::thread::sleep(Duration::from_millis(30));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(HostFsMetadata {
                is_dir: false,
                len: 1,
                modified: Some(UNIX_EPOCH + Duration::from_secs(1)),
                created: Some(UNIX_EPOCH + Duration::from_secs(1)),
                dev: Some(1),
                ino: Some(1),
            })
        }

        fn symlink_metadata(&self, path: &Path) -> io::Result<HostFsMetadata> {
            self.metadata(path)
        }

        fn read_dir(&self, _path: &Path) -> io::Result<Vec<HostFsDirEntry>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn metadata_many_uses_host_fs_pool_concurrently() {
        let host_fs = ConcurrentMetadataHostFs {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        };
        let max_active = Arc::clone(&host_fs.max_active);
        let pool = HostFsOpPool::new(4, Duration::from_secs(2));
        let paths = (0..8)
            .map(|idx| PathBuf::from(format!("/root/file-{idx}")))
            .collect::<Vec<_>>();

        let results = metadata_many_with_retry(&pool, Arc::new(host_fs), paths);

        assert_eq!(results.len(), 8);
        assert!(results.iter().all(|(_, result)| result.is_ok()));
        assert!(
            max_active.load(Ordering::SeqCst) > 1,
            "bulk catch-up metadata must use the host-fs op pool concurrently"
        );
    }
}

fn derived_ino_for_path(path: &Path) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

fn to_epoch_us(t: Option<std::time::SystemTime>) -> u64 {
    t.and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

const HOST_FS_RETRY_ATTEMPTS: usize = 4;
const HOST_FS_OP_TIMEOUT_ENV: &str = "FS_META_SOURCE_HOST_FS_OP_TIMEOUT_SECS";
const HOST_FS_OP_TIMEOUT_DEFAULT_SECS: u64 = 15;
const HOST_FS_OP_WORKERS_ENV: &str = "FS_META_SOURCE_HOST_FS_OP_WORKERS";
const HOST_FS_OP_WORKERS_MAX: usize = 256;
const HOST_FS_METADATA_BATCH_ENV: &str = "FS_META_SOURCE_METADATA_BATCH";
const HOST_FS_METADATA_BATCH_DEFAULT: usize = 4096;
const HOST_FS_METADATA_BATCH_MAX: usize = 65_536;

const AUDIT_DEEP_INTERVAL_ROUNDS_ENV: &str = "FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS";
const AUDIT_DEEP_INTERVAL_ROUNDS_DEFAULT: u64 = 24;
const AUDIT_DEEP_INTERVAL_ROUNDS_MIN: u64 = 1;
const AUDIT_DEEP_INTERVAL_ROUNDS_MAX: u64 = 1024;

#[derive(Debug, Clone, Copy, Default)]
struct DirAuditState {
    last_fingerprint: Option<u64>,
    last_deep_round: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct AuditScanResult {
    pub(crate) batch_count: usize,
    pub(crate) record_count: usize,
    pub(crate) completed: bool,
}

fn normalize_deep_interval_rounds(raw: u64) -> u64 {
    raw.clamp(
        AUDIT_DEEP_INTERVAL_ROUNDS_MIN,
        AUDIT_DEEP_INTERVAL_ROUNDS_MAX,
    )
}

fn deep_interval_rounds_from_env() -> u64 {
    let raw = std::env::var(AUDIT_DEEP_INTERVAL_ROUNDS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(AUDIT_DEEP_INTERVAL_ROUNDS_DEFAULT);
    normalize_deep_interval_rounds(raw)
}

fn audit_watch_schedule_enabled() -> bool {
    std::env::var("FS_META_SOURCE_AUDIT_SCHEDULE_WATCHES")
        .ok()
        .is_some_and(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
}

fn directory_fingerprint(entries: &[HostFsDirEntry]) -> u64 {
    let mut xor = 0_u64;
    let mut sum = 0_u64;
    let mut rotated_sum = 0_u64;
    let mut dir_count = 0_usize;
    let mut file_count = 0_usize;
    for entry in entries {
        let mut entry_hasher = std::collections::hash_map::DefaultHasher::new();
        entry
            .path
            .file_name()
            .map(|n| n.as_bytes())
            .unwrap_or_default()
            .hash(&mut entry_hasher);
        entry.is_dir.hash(&mut entry_hasher);
        let entry_hash = entry_hasher.finish();
        xor ^= entry_hash;
        sum = sum.wrapping_add(entry_hash);
        rotated_sum = rotated_sum.wrapping_add(entry_hash.rotate_left((entry_hash & 63) as u32));
        if entry.is_dir {
            dir_count += 1;
        } else {
            file_count += 1;
        }
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    entries.len().hash(&mut hasher);
    dir_count.hash(&mut hasher);
    file_count.hash(&mut hasher);
    xor.hash(&mut hasher);
    sum.hash(&mut hasher);
    rotated_sum.hash(&mut hasher);
    hasher.finish()
}

fn should_retry_host_fs_error(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::InvalidData
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::Other
    )
}

fn host_fs_error_indicates_unavailable(err: &io::Error) -> bool {
    err.to_string().contains("HOST_FS_UNAVAILABLE")
        || matches!(
            err.kind(),
            io::ErrorKind::NotConnected
                | io::ErrorKind::HostUnreachable
                | io::ErrorKind::NetworkUnreachable
                | io::ErrorKind::StaleNetworkFileHandle
        )
}

fn host_fs_op_timeout() -> Duration {
    let secs = std::env::var(HOST_FS_OP_TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(HOST_FS_OP_TIMEOUT_DEFAULT_SECS);
    Duration::from_secs(secs)
}

fn normalize_host_fs_op_workers(raw: usize) -> usize {
    raw.clamp(1, HOST_FS_OP_WORKERS_MAX)
}

fn host_fs_op_workers(scan_workers: usize) -> usize {
    std::env::var(HOST_FS_OP_WORKERS_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .map(normalize_host_fs_op_workers)
        .unwrap_or_else(|| normalize_host_fs_op_workers(scan_workers.saturating_mul(4)))
}

fn metadata_batch_size() -> usize {
    std::env::var(HOST_FS_METADATA_BATCH_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(HOST_FS_METADATA_BATCH_DEFAULT)
        .clamp(1, HOST_FS_METADATA_BATCH_MAX)
}

fn host_fs_timeout_error(op: &str, path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "{op} timed out after {:?} for {}",
            host_fs_op_timeout(),
            path.display()
        ),
    )
}

type HostFsJob = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
struct HostFsOpPool {
    tx: cb::Sender<HostFsJob>,
    timeout: Duration,
}

impl HostFsOpPool {
    fn new(worker_count: usize, timeout: Duration) -> Self {
        let (tx, rx) = cb::bounded::<HostFsJob>(worker_count.saturating_mul(2).max(1));
        for _ in 0..worker_count {
            let rx = rx.clone();
            std::thread::spawn(move || {
                while let Ok(job) = rx.recv() {
                    job();
                }
            });
        }
        Self { tx, timeout }
    }

    fn run<T, F>(&self, op: &str, path: &Path, f: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> io::Result<T> + Send + 'static,
    {
        let display_path = path.to_path_buf();
        let (tx, rx) = cb::bounded(1);
        let enqueue_started = Instant::now();
        self.tx
            .send_timeout(
                Box::new(move || {
                    let _ = tx.send(f());
                }),
                self.timeout,
            )
            .map_err(|err| match err {
                cb::SendTimeoutError::Timeout(_) => host_fs_timeout_error(op, &display_path),
                cb::SendTimeoutError::Disconnected(_) => io::Error::other(format!(
                    "{op} worker pool disconnected for {}",
                    display_path.display()
                )),
            })?;
        let elapsed = enqueue_started.elapsed();
        let remaining = self.timeout.checked_sub(elapsed).unwrap_or_default();
        if remaining.is_zero() {
            return Err(host_fs_timeout_error(op, &display_path));
        }
        match rx.recv_timeout(remaining) {
            Ok(result) => result,
            Err(cb::RecvTimeoutError::Timeout) => Err(host_fs_timeout_error(op, &display_path)),
            Err(cb::RecvTimeoutError::Disconnected) => Err(io::Error::other(format!(
                "{op} worker disconnected for {}",
                display_path.display()
            ))),
        }
    }
}

fn metadata_once_with_timeout(
    host_fs_ops: &HostFsOpPool,
    host_fs: Arc<dyn HostFs>,
    path: PathBuf,
) -> io::Result<HostFsMetadata> {
    let display_path = path.clone();
    host_fs_ops.run("metadata", &display_path, move || host_fs.metadata(&path))
}

fn read_dir_once_with_timeout(
    host_fs_ops: &HostFsOpPool,
    host_fs: Arc<dyn HostFs>,
    path: PathBuf,
) -> io::Result<Vec<HostFsDirEntry>> {
    let display_path = path.clone();
    host_fs_ops.run("read_dir", &display_path, move || host_fs.read_dir(&path))
}

fn metadata_with_retry(
    host_fs_ops: &HostFsOpPool,
    host_fs: Arc<dyn HostFs>,
    path: &Path,
) -> io::Result<HostFsMetadata> {
    let mut last = None::<io::Error>;
    for attempt in 1..=HOST_FS_RETRY_ATTEMPTS {
        match metadata_once_with_timeout(host_fs_ops, Arc::clone(&host_fs), path.to_path_buf()) {
            Ok(meta) => return Ok(meta),
            Err(err) if err.kind() == io::ErrorKind::TimedOut => return Err(err),
            Err(err)
                if attempt < HOST_FS_RETRY_ATTEMPTS && should_retry_host_fs_error(err.kind()) =>
            {
                last = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("metadata retry exhausted")))
}

fn metadata_direct_with_retry(host_fs: Arc<dyn HostFs>, path: &Path) -> io::Result<HostFsMetadata> {
    let mut last = None::<io::Error>;
    for attempt in 1..=HOST_FS_RETRY_ATTEMPTS {
        match host_fs.metadata(path) {
            Ok(meta) => return Ok(meta),
            Err(err)
                if attempt < HOST_FS_RETRY_ATTEMPTS && should_retry_host_fs_error(err.kind()) =>
            {
                last = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("metadata retry exhausted")))
}

fn metadata_many_with_retry(
    host_fs_ops: &HostFsOpPool,
    host_fs: Arc<dyn HostFs>,
    paths: Vec<PathBuf>,
) -> Vec<(PathBuf, io::Result<HostFsMetadata>)> {
    if paths.is_empty() {
        return Vec::new();
    }

    let started = Instant::now();
    let deadline = started + host_fs_ops.timeout;
    let (tx, rx) = cb::bounded(paths.len());
    let mut scheduled = HashSet::<PathBuf>::new();
    let mut output = Vec::with_capacity(paths.len());

    for path in paths {
        let remaining = deadline.checked_duration_since(Instant::now());
        let Some(remaining) = remaining else {
            output.push((path.clone(), Err(host_fs_timeout_error("metadata", &path))));
            continue;
        };
        let tx = tx.clone();
        let job_path = path.clone();
        let job_host_fs = Arc::clone(&host_fs);
        match host_fs_ops.tx.send_timeout(
            Box::new(move || {
                let result = metadata_direct_with_retry(job_host_fs, &job_path);
                let _ = tx.send((job_path, result));
            }),
            remaining,
        ) {
            Ok(()) => {
                scheduled.insert(path);
            }
            Err(cb::SendTimeoutError::Timeout(_)) => {
                output.push((path.clone(), Err(host_fs_timeout_error("metadata", &path))));
            }
            Err(cb::SendTimeoutError::Disconnected(_)) => {
                output.push((
                    path.clone(),
                    Err(io::Error::other(format!(
                        "metadata worker pool disconnected for {}",
                        path.display()
                    ))),
                ));
            }
        }
    }
    drop(tx);

    while !scheduled.is_empty() {
        let remaining = deadline.checked_duration_since(Instant::now());
        let Some(remaining) = remaining else {
            break;
        };
        match rx.recv_timeout(remaining) {
            Ok((path, result)) => {
                scheduled.remove(&path);
                output.push((path, result));
            }
            Err(cb::RecvTimeoutError::Timeout) => break,
            Err(cb::RecvTimeoutError::Disconnected) => break,
        }
    }

    for path in scheduled {
        output.push((path.clone(), Err(host_fs_timeout_error("metadata", &path))));
    }
    output
}

fn read_dir_with_retry(
    host_fs_ops: &HostFsOpPool,
    host_fs: Arc<dyn HostFs>,
    path: &Path,
) -> io::Result<Vec<HostFsDirEntry>> {
    let mut last = None::<io::Error>;
    for attempt in 1..=HOST_FS_RETRY_ATTEMPTS {
        match read_dir_once_with_timeout(host_fs_ops, Arc::clone(&host_fs), path.to_path_buf()) {
            Ok(entries) => return Ok(entries),
            Err(err) if err.kind() == io::ErrorKind::TimedOut => return Err(err),
            Err(err)
                if attempt < HOST_FS_RETRY_ATTEMPTS && should_retry_host_fs_error(err.kind()) =>
            {
                last = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("read_dir retry exhausted")))
}

fn flush_scan_records(
    result_tx: &cb::Sender<Vec<FileMetaRecord>>,
    records: &mut Vec<FileMetaRecord>,
    flush_at: usize,
) -> bool {
    if records.len() < flush_at {
        return true;
    }
    result_tx.send(std::mem::take(records)).is_ok()
}

/// Parallel directory scanner with work-stealing.
pub struct ParallelScanner {
    root_path: PathBuf,
    emit_prefix: PathBuf,
    scan_workers: usize,
    batch_size: usize,
    max_scan_events: usize,
    node_id: NodeId,
    host_fs: Arc<dyn HostFs>,
    host_fs_ops: HostFsOpPool,
    dir_state_cache: Arc<Mutex<HashMap<PathBuf, DirAuditState>>>,
    audit_round: Arc<AtomicU64>,
    deep_scan_interval_rounds: u64,
}

impl ParallelScanner {
    pub fn new(
        root_path: PathBuf,
        emit_prefix: PathBuf,
        scan_workers: usize,
        batch_size: usize,
        max_scan_events: usize,
        node_id: NodeId,
        host_fs: Arc<dyn HostFs>,
    ) -> Self {
        Self {
            root_path,
            emit_prefix,
            scan_workers,
            batch_size,
            max_scan_events,
            node_id,
            host_fs,
            host_fs_ops: HostFsOpPool::new(host_fs_op_workers(scan_workers), host_fs_op_timeout()),
            dir_state_cache: Arc::new(Mutex::new(HashMap::new())),
            audit_round: Arc::new(AtomicU64::new(0)),
            deep_scan_interval_rounds: deep_interval_rounds_from_env(),
        }
    }

    /// Run a full audit scan. Returns batched events including EpochStart/End.
    #[cfg(test)]
    pub fn scan_audit(
        &self,
        epoch_id: u64,
        mtime_cache: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
    ) -> Vec<Vec<Event>> {
        let mut batches = Vec::new();
        let _ = self.scan_audit_streaming(
            epoch_id,
            mtime_cache,
            drift_estimator,
            logical_clock,
            watch_scheduler,
            |batch| {
                batches.push(batch);
                true
            },
        );
        batches
    }

    /// Run an audit scan and stream batches as they are produced.
    ///
    /// Audit completeness is a business correctness boundary: this path never
    /// applies `max_scan_events`. That limit remains only on targeted query
    /// responses, where it protects a single request from returning an
    /// unbounded payload.
    pub(crate) fn scan_audit_streaming<F>(
        &self,
        epoch_id: u64,
        mtime_cache: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
        mut emit_batch: F,
    ) -> AuditScanResult
    where
        F: FnMut(Vec<Event>) -> bool,
    {
        let mut batch_count = 0;
        let mut emit = |batch: Vec<Event>| {
            if batch.is_empty() {
                return true;
            }
            batch_count += 1;
            emit_batch(batch)
        };

        // EpochStart signal
        let start_signal = ControlEvent::EpochStart {
            epoch_id,
            epoch_type: EpochType::Audit,
        };
        if let Some(ev) = self.build_control_event(&start_signal, drift_estimator, logical_clock) {
            if !emit(vec![ev]) {
                return AuditScanResult {
                    batch_count,
                    completed: false,
                    ..AuditScanResult::default()
                };
            }
        }

        // Run parallel scan
        let round = self.audit_round.fetch_add(1, Ordering::SeqCst) + 1;
        let (record_count, walk_completed) = self.parallel_walk(
            mtime_cache,
            drift_estimator,
            logical_clock,
            watch_scheduler,
            round,
            &mut emit,
        );
        if !walk_completed {
            return AuditScanResult {
                batch_count,
                record_count,
                completed: false,
                ..AuditScanResult::default()
            };
        }

        // EpochEnd signal
        let end_signal = ControlEvent::EpochEnd {
            epoch_id,
            epoch_type: EpochType::Audit,
        };
        if let Some(ev) = self.build_control_event(&end_signal, drift_estimator, logical_clock) {
            if !emit(vec![ev]) {
                return AuditScanResult {
                    batch_count,
                    record_count,
                    completed: false,
                    ..AuditScanResult::default()
                };
            }
        }

        AuditScanResult {
            batch_count,
            record_count,
            completed: true,
        }
    }

    /// Targeted scan for snapshot/on-demand (single path or recursive).
    pub fn scan_targeted(
        &self,
        path: &[u8],
        recursive: bool,
        max_depth: Option<usize>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
    ) -> Result<Vec<Event>, CnxError> {
        let path_str = String::from_utf8_lossy(path);
        let target = if path_str == "/" {
            self.root_path.clone()
        } else {
            self.root_path.join(path_str.trim_start_matches('/'))
        };

        let meta = match metadata_with_retry(&self.host_fs_ops, Arc::clone(&self.host_fs), &target)
        {
            Ok(meta) => meta,
            Err(err)
                if err.kind() == io::ErrorKind::NotFound
                    && target != self.root_path
                    && !host_fs_error_indicates_unavailable(&err) =>
            {
                return Ok(Vec::new());
            }
            Err(err) => {
                return Err(CnxError::Internal(format!(
                    "stat failed for {}: {err}",
                    target.display()
                )));
            }
        };

        let mut records = Vec::new();

        if meta.is_dir && recursive {
            self.walk_dir_targeted(&target, &mut records, max_depth.unwrap_or(usize::MAX), 0);
        } else {
            if let Some(rec) = self.stat_to_record(&target) {
                records.push(rec);
            }
        }

        if records.len() > self.max_scan_events {
            return Err(CnxError::Internal(format!(
                "Scan result exceeds max_scan_events ({} > {})",
                records.len(),
                self.max_scan_events
            )));
        }

        let drift_us = lock_or_recover(drift_estimator, "scanner.scan_targeted.drift").drift_us();
        let events: Vec<Event> = records
            .iter()
            .filter_map(|rec| watcher::build_event(rec, &self.node_id, drift_us, logical_clock))
            .collect();

        Ok(events)
    }

    pub fn reset_audit_caches_for_manual_rescan(&self) {
        lock_or_recover(
            &self.dir_state_cache,
            "scanner.reset_audit_caches_for_manual_rescan.dir_state_cache",
        )
        .clear();
    }

    fn emit_record_events<F>(
        &self,
        records: Vec<FileMetaRecord>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        current_batch: &mut Vec<Event>,
        emit_batch: &mut F,
    ) -> (usize, bool)
    where
        F: FnMut(Vec<Event>) -> bool,
    {
        let record_count = records.len();
        let drift_us = lock_or_recover(drift_estimator, "scanner.audit_stream.drift").drift_us();
        for record in records {
            if let Some(ev) = watcher::build_event(&record, &self.node_id, drift_us, logical_clock)
            {
                current_batch.push(ev);
                if current_batch.len() >= self.batch_size
                    && !emit_batch(std::mem::take(current_batch))
                {
                    return (record_count, false);
                }
            }
        }
        (record_count, true)
    }

    /// Parallel directory walk with work-stealing queue.
    fn parallel_walk(
        &self,
        mtime_cache_ref: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
        audit_round: u64,
        emit_batch: &mut impl FnMut(Vec<Event>) -> bool,
    ) -> (usize, bool) {
        if self.scan_workers <= 1 {
            return self.parallel_walk_inline(
                mtime_cache_ref,
                drift_estimator,
                logical_clock,
                watch_scheduler,
                audit_round,
                emit_batch,
            );
        }

        let (work_tx, work_rx) = cb::unbounded::<PathBuf>();
        let (result_tx, result_rx) = cb::unbounded::<Vec<FileMetaRecord>>();
        let pending_dirs = Arc::new(AtomicUsize::new(1));
        let stop_requested = Arc::new(AtomicBool::new(false));

        // Seed with root
        if work_tx.send(self.root_path.clone()).is_err() {
            log::warn!("scanner.parallel_walk: work queue closed during seed");
            return (0, false);
        }

        let visited = Arc::new(Mutex::new(HashSet::<(u64, u64)>::new()));
        let mtime_cache = Arc::new(Mutex::new(std::mem::take(mtime_cache_ref)));
        let dir_state_cache = Arc::clone(&self.dir_state_cache);
        let deep_scan_interval_rounds = self.deep_scan_interval_rounds;
        let root = self.root_path.clone();
        let schedule_audit_watches = audit_watch_schedule_enabled();
        let metadata_batch = metadata_batch_size();
        let flush_record_count = self.batch_size.max(1);
        // Spawn workers
        let handles: Vec<_> = (0..self.scan_workers)
            .map(|_| {
                let work_rx = work_rx.clone();
                let work_tx = work_tx.clone();
                let result_tx = result_tx.clone();
                let pending = Arc::clone(&pending_dirs);
                let stop_requested = Arc::clone(&stop_requested);
                let visited = Arc::clone(&visited);
                let mtime_cache = Arc::clone(&mtime_cache);
                let dir_state_cache = Arc::clone(&dir_state_cache);
                let drift_est = Arc::clone(drift_estimator);
                let root = root.clone();
                let watch_sched = if schedule_audit_watches {
                    watch_scheduler.clone()
                } else {
                    None
                };
                let host_fs = Arc::clone(&self.host_fs);
                let host_fs_ops = self.host_fs_ops.clone();
                let metadata_batch = metadata_batch;
                let flush_record_count = flush_record_count;
                std::thread::spawn(move || {
                    loop {
                        // Use recv_timeout instead of blocking recv to avoid deadlock.
                        // Workers hold work_tx clones, so recv() never returns Err.
                        // Instead, check pending_dirs to detect completion.
                        match work_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                            Ok(dir_path) => {
                                if stop_requested.load(Ordering::SeqCst) {
                                    pending.fetch_sub(1, Ordering::SeqCst);
                                    continue;
                                }
                                let mut records = Vec::new();

                                // Register watch first for realtime-covered directories.
                                if let Some(wm) = &watch_sched {
                                    let schedule_result = {
                                        let mut mgr = lock_or_recover(wm, "scanner.parallel_walk.watch");
                                        mgr.schedule(&dir_path)
                                    };
                                    if let Err(e) = schedule_result {
                                        log::warn!(
                                            "Failed to schedule watch for {:?}: {}. Will rely on audit compensation",
                                            dir_path,
                                            e
                                        );
                                    }
                                }

                                // Check symlink loop
                                let identity = match metadata_with_retry(&host_fs_ops, Arc::clone(&host_fs), &dir_path) {
                                    Ok(meta) => {
                                        let id = (
                                            meta.dev.unwrap_or(0),
                                            meta.ino.unwrap_or_else(|| derived_ino_for_path(&dir_path)),
                                        );
                                        if !lock_or_recover(&visited, "scanner.parallel_walk.visited")
                                            .insert(id)
                                        {
                                            log::warn!("Symlink loop detected at {:?}", dir_path);
                                            pending.fetch_sub(1, Ordering::SeqCst);
                                            continue;
                                        }
                                        Some((meta, id))
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Skipping directory {:?}: metadata failed: {}",
                                            dir_path,
                                            e
                                        );
                                        pending.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    }
                                };

                                // Collect dir mtime for drift estimation
                                if let Some((ref meta, _)) = identity {
                                    let Some(mtime) = meta.modified else {
                                        log::warn!(
                                            "Skipping directory {:?}: modified() missing",
                                            dir_path
                                        );
                                        pending.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    };

                                    let mtime_secs = mtime
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs_f64();
                                    let local_now = std::time::SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs_f64();
                                    lock_or_recover(
                                        &drift_est,
                                        "scanner.parallel_walk.push_scan_drift",
                                    )
                                    .push_scan_sample(mtime_secs, local_now);

                                    let current_mtime = mtime_secs;
                                    let mtime_unchanged = {
                                        let mut cache = lock_or_recover(
                                            &mtime_cache,
                                            "scanner.parallel_walk.mtime_cache",
                                        );
                                        let cached = cache.get(&dir_path).copied();
                                        let unchanged =
                                            cached.is_some_and(|c| (c - current_mtime).abs() < 1e-6);
                                        cache.insert(dir_path.clone(), current_mtime);
                                        unchanged
                                    };

                                    let relative = watcher::make_relative(&dir_path, &root);
                                    let file_name = dir_path
                                        .file_name()
                                        .map(|n| n.as_bytes().to_vec())
                                        .unwrap_or_default();
                                    let dir_record = |audit_skipped| {
                                        FileMetaRecord::scan_update(
                                            relative.clone(),
                                            file_name.clone(),
                                            UnixStat {
                                                is_dir: true,
                                                size: 0,
                                                mtime_us: (current_mtime * 1_000_000.0) as u64,
                                                ctime_us: 0,
                                                dev: None,
                                                ino: None,
                                            },
                                            Vec::new(),
                                            0,
                                            audit_skipped,
                                        )
                                    };

                                    let entries = match read_dir_with_retry(&host_fs_ops, Arc::clone(&host_fs), &dir_path)
                                    {
                                        Ok(entries) => entries,
                                        Err(e) => {
                                            log::warn!(
                                                "Skipping directory {:?}: read_dir failed: {}",
                                                dir_path,
                                                e
                                            );
                                            pending.fetch_sub(1, Ordering::SeqCst);
                                            continue;
                                        }
                                    };

                                    let fingerprint = directory_fingerprint(&entries);
                                    let should_deep_scan = {
                                        let mut states = lock_or_recover(
                                            &dir_state_cache,
                                            "scanner.parallel_walk.dir_state_cache",
                                        );
                                        let state = states.entry(dir_path.clone()).or_default();
                                        let fingerprint_changed = match state.last_fingerprint {
                                            Some(previous) => previous != fingerprint,
                                            None => true,
                                        };
                                        let deep_due = match state.last_deep_round {
                                            Some(last) => {
                                                audit_round.saturating_sub(last)
                                                    >= deep_scan_interval_rounds
                                            }
                                            None => true,
                                        };
                                        let should = !mtime_unchanged || fingerprint_changed || deep_due;
                                        state.last_fingerprint = Some(fingerprint);
                                        if should {
                                            state.last_deep_round = Some(audit_round);
                                        }
                                        should
                                    };

                                    // Emit directory record (includes root on initial scan).
                                    records.push(dir_record(!should_deep_scan));

                                    let parent_relative =
                                        watcher::make_relative(&dir_path, &root);
                                    let parent_mtime_us = (current_mtime * 1_000_000.0) as u64;
                                    let mut file_paths = Vec::new();

                                    for entry in entries {
                                        if stop_requested.load(Ordering::SeqCst) {
                                            break;
                                        }
                                        if entry.is_dir {
                                            // ALWAYS_RECURSE: still descend into subdirectories.
                                            pending.fetch_add(1, Ordering::SeqCst);
                                            let _ = work_tx.send(entry.path);
                                            continue;
                                        }

                                        if !should_deep_scan {
                                            continue;
                                        }

                                        file_paths.push(entry.path);
                                    }

                                    for chunk in file_paths.chunks(metadata_batch) {
                                        if stop_requested.load(Ordering::SeqCst) {
                                            break;
                                        }
                                        let metas = metadata_many_with_retry(
                                            &host_fs_ops,
                                            Arc::clone(&host_fs),
                                            chunk.to_vec(),
                                        );
                                        for (entry_path, meta_result) in metas {
                                            if stop_requested.load(Ordering::SeqCst) {
                                                break;
                                            }
                                            let relative =
                                                watcher::make_relative(&entry_path, &root);
                                            match meta_result {
                                                Ok(meta) => {
                                                    let mtime_us = to_epoch_us(meta.modified);
                                                    let ctime_us = to_epoch_us(meta.created);
                                                    records.push(FileMetaRecord::scan_update(
                                                        relative,
                                                        entry_path
                                                            .file_name()
                                                            .map(|n| n.as_bytes().to_vec())
                                                            .unwrap_or_default(),
                                                        UnixStat {
                                                            is_dir: false,
                                                            size: meta.len,
                                                            mtime_us,
                                                            ctime_us,
                                                            dev: meta.dev,
                                                            ino: meta.ino,
                                                        },
                                                        parent_relative.clone(),
                                                        parent_mtime_us,
                                                        false,
                                                    ));
                                                }
                                                Err(e) => log::warn!(
                                                    "Skipping file {:?}: metadata failed: {}",
                                                    entry_path,
                                                    e
                                                ),
                                            }
                                            if !flush_scan_records(
                                                &result_tx,
                                                &mut records,
                                                flush_record_count,
                                            ) {
                                                stop_requested.store(true, Ordering::SeqCst);
                                                break;
                                            }
                                        }
                                    }
                                }

                                if !records.is_empty() {
                                    let _ = result_tx.send(records);
                                }
                                pending.fetch_sub(1, Ordering::SeqCst);
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                                // No work available — check if all work is done
                                if pending.load(Ordering::SeqCst) == 0 {
                                    break;
                                }
                                // Otherwise, other workers may still produce work items
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();

        // Drop sender clones so workers can exit when done
        drop(work_tx);
        drop(result_tx);

        // Stream results while workers continue walking. This avoids retaining a
        // full audit in memory and lets callers consume progress immediately.
        let mut record_count = 0;
        let mut current_batch = Vec::new();
        let mut completed = true;
        for records in result_rx {
            if completed {
                let (records_seen, still_open) = self.emit_record_events(
                    records,
                    drift_estimator,
                    logical_clock,
                    &mut current_batch,
                    emit_batch,
                );
                record_count += records_seen;
                if !still_open {
                    completed = false;
                    stop_requested.store(true, Ordering::SeqCst);
                }
            } else {
                record_count += records.len();
            }
        }

        if completed && !current_batch.is_empty() && !emit_batch(std::mem::take(&mut current_batch))
        {
            completed = false;
        }

        for handle in handles {
            let _ = handle.join();
        }

        // Restore mtime cache
        if let Ok(mutex) = Arc::try_unwrap(mtime_cache) {
            *mtime_cache_ref = match mutex.into_inner() {
                Ok(v) => v,
                Err(poisoned) => {
                    log::warn!("scanner.parallel_walk: mtime cache poisoned at join");
                    poisoned.into_inner()
                }
            };
        }

        (record_count, completed)
    }

    fn parallel_walk_inline(
        &self,
        mtime_cache_ref: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
        audit_round: u64,
        emit_batch: &mut impl FnMut(Vec<Event>) -> bool,
    ) -> (usize, bool) {
        let mut pending = vec![self.root_path.clone()];
        let mut record_count = 0;
        let mut current_batch = Vec::new();
        let mut visited = HashSet::<(u64, u64)>::new();
        let mut mtime_cache = std::mem::take(mtime_cache_ref);
        let root = self.root_path.clone();
        let schedule_audit_watches = audit_watch_schedule_enabled();
        while let Some(dir_path) = pending.pop() {
            let mut records = Vec::new();

            if schedule_audit_watches {
                if let Some(wm) = &watch_scheduler {
                    let schedule_result = {
                        let mut mgr = lock_or_recover(wm, "scanner.parallel_walk_inline.watch");
                        mgr.schedule(&dir_path)
                    };
                    if let Err(e) = schedule_result {
                        log::warn!(
                            "Failed to schedule watch for {:?}: {}. Will rely on audit compensation",
                            dir_path,
                            e
                        );
                    }
                }
            }

            let identity = match metadata_with_retry(
                &self.host_fs_ops,
                Arc::clone(&self.host_fs),
                &dir_path,
            ) {
                Ok(meta) => {
                    let id = (
                        meta.dev.unwrap_or(0),
                        meta.ino.unwrap_or_else(|| derived_ino_for_path(&dir_path)),
                    );
                    if !visited.insert(id) {
                        log::warn!("Symlink loop detected at {:?}", dir_path);
                        continue;
                    }
                    Some(meta)
                }
                Err(e) => {
                    log::warn!("Skipping directory {:?}: metadata failed: {}", dir_path, e);
                    continue;
                }
            };

            if let Some(meta) = identity {
                let Some(mtime) = meta.modified else {
                    log::warn!("Skipping directory {:?}: modified() missing", dir_path);
                    continue;
                };

                let mtime_secs = mtime
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                let local_now = std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                lock_or_recover(
                    drift_estimator,
                    "scanner.parallel_walk_inline.push_scan_drift",
                )
                .push_scan_sample(mtime_secs, local_now);

                let current_mtime = mtime_secs;
                let mtime_unchanged = {
                    let cached = mtime_cache.get(&dir_path).copied();
                    let unchanged = cached.is_some_and(|c| (c - current_mtime).abs() < 1e-6);
                    mtime_cache.insert(dir_path.clone(), current_mtime);
                    unchanged
                };

                let relative = watcher::make_relative(&dir_path, &root);
                let dir_record = |audit_skipped| {
                    FileMetaRecord::scan_update(
                        relative.clone(),
                        dir_path
                            .file_name()
                            .map(|n| n.as_bytes().to_vec())
                            .unwrap_or_default(),
                        UnixStat {
                            is_dir: true,
                            size: 0,
                            mtime_us: (current_mtime * 1_000_000.0) as u64,
                            ctime_us: 0,
                            dev: None,
                            ino: None,
                        },
                        Vec::new(),
                        0,
                        audit_skipped,
                    )
                };

                let entries = match read_dir_with_retry(
                    &self.host_fs_ops,
                    Arc::clone(&self.host_fs),
                    &dir_path,
                ) {
                    Ok(entries) => entries,
                    Err(e) => {
                        log::warn!("Skipping directory {:?}: read_dir failed: {}", dir_path, e);
                        continue;
                    }
                };

                let fingerprint = directory_fingerprint(&entries);
                let should_deep_scan = {
                    let mut states = lock_or_recover(
                        &self.dir_state_cache,
                        "scanner.parallel_walk_inline.dir_state_cache",
                    );
                    let state = states.entry(dir_path.clone()).or_default();
                    let fingerprint_changed = match state.last_fingerprint {
                        Some(previous) => previous != fingerprint,
                        None => true,
                    };
                    let deep_due = match state.last_deep_round {
                        Some(last) => {
                            audit_round.saturating_sub(last) >= self.deep_scan_interval_rounds
                        }
                        None => true,
                    };
                    let should = !mtime_unchanged || fingerprint_changed || deep_due;
                    state.last_fingerprint = Some(fingerprint);
                    if should {
                        state.last_deep_round = Some(audit_round);
                    }
                    should
                };

                records.push(dir_record(!should_deep_scan));

                let parent_relative = watcher::make_relative(&dir_path, &root);
                let parent_mtime_us = (current_mtime * 1_000_000.0) as u64;

                for entry in entries {
                    if entry.is_dir {
                        pending.push(entry.path);
                        continue;
                    }

                    if !should_deep_scan {
                        continue;
                    }

                    let entry_path = entry.path;
                    let relative = watcher::make_relative(&entry_path, &root);
                    match metadata_with_retry(
                        &self.host_fs_ops,
                        Arc::clone(&self.host_fs),
                        &entry_path,
                    ) {
                        Ok(meta) => {
                            let mtime_us = to_epoch_us(meta.modified);
                            let ctime_us = to_epoch_us(meta.created);
                            records.push(FileMetaRecord::scan_update(
                                relative,
                                entry_path
                                    .file_name()
                                    .map(|n| n.as_bytes().to_vec())
                                    .unwrap_or_default(),
                                UnixStat {
                                    is_dir: false,
                                    size: meta.len,
                                    mtime_us,
                                    ctime_us,
                                    dev: meta.dev,
                                    ino: meta.ino,
                                },
                                parent_relative.clone(),
                                parent_mtime_us,
                                false,
                            ));
                        }
                        Err(e) => {
                            log::warn!("Skipping file {:?}: metadata failed: {}", entry_path, e)
                        }
                    }
                }
            }

            let (records_seen, still_open) = self.emit_record_events(
                records,
                drift_estimator,
                logical_clock,
                &mut current_batch,
                emit_batch,
            );
            record_count += records_seen;
            if !still_open {
                *mtime_cache_ref = mtime_cache;
                return (record_count, false);
            }
        }

        let completed = current_batch.is_empty() || emit_batch(std::mem::take(&mut current_batch));
        *mtime_cache_ref = mtime_cache;
        (record_count, completed)
    }

    /// Walk dir for targeted scan (non-parallel, depth-limited).
    fn walk_dir_targeted(
        &self,
        dir: &Path,
        records: &mut Vec<FileMetaRecord>,
        max_depth: usize,
        current_depth: usize,
    ) {
        if current_depth > max_depth {
            return;
        }

        // Emit the directory itself
        if let Some(rec) = self.stat_to_record(dir) {
            records.push(rec);
        }

        match read_dir_with_retry(&self.host_fs_ops, Arc::clone(&self.host_fs), dir) {
            Ok(entries) => {
                for entry in entries {
                    if entry.is_dir {
                        self.walk_dir_targeted(&entry.path, records, max_depth, current_depth + 1);
                    } else if let Some(rec) = self.stat_to_record(&entry.path) {
                        records.push(rec);
                    }
                }
            }
            Err(e) => {
                log::warn!("Skipping directory {:?}: read_dir failed: {}", dir, e);
            }
        }
    }

    /// Stat a single path and produce a FileMetaRecord.
    fn stat_to_record(&self, path: &Path) -> Option<FileMetaRecord> {
        let meta = match metadata_with_retry(&self.host_fs_ops, Arc::clone(&self.host_fs), path) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("Skipping path {:?}: metadata failed: {}", path, e);
                return None;
            }
        };
        let mtime_us = match meta.modified {
            Some(t) => match t.duration_since(UNIX_EPOCH) {
                Ok(d) => d.as_micros() as u64,
                Err(e) => {
                    log::warn!(
                        "Skipping path {:?}: mtime before epoch or invalid duration: {}",
                        path,
                        e
                    );
                    return None;
                }
            },
            None => {
                log::warn!("Skipping path {:?}: modified() missing", path);
                return None;
            }
        };
        let ctime_us = to_epoch_us(meta.created);

        let relative = watcher::make_relative_with_prefix(path, &self.root_path, &self.emit_prefix);

        let (parent_path, parent_mtime_us) = if let Some(parent) = path.parent() {
            let parent_meta =
                metadata_with_retry(&self.host_fs_ops, Arc::clone(&self.host_fs), parent).ok();
            let parent_mtime = parent_meta
                .and_then(|m| m.modified)
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            (
                watcher::make_relative_with_prefix(parent, &self.root_path, &self.emit_prefix),
                parent_mtime,
            )
        } else {
            (Vec::new(), 0)
        };

        Some(FileMetaRecord::scan_update(
            relative,
            path.file_name()
                .map(|n| n.as_bytes().to_vec())
                .unwrap_or_default(),
            UnixStat {
                is_dir: meta.is_dir,
                size: meta.len,
                mtime_us,
                ctime_us,
                dev: meta.dev,
                ino: meta.ino,
            },
            parent_path,
            parent_mtime_us,
            false,
        ))
    }

    /// Build a control event (EpochStart/End).
    fn build_control_event(
        &self,
        event: &ControlEvent,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
    ) -> Option<Event> {
        let payload = rmp_serde::to_vec_named(event).ok()?;
        let drift_us = lock_or_recover(drift_estimator, "scanner.control_event.drift").drift_us();
        Some(Event::new(
            EventMetadata {
                origin_id: self.node_id.clone(),
                timestamp_us: drift::shadow_now_us(drift_us),
                logical_ts: Some(logical_clock.tick()),
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        ))
    }
}
