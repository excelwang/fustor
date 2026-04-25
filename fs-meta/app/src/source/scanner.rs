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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, UNIX_EPOCH};

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

const AUDIT_DEEP_INTERVAL_ROUNDS_ENV: &str = "FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS";
const AUDIT_DEEP_INTERVAL_ROUNDS_DEFAULT: u64 = 24;
const AUDIT_DEEP_INTERVAL_ROUNDS_MIN: u64 = 1;
const AUDIT_DEEP_INTERVAL_ROUNDS_MAX: u64 = 1024;

#[derive(Debug, Clone, Copy, Default)]
struct DirAuditState {
    last_fingerprint: Option<u64>,
    last_deep_round: Option<u64>,
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

fn directory_fingerprint(entries: &[HostFsDirEntry]) -> u64 {
    let mut parts = entries
        .iter()
        .map(|entry| {
            (
                entry
                    .path
                    .file_name()
                    .map(|n| n.as_bytes().to_vec())
                    .unwrap_or_default(),
                entry.is_dir,
            )
        })
        .collect::<Vec<_>>();
    parts.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.len().hash(&mut hasher);
    for (name, is_dir) in parts {
        name.hash(&mut hasher);
        is_dir.hash(&mut hasher);
    }
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

fn host_fs_op_timeout() -> Duration {
    let secs = std::env::var(HOST_FS_OP_TIMEOUT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(HOST_FS_OP_TIMEOUT_DEFAULT_SECS);
    Duration::from_secs(secs)
}

fn host_fs_timeout_error(op: &str, path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{op} timed out after {:?} for {}", host_fs_op_timeout(), path.display()),
    )
}

fn metadata_once_with_timeout(
    host_fs: Arc<dyn HostFs>,
    path: PathBuf,
) -> io::Result<HostFsMetadata> {
    let display_path = path.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(host_fs.metadata(&path));
    });
    match rx.recv_timeout(host_fs_op_timeout()) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(host_fs_timeout_error("metadata", &display_path))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err(io::Error::other(format!(
                "metadata worker disconnected for {}",
                display_path.display()
            )))
        }
    }
}

fn read_dir_once_with_timeout(
    host_fs: Arc<dyn HostFs>,
    path: PathBuf,
) -> io::Result<Vec<HostFsDirEntry>> {
    let display_path = path.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(host_fs.read_dir(&path));
    });
    match rx.recv_timeout(host_fs_op_timeout()) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(host_fs_timeout_error("read_dir", &display_path))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other(format!(
            "read_dir worker disconnected for {}",
            display_path.display()
        ))),
    }
}

fn metadata_with_retry(host_fs: Arc<dyn HostFs>, path: &Path) -> io::Result<HostFsMetadata> {
    let mut last = None::<io::Error>;
    for attempt in 1..=HOST_FS_RETRY_ATTEMPTS {
        match metadata_once_with_timeout(Arc::clone(&host_fs), path.to_path_buf()) {
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

fn read_dir_with_retry(
    host_fs: Arc<dyn HostFs>,
    path: &Path,
) -> io::Result<Vec<HostFsDirEntry>> {
    let mut last = None::<io::Error>;
    for attempt in 1..=HOST_FS_RETRY_ATTEMPTS {
        match read_dir_once_with_timeout(Arc::clone(&host_fs), path.to_path_buf()) {
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

/// Parallel directory scanner with work-stealing.
pub struct ParallelScanner {
    root_path: PathBuf,
    emit_prefix: PathBuf,
    scan_workers: usize,
    batch_size: usize,
    max_scan_events: usize,
    node_id: NodeId,
    host_fs: Arc<dyn HostFs>,
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
            dir_state_cache: Arc::new(Mutex::new(HashMap::new())),
            audit_round: Arc::new(AtomicU64::new(0)),
            deep_scan_interval_rounds: deep_interval_rounds_from_env(),
        }
    }

    /// Run a full audit scan. Returns batched events including EpochStart/End.
    pub fn scan_audit(
        &self,
        epoch_id: u64,
        mtime_cache: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
    ) -> Vec<Vec<Event>> {
        let mut all_batches = Vec::new();

        // EpochStart signal
        let start_signal = ControlEvent::EpochStart {
            epoch_id,
            epoch_type: EpochType::Audit,
        };
        if let Some(ev) = self.build_control_event(&start_signal, drift_estimator, logical_clock) {
            all_batches.push(vec![ev]);
        }

        // Run parallel scan
        let round = self.audit_round.fetch_add(1, Ordering::SeqCst) + 1;
        let records = self.parallel_walk(mtime_cache, drift_estimator, watch_scheduler, round);

        // Re-batch
        let batches = self.rebatch_to_events(records, drift_estimator, logical_clock);
        all_batches.extend(batches);

        // EpochEnd signal
        let end_signal = ControlEvent::EpochEnd {
            epoch_id,
            epoch_type: EpochType::Audit,
        };
        if let Some(ev) = self.build_control_event(&end_signal, drift_estimator, logical_clock) {
            all_batches.push(vec![ev]);
        }

        all_batches
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

        let meta = match metadata_with_retry(Arc::clone(&self.host_fs), &target) {
            Ok(meta) => meta,
            Err(err) if err.kind() == io::ErrorKind::NotFound && target != self.root_path => {
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

    /// Parallel directory walk with work-stealing queue.
    fn parallel_walk(
        &self,
        mtime_cache_ref: &mut HashMap<PathBuf, f64>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        watch_scheduler: Option<Arc<Mutex<crate::source::watcher::WatchManager>>>,
        audit_round: u64,
    ) -> Vec<FileMetaRecord> {
        let (work_tx, work_rx) = cb::unbounded::<PathBuf>();
        let (result_tx, result_rx) = cb::unbounded::<Vec<FileMetaRecord>>();
        let pending_dirs = Arc::new(AtomicUsize::new(1));

        // Seed with root
        if work_tx.send(self.root_path.clone()).is_err() {
            log::warn!("scanner.parallel_walk: work queue closed during seed");
            return Vec::new();
        }

        let visited = Arc::new(Mutex::new(HashSet::<(u64, u64)>::new()));
        let mtime_cache = Arc::new(Mutex::new(std::mem::take(mtime_cache_ref)));
        let dir_state_cache = Arc::clone(&self.dir_state_cache);
        let deep_scan_interval_rounds = self.deep_scan_interval_rounds;
        let root = self.root_path.clone();

        // Spawn workers
        let handles: Vec<_> = (0..self.scan_workers)
            .map(|_| {
                let work_rx = work_rx.clone();
                let work_tx = work_tx.clone();
                let result_tx = result_tx.clone();
                let pending = Arc::clone(&pending_dirs);
                let visited = Arc::clone(&visited);
                let mtime_cache = Arc::clone(&mtime_cache);
                let dir_state_cache = Arc::clone(&dir_state_cache);
                let drift_est = Arc::clone(drift_estimator);
                let root = root.clone();
                let watch_sched = watch_scheduler.clone();
                let host_fs = Arc::clone(&self.host_fs);

                std::thread::spawn(move || {
                    loop {
                        // Use recv_timeout instead of blocking recv to avoid deadlock.
                        // Workers hold work_tx clones, so recv() never returns Err.
                        // Instead, check pending_dirs to detect completion.
                        match work_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                            Ok(dir_path) => {
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
                                let identity = match metadata_with_retry(Arc::clone(&host_fs), &dir_path) {
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

                                    let entries = match read_dir_with_retry(Arc::clone(&host_fs), &dir_path)
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
                                    let relative = watcher::make_relative(&dir_path, &root);
                                        records.push(FileMetaRecord::scan_update(
                                            relative,
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
                                        !should_deep_scan,
                                    ));

                                    let parent_relative =
                                        watcher::make_relative(&dir_path, &root);
                                    let parent_mtime_us = (current_mtime * 1_000_000.0) as u64;

                                    for entry in entries {
                                        if entry.is_dir {
                                            // ALWAYS_RECURSE: still descend into subdirectories.
                                            pending.fetch_add(1, Ordering::SeqCst);
                                            let _ = work_tx.send(entry.path);
                                            continue;
                                        }

                                        if !should_deep_scan {
                                            continue;
                                        }

                                        let entry_path = entry.path;
                                        match metadata_with_retry(Arc::clone(&host_fs), &entry_path) {
                                            Ok(meta) => {
                                                let mtime_us = to_epoch_us(meta.modified);
                                                let ctime_us = to_epoch_us(meta.created);
                                                let relative =
                                                    watcher::make_relative(&entry_path, &root);
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

        // Collect results
        let mut all_records = Vec::new();
        for batch in result_rx {
            all_records.extend(batch);
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

        all_records
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

        match read_dir_with_retry(Arc::clone(&self.host_fs), dir) {
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
        let meta = match metadata_with_retry(Arc::clone(&self.host_fs), path) {
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
            let parent_meta = metadata_with_retry(Arc::clone(&self.host_fs), parent).ok();
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

    /// Convert records to batched events.
    fn rebatch_to_events(
        &self,
        records: Vec<FileMetaRecord>,
        drift_estimator: &Arc<Mutex<DriftEstimator>>,
        logical_clock: &Arc<LogicalClock>,
    ) -> Vec<Vec<Event>> {
        let drift_us = lock_or_recover(drift_estimator, "scanner.rebatch.drift").drift_us();
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();

        for record in records {
            if let Some(ev) = watcher::build_event(&record, &self.node_id, drift_us, logical_clock)
            {
                current_batch.push(ev);
                if current_batch.len() >= self.batch_size {
                    batches.push(std::mem::take(&mut current_batch));
                }
            }
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        batches
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
