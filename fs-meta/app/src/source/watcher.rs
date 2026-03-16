//! Watch Manager: host watch backend integration with LRU watch scheduling.
//!
//! Manages a capacity-limited set of active host watches with LRU eviction.
//! Handles event classification, throttle dedup, and upward activity propagation.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io;

#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event};
use capanix_host_adapter_fs_meta::{
    HostFsMeta, HostFsWatch, HostFsWatchDescriptor, HostFsWatchEvent, HostFsWatchMask,
};

use capanix_host_fs_types::{ControlEvent, FileMetaRecord, UnixStat};

use crate::query::path::path_to_bytes;
use crate::source::config::SourceConfig;
use crate::source::drift::{self, DriftEstimator};

fn lock_or_recover<'a, T>(m: &'a Mutex<T>, context: &str) -> MutexGuard<'a, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            log::warn!("{context}: mutex poisoned; recovering inner state");
            poisoned.into_inner()
        }
    }
}

/// A single active watch entry.
struct WatchEntry {
    wd: HostFsWatchDescriptor,
    last_activity: Instant,
    /// Cached directory mtime for audit diff pruning.
    #[allow(dead_code)]
    parent_mtime: Option<f64>,
}

/// LRU-based host watch manager.
pub struct WatchManager {
    /// Active watches: path → entry.
    watches: HashMap<PathBuf, WatchEntry>,
    /// Reverse lookup: WatchDescriptor → path.
    wd_to_path: HashMap<HostFsWatchDescriptor, PathBuf>,
    /// LRU priority queue: min-heap by (timestamp, path).
    lru_heap: BinaryHeap<Reverse<(Instant, PathBuf)>>,
    /// Maximum simultaneous watches (dynamically reduced on ENOSPC).
    capacity: usize,
    /// Minimum age before eviction.
    min_monitoring_window: Duration,
    /// Host watch backend.
    watch: Box<dyn HostFsWatch>,
    /// Root path for upward propagation boundary.
    root_path: PathBuf,
    /// Prefix to keep emitted paths in logical namespace.
    emit_prefix: PathBuf,
}

/// Standard watch mask for directories.
const WATCH_MASK: HostFsWatchMask = HostFsWatchMask::from_bits_truncate(
    HostFsWatchMask::CREATE.bits()
        | HostFsWatchMask::MODIFY.bits()
        | HostFsWatchMask::CLOSE_WRITE.bits()
        | HostFsWatchMask::DELETE.bits()
        | HostFsWatchMask::MOVED_FROM.bits()
        | HostFsWatchMask::MOVED_TO.bits()
        | HostFsWatchMask::ATTRIB.bits()
        | HostFsWatchMask::DONT_FOLLOW.bits()
        | HostFsWatchMask::DELETE_SELF.bits(),
);

impl WatchManager {
    /// Create a new WatchManager with the given configuration.
    pub fn new(
        config: &SourceConfig,
        root_path: PathBuf,
        emit_prefix: PathBuf,
        watch: Box<dyn HostFsWatch>,
    ) -> io::Result<Self> {
        Ok(Self {
            watches: HashMap::new(),
            wd_to_path: HashMap::new(),
            lru_heap: BinaryHeap::new(),
            capacity: config.lru_capacity,
            min_monitoring_window: config.min_monitoring_window,
            watch,
            root_path,
            emit_prefix,
        })
    }

    /// Schedule a watch on a directory. If at capacity, evicts the oldest.
    pub fn schedule(&mut self, path: &Path) -> Result<(), CnxError> {
        if self.watches.contains_key(path) {
            self.touch(path);
            return Ok(());
        }

        if self.capacity == 0 {
            log::warn!(
                "watch capacity is 0; skipping watch registration for {} and relying on Audit",
                path.display()
            );
            return Ok(());
        }

        // Evict if at capacity
        while self.watches.len() >= self.capacity {
            self.evict_oldest();
        }

        match self.watch.add_watch(path, WATCH_MASK) {
            Ok(wd) => {
                let now = Instant::now();
                self.wd_to_path.insert(wd.clone(), path.to_owned());
                self.watches.insert(
                    path.to_owned(),
                    WatchEntry {
                        wd,
                        last_activity: now,
                        parent_mtime: None,
                    },
                );
                self.lru_heap.push(Reverse((now, path.to_owned())));
                Ok(())
            }
            Err(e) if e.raw_os_error() == Some(28) => {
                // ENOSPC: system-wide limit hit
                self.capacity = self.watches.len().max(1);
                log::warn!(
                    "watch backend limit hit, reducing capacity to {}. \
                     Directory {} will rely on Audit.",
                    self.capacity,
                    path.display()
                );
                Ok(())
            }
            Err(e) => Err(CnxError::Internal(format!("watch_add: {e}"))),
        }
    }

    /// Evict the oldest watch. Never fails.
    fn evict_oldest(&mut self) {
        while let Some(Reverse((ts, path))) = self.lru_heap.pop() {
            if let Some(entry) = self.watches.get(&path) {
                if entry.last_activity == ts {
                    let age = Instant::now() - entry.last_activity;
                    if age < self.min_monitoring_window {
                        log::warn!(
                            "Evicting young watch {} (age {:?} < window {:?})",
                            path.display(),
                            age,
                            self.min_monitoring_window
                        );
                    }
                    let _ = self.watch.remove_watch(entry.wd);
                    self.wd_to_path.remove(&entry.wd);
                    self.watches.remove(&path);
                    return;
                }
                // Stale entry: skip
            }
        }
    }

    /// Update activity timestamp for a path.
    pub fn touch(&mut self, path: &Path) {
        if let Some(entry) = self.watches.get_mut(path) {
            let now = Instant::now();
            entry.last_activity = now;
            self.lru_heap.push(Reverse((now, path.to_owned())));
        }
    }

    /// Touch all ancestor directories up to root.
    pub fn touch_upward(&mut self, path: &Path) {
        let mut current = path.parent();
        while let Some(dir) = current {
            if dir < self.root_path.as_path() {
                break;
            }
            self.touch(dir);
            current = dir.parent();
        }
    }

    /// Handle IN_IGNORED event (watch removed by kernel).
    pub fn handle_in_ignored(&mut self, wd: HostFsWatchDescriptor) -> Option<PathBuf> {
        if let Some(path) = self.wd_to_path.remove(&wd) {
            let is_root = path == self.root_path;
            self.watches.remove(&path);
            if is_root {
                return Some(path);
            }
        }
        None
    }

    /// Look up path by watch descriptor.
    pub fn path_for_wd(&self, wd: HostFsWatchDescriptor) -> Option<&PathBuf> {
        self.wd_to_path.get(&wd)
    }

    /// Number of active watches.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn watch_count(&self) -> usize {
        self.watches.len()
    }

    /// Read host watch events.
    pub fn read_events(&mut self, buffer: &mut [u8]) -> io::Result<Vec<HostFsWatchEvent>> {
        self.watch.read_events(buffer)
    }
}

// ── Throttle Dedup ──

/// IN_MODIFY throttle: suppresses rapid-fire modify events.
pub struct ThrottleState {
    last_emitted: HashMap<PathBuf, Instant>,
    interval: Duration,
}

impl ThrottleState {
    pub fn new(interval: Duration) -> Self {
        Self {
            last_emitted: HashMap::new(),
            interval,
        }
    }

    /// Return true if a modify event for this path should be emitted.
    pub fn should_emit_modify(&mut self, path: &Path) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last_emitted.get(path) {
            if now.duration_since(*last) < self.interval {
                return false;
            }
        }
        self.last_emitted.insert(path.to_owned(), now);
        true
    }

    /// Clear throttle entry on CLOSE_WRITE.
    pub fn clear(&mut self, path: &Path) {
        self.last_emitted.remove(path);
    }
}

// ── Event Loop Bridge ──

/// Classify a host watch event and produce Capanix events.
fn classify_event(
    mask: HostFsWatchMask,
    event_path: PathBuf,
    root_path: &Path,
    emit_prefix: &Path,
    host_fs: &dyn HostFsMeta,
    node_id: &NodeId,
    drift_us: i64,
    logical_clock: &capanix_host_fs_types::LogicalClock,
    throttle: &mut ThrottleState,
) -> Vec<Event> {
    let mut results = Vec::new();

    let relative_path = make_relative_with_prefix(&event_path, root_path, emit_prefix);

    if mask.contains(HostFsWatchMask::DELETE) || mask.contains(HostFsWatchMask::MOVED_FROM) {
        // Delete event: no stat needed
        let record = FileMetaRecord::realtime_delete(
            relative_path.clone(),
            event_path
                .file_name()
                .map(|n| n.as_bytes().to_vec())
                .unwrap_or_default(),
            mask.contains(HostFsWatchMask::ISDIR),
        );
        if let Some(ev) = build_event(&record, node_id, drift_us, logical_clock) {
            results.push(ev);
        }
    } else if mask.contains(HostFsWatchMask::CLOSE_WRITE) {
        // Definitive update — flush-before-notify guarantees fresh mtime
        throttle.clear(&event_path);
        if let Some(ev) = stat_and_emit(
            &event_path,
            root_path,
            emit_prefix,
            host_fs,
            true, // is_atomic: CLOSE_WRITE = data stabilized
            node_id,
            drift_us,
            logical_clock,
        ) {
            results.push(ev);
        }
    } else if mask.contains(HostFsWatchMask::ATTRIB) {
        // Attribute change (chmod/chown/setxattr). mtime is NOT updated by
        // these operations, so stat() returns the last-write mtime (may be
        // arbitrarily old). Marked non-atomic to prevent clearing suspect
        // and to prevent LWW override of Scan's more accurate mtime.
        if let Some(ev) = stat_and_emit(
            &event_path,
            root_path,
            emit_prefix,
            host_fs,
            false, // is_atomic: ATTRIB ≠ data stabilization
            node_id,
            drift_us,
            logical_clock,
        ) {
            results.push(ev);
        }
    } else if mask.contains(HostFsWatchMask::MODIFY) {
        // Throttled modify
        if throttle.should_emit_modify(&event_path) {
            if let Some(ev) = stat_and_emit(
                &event_path,
                root_path,
                emit_prefix,
                host_fs,
                false,
                node_id,
                drift_us,
                logical_clock,
            ) {
                results.push(ev);
            }
        }
    } else if mask.contains(HostFsWatchMask::CREATE) {
        if mask.contains(HostFsWatchMask::ISDIR) {
            // Directory create: emit the directory event, then recursively scan subtree
            if let Some(ev) = stat_and_emit(
                &event_path,
                root_path,
                emit_prefix,
                host_fs,
                true,
                node_id,
                drift_us,
                logical_clock,
            ) {
                results.push(ev);
            }
            // Recursively scan and emit events for all descendants
            recursive_scan_dir(
                &event_path,
                root_path,
                emit_prefix,
                host_fs,
                node_id,
                drift_us,
                logical_clock,
                &mut results,
            );
        }
        // File create: no event until CLOSE_WRITE or MODIFY
    } else if mask.contains(HostFsWatchMask::MOVED_TO) {
        // Destination of move: emit update
        if let Some(ev) = stat_and_emit(
            &event_path,
            root_path,
            emit_prefix,
            host_fs,
            true,
            node_id,
            drift_us,
            logical_clock,
        ) {
            results.push(ev);
        }
        // If directory move-in, recursively scan entire subtree
        if mask.contains(HostFsWatchMask::ISDIR) {
            recursive_scan_dir(
                &event_path,
                root_path,
                emit_prefix,
                host_fs,
                node_id,
                drift_us,
                logical_clock,
                &mut results,
            );
        }
    }

    results
}

/// Recursively scan a directory and emit Realtime events for all descendants.
/// Used for DIR_CREATE and DIR_MOVED_TO to ensure immediate subtree coverage.
/// Uses symlink_metadata to avoid following symlinks into loops.
fn recursive_scan_dir(
    dir: &Path,
    root: &Path,
    emit_prefix: &Path,
    host_fs: &dyn HostFsMeta,
    node_id: &NodeId,
    drift_us: i64,
    logical_clock: &capanix_host_fs_types::LogicalClock,
    results: &mut Vec<Event>,
) {
    let entries = match host_fs.read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            log::warn!("recursive_scan_dir: cannot read {:?}: {}", dir, e);
            return;
        }
    };

    for entry in entries {
        let path = entry.path;
        if let Some(ev) = stat_and_emit(
            &path,
            root,
            emit_prefix,
            host_fs,
            true,
            node_id,
            drift_us,
            logical_clock,
        ) {
            results.push(ev);
        }
        // Only recurse into real directories, not symlinks (prevents loops)
        if let Ok(meta) = host_fs.symlink_metadata(&path)
            && meta.is_dir
        {
            recursive_scan_dir(
                &path,
                root,
                emit_prefix,
                host_fs,
                node_id,
                drift_us,
                logical_clock,
                results,
            );
        }
    }
}

/// Stat a file and build a Capanix Event.
fn stat_and_emit(
    path: &Path,
    root: &Path,
    emit_prefix: &Path,
    host_fs: &dyn HostFsMeta,
    is_atomic: bool,
    node_id: &NodeId,
    drift_us: i64,
    logical_clock: &capanix_host_fs_types::LogicalClock,
) -> Option<Event> {
    let meta = match host_fs.metadata(path) {
        Ok(meta) => meta,
        Err(e) => {
            log::warn!("Skipping path {:?}: metadata failed: {}", path, e);
            return None;
        }
    };
    let mtime = match meta.modified {
        Some(mtime) => mtime,
        None => {
            log::warn!("Skipping path {:?}: modified() missing", path);
            return None;
        }
    };
    let mtime_us = match mtime.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(e) => {
            log::warn!(
                "Skipping path {:?}: mtime before epoch or invalid duration: {}",
                path,
                e
            );
            return None;
        }
    };
    let ctime_us = match meta.created {
        Some(created) => match created.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_micros() as u64,
            Err(e) => {
                log::warn!(
                    "Path {:?}: created() before epoch or invalid duration: {}; using 0",
                    path,
                    e
                );
                0
            }
        },
        None => 0,
    };

    let relative = make_relative_with_prefix(path, root, emit_prefix);
    let record = FileMetaRecord::realtime_update(
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
        is_atomic,
    );

    build_event(&record, node_id, drift_us, logical_clock)
}

/// Build a Capanix Event from a FileMetaRecord.
pub fn build_event(
    record: &FileMetaRecord,
    node_id: &NodeId,
    drift_us: i64,
    logical_clock: &capanix_host_fs_types::LogicalClock,
) -> Option<Event> {
    let payload = rmp_serde::to_vec_named(record).ok()?;
    Some(Event::new(
        EventMetadata {
            origin_id: node_id.clone(),
            timestamp_us: drift::shadow_now_us(drift_us),
            logical_ts: Some(logical_clock.tick()),
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    ))
}

fn build_control_event(
    control: &ControlEvent,
    node_id: &NodeId,
    drift_us: i64,
    logical_clock: &capanix_host_fs_types::LogicalClock,
) -> Option<Event> {
    let payload = rmp_serde::to_vec_named(control).ok()?;
    Some(Event::new(
        EventMetadata {
            origin_id: node_id.clone(),
            timestamp_us: drift::shadow_now_us(drift_us),
            logical_ts: Some(logical_clock.tick()),
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        Bytes::from(payload),
    ))
}

/// Convert an absolute path to a root-relative byte path.
pub fn make_relative(path: &Path, root: &Path) -> Vec<u8> {
    if path == root {
        return b"/".to_vec();
    }
    match path.strip_prefix(root) {
        Ok(rel) => {
            let mut buf = Vec::with_capacity(rel.as_os_str().len() + 1);
            buf.push(b'/');
            buf.extend_from_slice(&path_to_bytes(rel));
            buf
        }
        Err(_) => path_to_bytes(path),
    }
}

#[cfg(target_family = "unix")]
fn bytes_to_pathbuf(bytes: &[u8]) -> PathBuf {
    crate::query::path::path_buf_from_bytes(bytes)
}

/// Convert an absolute path to a root-relative path and then prepend a stable
/// logical namespace prefix. `emit_prefix=/` keeps the canonical relative shape.
pub fn make_relative_with_prefix(path: &Path, root: &Path, emit_prefix: &Path) -> Vec<u8> {
    if emit_prefix == Path::new("/") {
        return make_relative(path, root);
    }
    let rel = make_relative(path, root);
    #[cfg(target_family = "unix")]
    {
        let rel_path = bytes_to_pathbuf(&rel);
        let joined = if rel_path == Path::new("/") {
            emit_prefix.to_path_buf()
        } else {
            emit_prefix.join(rel_path.strip_prefix("/").unwrap_or(rel_path.as_path()))
        };
        return path_to_bytes(&joined);
    }
    #[allow(unreachable_code)]
    rel
}

fn process_raw_events(
    watcher: &Arc<Mutex<WatchManager>>,
    drift_estimator: &Arc<Mutex<DriftEstimator>>,
    raw_events: Vec<HostFsWatchEvent>,
    shutdown: &CancellationToken,
    root_path: &Path,
    emit_prefix: &Path,
    node_id: &NodeId,
    logical_clock: &capanix_host_fs_types::LogicalClock,
    host_fs: &dyn HostFsMeta,
    throttle: &mut ThrottleState,
    rescan_tx: Option<&tokio::sync::broadcast::Sender<super::RescanReason>>,
) -> Option<Vec<Event>> {
    let drift_us = lock_or_recover(drift_estimator, "watcher.process_raw_events.drift").drift_us();
    let mut batch = Vec::new();

    for HostFsWatchEvent { wd, mask, name } in raw_events {
        if mask.contains(HostFsWatchMask::Q_OVERFLOW) {
            log::warn!("watch queue overflow — marking group unreliable until audit");
            if let Some(control_event) = build_control_event(
                &ControlEvent::WatchOverflow,
                node_id,
                drift_us,
                logical_clock,
            ) {
                batch.push(control_event);
            }
            if let Some(tx) = rescan_tx {
                let _ = tx.send(super::RescanReason::Overflow);
            }
            continue;
        }

        if mask.contains(HostFsWatchMask::IGNORED) {
            let mut mgr = lock_or_recover(watcher, "watcher.process_raw_events.ignored");
            if let Some(_root) = mgr.handle_in_ignored(wd) {
                log::error!("Root directory deleted or unmounted");
                shutdown.cancel();
                return None;
            }
            continue;
        }

        let dir_path = {
            let mgr = lock_or_recover(watcher, "watcher.process_raw_events.path_lookup");
            mgr.path_for_wd(wd).cloned()
        };

        if let Some(dir_path) = dir_path {
            let event_path = match &name {
                Some(n) => dir_path.join(n),
                None => dir_path.clone(),
            };

            // Only CLOSE_WRITE and DIR_CREATE have a fresh server mtime sample.
            if mask.contains(HostFsWatchMask::CLOSE_WRITE)
                || (mask.contains(HostFsWatchMask::CREATE) && mask.contains(HostFsWatchMask::ISDIR))
            {
                match host_fs.metadata(&event_path) {
                    Ok(meta) => {
                        if let Some(mtime) = meta.modified {
                            let mtime_secs = mtime
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs_f64();
                            let local_now_secs = std::time::SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs_f64();
                            lock_or_recover(
                                drift_estimator,
                                "watcher.process_raw_events.push_drift",
                            )
                            .push_realtime_sample(mtime_secs, local_now_secs);
                        } else {
                            log::warn!(
                                "drift sample skipped for {:?}: modified() missing",
                                event_path
                            );
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "drift sample skipped for {:?}: metadata failed: {}",
                            event_path,
                            e
                        );
                    }
                }
            }

            {
                let mut mgr = lock_or_recover(watcher, "watcher.process_raw_events.touch_upward");
                mgr.touch_upward(&event_path);
                if mask.contains(HostFsWatchMask::ISDIR)
                    && (mask.contains(HostFsWatchMask::CREATE)
                        || mask.contains(HostFsWatchMask::MOVED_TO))
                {
                    let _ = mgr.schedule(&event_path);
                }
            }

            let events = classify_event(
                mask,
                event_path,
                root_path,
                emit_prefix,
                host_fs,
                node_id,
                drift_us,
                logical_clock,
                throttle,
            );
            batch.extend(events);
        }
    }

    Some(batch)
}

/// Start the blocking host watch event loop in a spawned task.
///
/// Returns a `JoinHandle` for the blocking task.
/// `rescan_tx`: sends `RescanReason::Overflow` for overflow diagnostics signaling.
pub(crate) fn start_watch_loop(
    watcher: Arc<Mutex<WatchManager>>,
    drift_estimator: Arc<Mutex<DriftEstimator>>,
    tx: mpsc::Sender<Vec<Event>>,
    shutdown: CancellationToken,
    node_id: NodeId,
    logical_clock: Arc<capanix_host_fs_types::LogicalClock>,
    host_fs: Arc<dyn HostFsMeta>,
    throttle_interval: Duration,
    rescan_tx: Option<tokio::sync::broadcast::Sender<super::RescanReason>>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; 8192];
        let mut throttle = ThrottleState::new(throttle_interval);

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let root_path;
            let emit_prefix;
            let events_result;
            {
                let mut mgr = lock_or_recover(&watcher, "watcher.loop.manager");
                root_path = mgr.root_path.clone();
                emit_prefix = mgr.emit_prefix.clone();
                events_result = mgr.read_events(&mut buf);
            }

            match events_result {
                Ok(raw_events) => {
                    let Some(batch) = process_raw_events(
                        &watcher,
                        &drift_estimator,
                        raw_events,
                        &shutdown,
                        &root_path,
                        &emit_prefix,
                        &node_id,
                        logical_clock.as_ref(),
                        host_fs.as_ref(),
                        &mut throttle,
                        rescan_tx.as_ref(),
                    ) else {
                        return;
                    };

                    if !batch.is_empty() {
                        let _ = tx.blocking_send(batch);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No events ready, sleep briefly
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => {
                    log::error!("watch backend read error: {e}");
                    break;
                }
            }
        }
    })
}

use std::time::UNIX_EPOCH;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet, VecDeque};

    use capanix_host_adapter_fs_meta::{FsMetaDirEntry, FsMetaMetadata};
    use capanix_host_fs_types::{EventKind, SyncTrack};

    #[derive(Default)]
    struct MockHostFs {
        metadata_map: HashMap<PathBuf, FsMetaMetadata>,
        metadata_fail_paths: HashSet<PathBuf>,
        read_dir_map: HashMap<PathBuf, Vec<FsMetaDirEntry>>,
    }

    impl MockHostFs {
        fn with_metadata(mut self, path: &str, metadata: FsMetaMetadata) -> Self {
            self.metadata_map.insert(PathBuf::from(path), metadata);
            self
        }

        fn with_metadata_failure(mut self, path: &str) -> Self {
            self.metadata_fail_paths.insert(PathBuf::from(path));
            self
        }

        fn with_read_dir(mut self, path: &str, entries: Vec<FsMetaDirEntry>) -> Self {
            self.read_dir_map.insert(PathBuf::from(path), entries);
            self
        }
    }

    impl HostFsMeta for MockHostFs {
        fn metadata(&self, path: &Path) -> io::Result<FsMetaMetadata> {
            if self.metadata_fail_paths.contains(path) {
                return Err(io::Error::other("mock metadata failure"));
            }
            self.metadata_map
                .get(path)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "mock metadata not found"))
        }

        fn symlink_metadata(&self, path: &Path) -> io::Result<FsMetaMetadata> {
            self.metadata(path)
        }

        fn read_dir(&self, _path: &Path) -> io::Result<Vec<FsMetaDirEntry>> {
            Ok(self.read_dir_map.get(_path).cloned().unwrap_or_default())
        }
    }

    fn mock_file_metadata() -> FsMetaMetadata {
        FsMetaMetadata {
            is_dir: false,
            len: 123,
            modified: Some(std::time::SystemTime::now()),
            created: Some(std::time::SystemTime::now()),
            dev: Some(1),
            ino: Some(2),
        }
    }

    fn mock_dir_metadata(ino: u64) -> FsMetaMetadata {
        FsMetaMetadata {
            is_dir: true,
            len: 0,
            modified: Some(std::time::SystemTime::now()),
            created: Some(std::time::SystemTime::now()),
            dev: Some(1),
            ino: Some(ino),
        }
    }

    fn watch_event(
        wd: HostFsWatchDescriptor,
        mask: HostFsWatchMask,
        name: Option<&str>,
    ) -> HostFsWatchEvent {
        HostFsWatchEvent {
            wd,
            mask,
            name: name.map(std::ffi::OsString::from),
        }
    }

    fn decode_records(events: &[Event]) -> Vec<FileMetaRecord> {
        events
            .iter()
            .map(|event| {
                rmp_serde::from_slice(event.payload_bytes()).expect("decode FileMetaRecord")
            })
            .collect()
    }

    struct MockWatchBackend {
        add_results: VecDeque<io::Result<HostFsWatchDescriptor>>,
        next_id: u64,
    }

    impl MockWatchBackend {
        fn with_add_results(add_results: Vec<io::Result<HostFsWatchDescriptor>>) -> Self {
            Self {
                add_results: add_results.into(),
                next_id: 1,
            }
        }
    }

    impl HostFsWatch for MockWatchBackend {
        fn add_watch(
            &mut self,
            _path: &Path,
            _mask: HostFsWatchMask,
        ) -> io::Result<HostFsWatchDescriptor> {
            if let Some(result) = self.add_results.pop_front() {
                return result;
            }
            let wd = HostFsWatchDescriptor(self.next_id);
            self.next_id += 1;
            Ok(wd)
        }

        fn remove_watch(&mut self, _wd: HostFsWatchDescriptor) -> io::Result<()> {
            Ok(())
        }

        fn read_events(&mut self, _buffer: &mut [u8]) -> io::Result<Vec<HostFsWatchEvent>> {
            Ok(Vec::new())
        }
    }

    fn watch_test_config(capacity: usize) -> SourceConfig {
        SourceConfig {
            lru_capacity: capacity,
            min_monitoring_window: Duration::from_millis(0),
            ..SourceConfig::default()
        }
    }

    #[test]
    fn stat_and_emit_metadata_failure_returns_none() {
        let host_fs = MockHostFs::default().with_metadata_failure("/root/fail.txt");
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let event = stat_and_emit(
            Path::new("/root/fail.txt"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            false,
            &node_id,
            0,
            &logical_clock,
        );
        assert!(
            event.is_none(),
            "metadata failure should be isolated (log+skip)"
        );
    }

    #[test]
    fn classify_event_attrib_emits_non_atomic_update() {
        let host_fs = MockHostFs::default().with_metadata("/root/file.txt", mock_file_metadata());
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let events = classify_event(
            HostFsWatchMask::ATTRIB,
            PathBuf::from("/root/file.txt"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );

        assert_eq!(events.len(), 1);
        let record: FileMetaRecord =
            rmp_serde::from_slice(events[0].payload_bytes()).expect("decode FileMetaRecord");
        assert_eq!(record.event_kind, EventKind::Update);
        assert!(!record.is_atomic_write);
        assert_eq!(record.source, SyncTrack::Realtime);
    }

    #[test]
    fn classify_event_attrib_metadata_failure_isolated() {
        let host_fs = MockHostFs::default().with_metadata_failure("/root/file.txt");
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let events = classify_event(
            HostFsWatchMask::ATTRIB,
            PathBuf::from("/root/file.txt"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );

        assert!(
            events.is_empty(),
            "single-entry failure should not abort pipeline"
        );
    }

    #[test]
    fn classify_event_modify_then_close_write_emits_final_atomic_update_after_throttle() {
        let initial = FsMetaMetadata {
            is_dir: false,
            len: 10,
            modified: Some(std::time::SystemTime::now()),
            created: Some(std::time::SystemTime::now()),
            dev: Some(1),
            ino: Some(2),
        };
        let final_meta = FsMetaMetadata {
            is_dir: false,
            len: 20,
            modified: Some(std::time::SystemTime::now()),
            created: Some(std::time::SystemTime::now()),
            dev: Some(1),
            ino: Some(2),
        };
        let first_host_fs = MockHostFs::default().with_metadata("/root/file.txt", initial);
        let final_host_fs = MockHostFs::default().with_metadata("/root/file.txt", final_meta);
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_secs(60));

        let first = classify_event(
            HostFsWatchMask::MODIFY,
            PathBuf::from("/root/file.txt"),
            Path::new("/root"),
            Path::new("/"),
            &first_host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );
        assert_eq!(first.len(), 1);
        let first_record: FileMetaRecord =
            rmp_serde::from_slice(first[0].payload_bytes()).expect("decode first modify");
        assert!(!first_record.is_atomic_write);
        assert_eq!(first_record.unix_stat.size, 10);

        let throttled = classify_event(
            HostFsWatchMask::MODIFY,
            PathBuf::from("/root/file.txt"),
            Path::new("/root"),
            Path::new("/"),
            &final_host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );
        assert!(
            throttled.is_empty(),
            "rapid repeated MODIFY should be throttled"
        );

        let close = classify_event(
            HostFsWatchMask::CLOSE_WRITE,
            PathBuf::from("/root/file.txt"),
            Path::new("/root"),
            Path::new("/"),
            &final_host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );
        assert_eq!(close.len(), 1);
        let close_record: FileMetaRecord =
            rmp_serde::from_slice(close[0].payload_bytes()).expect("decode close-write");
        assert!(close_record.is_atomic_write);
        assert_eq!(close_record.unix_stat.size, 20);
        assert_eq!(close_record.path, b"/file.txt".to_vec());
    }

    #[test]
    fn classify_event_moved_from_emits_delete_record() {
        let host_fs = MockHostFs::default();
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let events = classify_event(
            HostFsWatchMask::MOVED_FROM,
            PathBuf::from("/root/a.txt"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );

        assert_eq!(events.len(), 1);
        let record: FileMetaRecord =
            rmp_serde::from_slice(events[0].payload_bytes()).expect("decode delete record");
        assert_eq!(record.event_kind, EventKind::Delete);
        assert_eq!(record.path, b"/a.txt".to_vec());
    }

    #[test]
    fn classify_event_moved_to_emits_atomic_update_record() {
        let host_fs = MockHostFs::default().with_metadata("/root/a.txt", mock_file_metadata());
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let events = classify_event(
            HostFsWatchMask::MOVED_TO,
            PathBuf::from("/root/a.txt"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );

        assert_eq!(events.len(), 1);
        let record: FileMetaRecord =
            rmp_serde::from_slice(events[0].payload_bytes()).expect("decode update record");
        assert_eq!(record.event_kind, EventKind::Update);
        assert!(record.is_atomic_write);
        assert_eq!(record.source, SyncTrack::Realtime);
        assert_eq!(record.path, b"/a.txt".to_vec());
    }

    #[test]
    fn classify_event_moved_to_dir_recursively_emits_descendants() {
        let dir_meta = FsMetaMetadata {
            is_dir: true,
            len: 0,
            modified: Some(std::time::SystemTime::now()),
            created: Some(std::time::SystemTime::now()),
            dev: Some(1),
            ino: Some(10),
        };
        let file_meta = mock_file_metadata();

        let host_fs = MockHostFs::default()
            .with_metadata("/root/dir", dir_meta.clone())
            .with_metadata("/root/dir/a.txt", file_meta.clone())
            .with_metadata("/root/dir/sub", dir_meta)
            .with_metadata("/root/dir/sub/b.txt", file_meta)
            .with_read_dir(
                "/root/dir",
                vec![
                    FsMetaDirEntry {
                        path: PathBuf::from("/root/dir/a.txt"),
                        is_dir: false,
                    },
                    FsMetaDirEntry {
                        path: PathBuf::from("/root/dir/sub"),
                        is_dir: true,
                    },
                ],
            )
            .with_read_dir(
                "/root/dir/sub",
                vec![FsMetaDirEntry {
                    path: PathBuf::from("/root/dir/sub/b.txt"),
                    is_dir: false,
                }],
            );

        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let events = classify_event(
            HostFsWatchMask::from_bits_truncate(
                HostFsWatchMask::MOVED_TO.bits() | HostFsWatchMask::ISDIR.bits(),
            ),
            PathBuf::from("/root/dir"),
            Path::new("/root"),
            Path::new("/"),
            &host_fs,
            &node_id,
            0,
            &logical_clock,
            &mut throttle,
        );

        assert_eq!(events.len(), 4);
        let mut paths = Vec::new();
        for event in events {
            let record: FileMetaRecord =
                rmp_serde::from_slice(event.payload_bytes()).expect("decode record");
            paths.push(record.path);
        }
        assert!(paths.contains(&b"/dir".to_vec()));
        assert!(paths.contains(&b"/dir/a.txt".to_vec()));
        assert!(paths.contains(&b"/dir/sub".to_vec()));
        assert!(paths.contains(&b"/dir/sub/b.txt".to_vec()));
    }

    #[test]
    fn schedule_enospc_degrades_without_failing_and_keeps_capacity_non_zero() {
        let config = watch_test_config(2);
        let watch = Box::new(MockWatchBackend::with_add_results(vec![Err(
            io::Error::from_raw_os_error(28),
        )]));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");

        assert!(mgr.schedule(Path::new("/root/a")).is_ok());
        assert_eq!(mgr.watch_count(), 0);
        assert_eq!(mgr.capacity, 1);

        // Second scheduling attempt must not loop forever when first ENOSPC happened with 0 watches.
        assert!(mgr.schedule(Path::new("/root/b")).is_ok());
    }

    #[test]
    fn schedule_at_capacity_evicts_least_recently_used_watch() {
        let config = watch_test_config(2);
        let watch = Box::new(MockWatchBackend::with_add_results(vec![]));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");

        assert!(mgr.schedule(Path::new("/root/a")).is_ok());
        assert!(mgr.schedule(Path::new("/root/b")).is_ok());
        assert_eq!(mgr.watch_count(), 2);

        std::thread::sleep(Duration::from_millis(1));
        mgr.touch(Path::new("/root/a"));

        assert!(mgr.schedule(Path::new("/root/c")).is_ok());
        assert_eq!(mgr.watch_count(), 2);

        assert_eq!(
            mgr.path_for_wd(HostFsWatchDescriptor(1))
                .map(|p| p.as_path()),
            Some(Path::new("/root/a"))
        );
        assert_eq!(mgr.path_for_wd(HostFsWatchDescriptor(2)), None);
        assert_eq!(
            mgr.path_for_wd(HostFsWatchDescriptor(3))
                .map(|p| p.as_path()),
            Some(Path::new("/root/c"))
        );
    }

    #[test]
    fn schedule_non_enospc_error_returns_internal_error() {
        let config = watch_test_config(2);
        let watch = Box::new(MockWatchBackend::with_add_results(vec![Err(
            io::Error::from_raw_os_error(22),
        )]));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");

        match mgr.schedule(Path::new("/root/a")) {
            Err(CnxError::Internal(msg)) => {
                assert!(
                    msg.contains("watch_add"),
                    "error should include watch_add context"
                );
            }
            other => panic!("expected Internal watch_add error, got {other:?}"),
        }
        assert_eq!(mgr.watch_count(), 0);
    }

    #[test]
    fn handle_in_ignored_purges_watch_mapping_and_flags_root_removal() {
        let config = watch_test_config(4);
        let watch = Box::new(MockWatchBackend::with_add_results(Vec::new()));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");

        mgr.schedule(Path::new("/root"))
            .expect("schedule root watch");
        mgr.schedule(Path::new("/root/child"))
            .expect("schedule child watch");

        let root_wd = mgr.watches.get(Path::new("/root")).expect("root watch").wd;
        let child_wd = mgr
            .watches
            .get(Path::new("/root/child"))
            .expect("child watch")
            .wd;

        let root_removed = mgr.handle_in_ignored(root_wd);
        assert_eq!(root_removed, Some(PathBuf::from("/root")));
        assert!(mgr.path_for_wd(root_wd).is_none());
        assert!(!mgr.watches.contains_key(Path::new("/root")));

        let child_removed = mgr.handle_in_ignored(child_wd);
        assert_eq!(child_removed, None);
        assert!(mgr.path_for_wd(child_wd).is_none());
        assert!(!mgr.watches.contains_key(Path::new("/root/child")));
    }

    #[test]
    fn process_raw_events_directory_create_and_ignored_cleanup_update_watch_lifecycle() {
        let config = watch_test_config(8);
        let watch = Box::new(MockWatchBackend::with_add_results(Vec::new()));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");
        mgr.schedule(Path::new("/root"))
            .expect("schedule root directory");
        let root_wd = mgr.watches.get(Path::new("/root")).expect("root watch").wd;

        let watcher = Arc::new(Mutex::new(mgr));
        let drift_estimator = Arc::new(Mutex::new(DriftEstimator::new(32, 10, 1_000_000)));
        let host_fs = MockHostFs::default()
            .with_metadata("/root/new_dynamic_dir", mock_dir_metadata(10))
            .with_metadata("/root/new_dynamic_dir/nested", mock_dir_metadata(11));
        let shutdown = CancellationToken::new();
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        let created = process_raw_events(
            &watcher,
            &drift_estimator,
            vec![watch_event(
                root_wd,
                HostFsWatchMask::from_bits_truncate(
                    HostFsWatchMask::CREATE.bits() | HostFsWatchMask::ISDIR.bits(),
                ),
                Some("new_dynamic_dir"),
            )],
            &shutdown,
            Path::new("/root"),
            Path::new("/"),
            &node_id,
            &logical_clock,
            &host_fs,
            &mut throttle,
            None,
        )
        .expect("create batch should continue");
        let created_records = decode_records(&created);
        assert_eq!(created_records.len(), 1);
        assert_eq!(created_records[0].path, b"/new_dynamic_dir".to_vec());
        assert!(
            watcher
                .lock()
                .expect("watcher lock")
                .watches
                .contains_key(Path::new("/root/new_dynamic_dir"))
        );

        let new_dir_wd = watcher
            .lock()
            .expect("watcher lock")
            .watches
            .get(Path::new("/root/new_dynamic_dir"))
            .expect("new dir watch")
            .wd;
        let nested = process_raw_events(
            &watcher,
            &drift_estimator,
            vec![watch_event(
                new_dir_wd,
                HostFsWatchMask::from_bits_truncate(
                    HostFsWatchMask::CREATE.bits() | HostFsWatchMask::ISDIR.bits(),
                ),
                Some("nested"),
            )],
            &shutdown,
            Path::new("/root"),
            Path::new("/"),
            &node_id,
            &logical_clock,
            &host_fs,
            &mut throttle,
            None,
        )
        .expect("nested create batch should continue");
        let nested_records = decode_records(&nested);
        assert_eq!(nested_records.len(), 1);
        assert_eq!(nested_records[0].path, b"/new_dynamic_dir/nested".to_vec());

        let nested_wd = watcher
            .lock()
            .expect("watcher lock")
            .watches
            .get(Path::new("/root/new_dynamic_dir/nested"))
            .expect("nested dir watch")
            .wd;
        let removed = process_raw_events(
            &watcher,
            &drift_estimator,
            vec![
                watch_event(nested_wd, HostFsWatchMask::IGNORED, None),
                watch_event(new_dir_wd, HostFsWatchMask::IGNORED, None),
            ],
            &shutdown,
            Path::new("/root"),
            Path::new("/"),
            &node_id,
            &logical_clock,
            &host_fs,
            &mut throttle,
            None,
        )
        .expect("child cleanup should not stop loop");
        assert!(removed.is_empty());

        let mgr = watcher.lock().expect("watcher lock");
        assert!(!mgr.watches.contains_key(Path::new("/root/new_dynamic_dir")));
        assert!(
            !mgr.watches
                .contains_key(Path::new("/root/new_dynamic_dir/nested"))
        );
        assert!(mgr.watches.contains_key(Path::new("/root")));
        assert!(!shutdown.is_cancelled());
    }

    #[test]
    fn process_raw_events_directory_move_emits_source_delete_dest_subtree_and_touches_parents() {
        let config = watch_test_config(8);
        let watch = Box::new(MockWatchBackend::with_add_results(Vec::new()));
        let mut mgr = WatchManager::new(&config, "/root".into(), "/".into(), watch)
            .expect("construct watch manager");
        mgr.schedule(Path::new("/root"))
            .expect("schedule root directory");
        mgr.schedule(Path::new("/root/src_root"))
            .expect("schedule src root");
        mgr.schedule(Path::new("/root/dest_root"))
            .expect("schedule dest root");

        let src_root_wd = mgr
            .watches
            .get(Path::new("/root/src_root"))
            .expect("src root watch")
            .wd;
        let dest_root_wd = mgr
            .watches
            .get(Path::new("/root/dest_root"))
            .expect("dest root watch")
            .wd;
        let src_root_before = mgr
            .watches
            .get(Path::new("/root/src_root"))
            .expect("src root watch")
            .last_activity;
        let dest_root_before = mgr
            .watches
            .get(Path::new("/root/dest_root"))
            .expect("dest root watch")
            .last_activity;

        let watcher = Arc::new(Mutex::new(mgr));
        let drift_estimator = Arc::new(Mutex::new(DriftEstimator::new(32, 10, 1_000_000)));
        let host_fs = MockHostFs::default()
            .with_metadata("/root/dest_root/dest_dir", mock_dir_metadata(20))
            .with_metadata("/root/dest_root/dest_dir/sub_dir", mock_dir_metadata(21))
            .with_metadata(
                "/root/dest_root/dest_dir/file_in_src.txt",
                mock_file_metadata(),
            )
            .with_metadata(
                "/root/dest_root/dest_dir/sub_dir/file_in_sub.txt",
                mock_file_metadata(),
            )
            .with_read_dir(
                "/root/dest_root/dest_dir",
                vec![
                    FsMetaDirEntry {
                        path: PathBuf::from("/root/dest_root/dest_dir/sub_dir"),
                        is_dir: true,
                    },
                    FsMetaDirEntry {
                        path: PathBuf::from("/root/dest_root/dest_dir/file_in_src.txt"),
                        is_dir: false,
                    },
                ],
            )
            .with_read_dir(
                "/root/dest_root/dest_dir/sub_dir",
                vec![FsMetaDirEntry {
                    path: PathBuf::from("/root/dest_root/dest_dir/sub_dir/file_in_sub.txt"),
                    is_dir: false,
                }],
            );
        let shutdown = CancellationToken::new();
        let node_id = NodeId("node-a".to_string());
        let logical_clock = capanix_host_fs_types::LogicalClock::new();
        let mut throttle = ThrottleState::new(Duration::from_millis(5));

        std::thread::sleep(Duration::from_millis(1));
        let moved = process_raw_events(
            &watcher,
            &drift_estimator,
            vec![
                watch_event(
                    src_root_wd,
                    HostFsWatchMask::from_bits_truncate(
                        HostFsWatchMask::MOVED_FROM.bits() | HostFsWatchMask::ISDIR.bits(),
                    ),
                    Some("src_dir"),
                ),
                watch_event(
                    dest_root_wd,
                    HostFsWatchMask::from_bits_truncate(
                        HostFsWatchMask::MOVED_TO.bits() | HostFsWatchMask::ISDIR.bits(),
                    ),
                    Some("dest_dir"),
                ),
            ],
            &shutdown,
            Path::new("/root"),
            Path::new("/"),
            &node_id,
            &logical_clock,
            &host_fs,
            &mut throttle,
            None,
        )
        .expect("move batch should continue");
        let moved_records = decode_records(&moved);
        assert_eq!(moved_records.len(), 5);

        let delete_record = moved_records
            .iter()
            .find(|record| record.path == b"/src_root/src_dir".to_vec())
            .expect("source delete record");
        assert_eq!(delete_record.event_kind, EventKind::Delete);

        let mut update_paths = moved_records
            .iter()
            .filter(|record| record.event_kind == EventKind::Update)
            .map(|record| record.path.clone())
            .collect::<Vec<_>>();
        update_paths.sort();
        assert_eq!(
            update_paths,
            vec![
                b"/dest_root/dest_dir".to_vec(),
                b"/dest_root/dest_dir/file_in_src.txt".to_vec(),
                b"/dest_root/dest_dir/sub_dir".to_vec(),
                b"/dest_root/dest_dir/sub_dir/file_in_sub.txt".to_vec(),
            ]
        );

        let mgr = watcher.lock().expect("watcher lock");
        assert!(
            mgr.watches
                .contains_key(Path::new("/root/dest_root/dest_dir"))
        );
        assert!(
            mgr.watches
                .get(Path::new("/root/src_root"))
                .expect("src root watch")
                .last_activity
                > src_root_before
        );
        assert!(
            mgr.watches
                .get(Path::new("/root/dest_root"))
                .expect("dest root watch")
                .last_activity
                > dest_root_before
        );
        assert!(!shutdown.is_cancelled());
    }
}
