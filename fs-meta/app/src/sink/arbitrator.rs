//! Arbitrator: LWW arbitration with SyncTrack-based authority hierarchy.
//!
//! Implements `process_upsert` and `process_delete` from L3 CONSISTENCY.md.

use std::time::{Duration, Instant};

use capanix_host_fs_types::{EventKind, FileMetaRecord, SyncTrack};

use crate::sink::clock::SinkClock;
use crate::sink::tree::{FileMetaNode, MaterializedTree};

/// Tombstone zombie-rejection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstonePolicy {
    /// Tombstone TTL duration.
    pub ttl: Duration,
    /// mtime tolerance for zombie rejection (microseconds).
    pub tolerance_us: u64,
}

impl Default for TombstonePolicy {
    fn default() -> Self {
        Self {
            // Baseline safe defaults: exceeds NFS acregmax (60s).
            ttl: Duration::from_secs(90),
            tolerance_us: 1_000_000, // 1 second
        }
    }
}

/// Hot file threshold: non-atomic writes are suspect for this duration.
const HOT_FILE_THRESHOLD: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    Ignored,
    UpsertCreated,
    UpsertModified,
    DeleteApplied,
}

/// Process an incoming file metadata record against the materialized tree.
pub fn process_event(
    record: &FileMetaRecord,
    tree: &mut MaterializedTree,
    clock: &SinkClock,
    tombstone_policy: TombstonePolicy,
    current_epoch: u64,
) -> ProcessOutcome {
    match record.event_kind {
        EventKind::Update => process_upsert(record, tree, clock, tombstone_policy, current_epoch),
        EventKind::Delete => process_delete(record, tree, clock, tombstone_policy),
    }
}

/// Upsert processing with LWW arbitration and SyncTrack authority.
fn process_upsert(
    record: &FileMetaRecord,
    tree: &mut MaterializedTree,
    clock: &SinkClock,
    tombstone_policy: TombstonePolicy,
    current_epoch: u64,
) -> ProcessOutcome {
    let path = &record.path;
    let had_existing = tree.get(path).is_some();
    let had_tombstone = tree.get(path).is_some_and(|node| node.is_tombstoned);

    // ── Step 0: Ancestor tombstone repulsion ──
    // Scan events under a tombstoned ancestor are rejected outright.
    // Realtime events may proceed (they represent genuine reincarnation).
    if record.source == SyncTrack::Scan && tree.has_tombstoned_ancestor(path) {
        return ProcessOutcome::Ignored;
    }

    // ── Step 0.25: Compensation parent staleness check ──
    // Reject nested Scan events that claim a parent mtime older than our known reality.
    // L3 strictly limits this to "Audit, new file only".
    if record.source == SyncTrack::Scan && !record.parent_path.is_empty() {
        if tree.get(path).is_none() {
            if let Some(parent) = tree.get(&record.parent_path) {
                if record.parent_mtime_us > 0 && record.parent_mtime_us < parent.modified_time_us {
                    return ProcessOutcome::Ignored;
                }
            }
        }
    }

    // ── Step 0.5: Self-tombstone handling ──
    if let Some(existing) = tree.get(path) {
        if existing.is_tombstoned {
            match record.source {
                SyncTrack::Realtime if record.is_atomic_write => {
                    // Atomic Realtime reincarnation — proceed unconditionally.
                    // CLOSE_WRITE has flush-before-notify guarantee, so if
                    // mtime differs from tombstone, it's a genuine new file.
                }
                SyncTrack::Realtime => {
                    // Non-atomic Realtime (IN_MODIFY, IN_ATTRIB): mtime is
                    // unreliable (stale NFS cache). Apply same zombie check
                    // as Scan to avoid false reincarnation.
                    let tombstone_expired = existing
                        .tombstone_expires_at
                        .map_or(false, |expiry| Instant::now() >= expiry);

                    if tombstone_expired {
                        // TTL expired — allow reincarnation
                    } else {
                        let mtime_diff = if record.unix_stat.mtime_us >= existing.modified_time_us {
                            record.unix_stat.mtime_us - existing.modified_time_us
                        } else {
                            existing.modified_time_us - record.unix_stat.mtime_us
                        };
                        if mtime_diff < tombstone_policy.tolerance_us {
                            tree.with_node_mut(path, |node| {
                                node.last_seen_epoch = current_epoch;
                            });
                            return ProcessOutcome::Ignored; // NFS cache zombie — reject
                        }
                    }
                }
                SyncTrack::Scan => {
                    let tombstone_expired = existing
                        .tombstone_expires_at
                        .map_or(false, |expiry| Instant::now() >= expiry);

                    if tombstone_expired {
                        // TTL expired — allow reincarnation from Scan
                    } else {
                        // Tolerance-based zombie rejection: mtime within window → reject
                        let mtime_diff = if record.unix_stat.mtime_us >= existing.modified_time_us {
                            record.unix_stat.mtime_us - existing.modified_time_us
                        } else {
                            existing.modified_time_us - record.unix_stat.mtime_us
                        };
                        if mtime_diff < tombstone_policy.tolerance_us {
                            // NFS cache zombie — same mtime (within tolerance), reject
                            tree.with_node_mut(path, |node| {
                                node.last_seen_epoch = current_epoch;
                            });
                            return ProcessOutcome::Ignored;
                        }
                        // mtime differs beyond tolerance → genuine reincarnation
                    }
                }
            }
        }
    }

    // Clear tombstone now (only reached for valid reincarnations)
    tree.with_node_mut(path, |existing| {
        if existing.is_tombstoned {
            existing.is_tombstoned = false;
            existing.tombstone_expires_at = None;
        }
    });

    // ── Step 0.75: Type mutation subtree purge ──
    // If an existing path changes type (dir<->file), any cached descendants
    // become invalid under the new type and must be purged first.
    if let Some(existing) = tree.get(path) {
        if existing.is_dir != record.unix_stat.is_dir && !existing.is_tombstoned {
            tree.purge_descendants(path);
        }
    }

    let now = Instant::now();

    if let Some(existing_before) = tree.get(path).cloned() {
        // ── Step 1: Always update last_seen_epoch for Scan events ──
        // This is critical for MID: even if LWW rejects the mtime update,
        // the fact that the node was reported by the scan means it still exists.
        if record.source == SyncTrack::Scan {
            tree.with_node_mut(path, |existing| {
                existing.last_seen_epoch = current_epoch;
            });
        }

        // ── Step 2: LWW check ──
        // audit_skipped bypasses the mtime comparison
        if !record.audit_skipped && existing_before.modified_time_us >= record.unix_stat.mtime_us {
            // Existing is newer or same mtime — reject.
            // Exception: only atomic Realtime (CLOSE_WRITE, DIR_CREATE) can
            // override Scan's mtime. Non-atomic Realtime (IN_MODIFY, IN_ATTRIB)
            // has unreliable mtime on NFS (pre-flush stale cache / arbitrarily
            // old attribute value), so it must NOT override Scan.
            let incoming_is_authoritative =
                record.source == SyncTrack::Realtime && record.is_atomic_write;
            let existing_is_scan = existing_before.source == SyncTrack::Scan;
            if !(incoming_is_authoritative && existing_is_scan) {
                return ProcessOutcome::Ignored;
            }
        }

        let mtime_changed = existing_before.modified_time_us != record.unix_stat.mtime_us;

        // ── Step 3: Apply metadata ──
        tree.with_node_mut(path, |existing| {
            existing.size = record.unix_stat.size;
            existing.modified_time_us = record.unix_stat.mtime_us;
            existing.created_time_us = record.unix_stat.ctime_us;
            existing.is_dir = record.unix_stat.is_dir;
            existing.source = record.source.clone();
            existing.last_seen_epoch = current_epoch;

            // ── Step 4: Integrity flags ──
            match record.source {
                SyncTrack::Realtime => {
                    existing.monitoring_attested = true;
                    existing.last_confirmed_at = Some(now);
                    existing.blind_spot = false;

                    if record.is_atomic_write {
                        // Atomic write clears suspect
                        existing.suspect_until = None;
                    } else {
                        // Non-atomic write: mark suspect for convergence window
                        existing.suspect_until = Some(now + HOT_FILE_THRESHOLD);
                    }
                }
                SyncTrack::Scan => {
                    // Scan events do NOT update last_confirmed_at or clear suspect
                    if mtime_changed {
                        existing.monitoring_attested = false;
                        // If mtime changed but we're in Scan, it might be a blind spot
                        if current_epoch >= 1 {
                            existing.blind_spot = true;
                        }

                        // Suspect window: if file's mtime is "hot" (recent in NFS domain),
                        // mark it suspect for a physical duration. Otherwise clear.
                        let mtime_secs = record.unix_stat.mtime_us as f64 / 1_000_000.0;
                        let age_secs = clock.now_secs() - mtime_secs;
                        if age_secs < HOT_FILE_THRESHOLD.as_secs_f64() {
                            existing.suspect_until = Some(now + HOT_FILE_THRESHOLD);
                        } else {
                            existing.suspect_until = None;
                        }
                    }
                    // If mtime unchanged (audit_skipped case), preserve existing flags
                }
            }
        });
        if had_tombstone {
            ProcessOutcome::UpsertCreated
        } else {
            ProcessOutcome::UpsertModified
        }
    } else {
        // ── New node ──
        let monitoring_attested = record.source == SyncTrack::Realtime;
        let last_confirmed_at = if monitoring_attested { Some(now) } else { None };
        let (suspect_until, blind_spot) = match record.source {
            SyncTrack::Realtime => (
                if record.is_atomic_write {
                    None
                } else {
                    Some(now + HOT_FILE_THRESHOLD)
                },
                false,
            ),
            SyncTrack::Scan => {
                let mtime_secs = record.unix_stat.mtime_us as f64 / 1_000_000.0;
                let age_secs = clock.now_secs() - mtime_secs;
                (
                    if age_secs < HOT_FILE_THRESHOLD.as_secs_f64() {
                        Some(now + HOT_FILE_THRESHOLD)
                    } else {
                        None
                    },
                    current_epoch >= 1,
                )
            }
        };

        tree.insert(
            path.clone(),
            FileMetaNode {
                size: record.unix_stat.size,
                modified_time_us: record.unix_stat.mtime_us,
                created_time_us: record.unix_stat.ctime_us,
                is_dir: record.unix_stat.is_dir,
                source: record.source.clone(),
                monitoring_attested,
                last_confirmed_at,
                suspect_until,
                blind_spot,
                is_tombstoned: false,
                tombstone_expires_at: None,
                last_seen_epoch: current_epoch,
                subtree_last_write_significant_change_at: None,
            },
        );
        if had_existing {
            ProcessOutcome::UpsertModified
        } else {
            ProcessOutcome::UpsertCreated
        }
    }
}

/// Delete processing with SyncTrack-aware behavior.
///
/// - Realtime: authoritative delete — create inline tombstone with TTL.
/// - Scan: compensation delete — LWW mtime check, then hard remove.
fn process_delete(
    record: &FileMetaRecord,
    tree: &mut MaterializedTree,
    _clock: &SinkClock,
    tombstone_policy: TombstonePolicy,
) -> ProcessOutcome {
    let path = &record.path;
    let is_realtime = matches!(record.source, SyncTrack::Realtime);

    if is_realtime {
        // Authoritative delete — create inline tombstone
        tree.with_node_mut(path, |existing| {
            existing.is_tombstoned = true;
            // Preserve original mtime for tolerance comparison — do NOT overwrite.
            existing.tombstone_expires_at = Some(Instant::now() + tombstone_policy.ttl);
            existing.suspect_until = None;
        });

        // Purge all descendants if it was a directory
        if record.unix_stat.is_dir {
            tree.purge_descendants(path);
        }
        ProcessOutcome::DeleteApplied
    } else {
        // Compensation delete — less trusted
        if let Some(existing) = tree.get(path) {
            if existing.is_tombstoned {
                return ProcessOutcome::Ignored; // Already explicitly deleted by Realtime
            }
            if existing.modified_time_us > record.unix_stat.mtime_us {
                return ProcessOutcome::Ignored; // Memory is newer — LWW reject
            }
        }
        // Hard remove (not tombstone) for Scan deletes
        if tree.remove(path).is_some() {
            ProcessOutcome::DeleteApplied
        } else {
            ProcessOutcome::Ignored
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::clock::SinkClock;
    use crate::sink::tree::MaterializedTree;
    use capanix_host_fs_types::UnixStat;

    fn make_record(path: &[u8], mtime: u64, kind: EventKind, source: SyncTrack) -> FileMetaRecord {
        FileMetaRecord::from_unix(
            path.to_vec(),
            path.rsplit(|byte| *byte == b'/')
                .next()
                .unwrap_or(path)
                .to_vec(),
            UnixStat {
                is_dir: false,
                size: 1024,
                mtime_us: mtime,
                ctime_us: mtime,
                dev: None,
                ino: None,
            },
            kind,
            true,
            source,
            Vec::new(),
            0,
            false,
        )
    }

    #[test]
    fn test_upsert_new_node() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let record = make_record(b"/test.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        process_event(&record, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(tree.len(), 1);
        let node = tree.get(b"/test.txt").unwrap();
        assert!(node.monitoring_attested);
        assert!(node.last_confirmed_at.is_some());
    }

    #[test]
    fn test_lww_rejects_older() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let r1 = make_record(b"/a.txt", 2000, EventKind::Update, SyncTrack::Realtime);
        let r2 = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Scan);
        process_event(&r1, &mut tree, &clock, TombstonePolicy::default(), 0);
        process_event(&r2, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(tree.get(b"/a.txt").unwrap().modified_time_us, 2000);
    }

    #[test]
    fn test_realtime_wins_over_scan_same_mtime() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let scan = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Scan);
        let rt = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(!tree.get(b"/a.txt").unwrap().monitoring_attested);
        process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").unwrap().monitoring_attested);
    }

    #[test]
    fn test_scan_same_mtime_preserves_attestation() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let rt = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").unwrap().monitoring_attested);

        let scan_same = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Scan);
        process_event(&scan_same, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").unwrap().monitoring_attested);
    }

    #[test]
    fn test_scan_newer_mtime_can_downgrade_attestation() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        let rt = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").unwrap().monitoring_attested);

        clock.advance(2_000);
        let scan_newer = make_record(b"/a.txt", 2000, EventKind::Update, SyncTrack::Scan);
        process_event(
            &scan_newer,
            &mut tree,
            &clock,
            TombstonePolicy::default(),
            1,
        );
        assert!(!tree.get(b"/a.txt").unwrap().monitoring_attested);
    }

    #[test]
    fn test_scan_same_mtime_preserves_existing_suspect_window() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        clock.advance(10_000_000);

        let scan = make_record(
            b"/steady-hot.txt",
            9_999_000,
            EventKind::Update,
            SyncTrack::Scan,
        );
        process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        let original_suspect_until = tree
            .get(b"/steady-hot.txt")
            .expect("steady hot node")
            .suspect_until;
        assert!(original_suspect_until.is_some());

        clock.advance(20_000_000);
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::Ignored);

        let node = tree.get(b"/steady-hot.txt").expect("steady hot node");
        assert_eq!(node.suspect_until, original_suspect_until);
        assert!(node.blind_spot);
    }

    #[test]
    fn test_scan_newer_hot_mtime_reactivates_expired_suspect_window() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        clock.advance(10_000_000);

        let first = make_record(
            b"/renewed-hot.txt",
            9_999_000,
            EventKind::Update,
            SyncTrack::Scan,
        );
        process_event(&first, &mut tree, &clock, TombstonePolicy::default(), 1);

        tree.with_node_mut(b"/renewed-hot.txt", |expired| {
            expired.suspect_until = Some(Instant::now() - Duration::from_secs(1));
            assert!(!expired.is_currently_suspect());
        })
        .expect("renewable node");

        clock.advance(15_000_000);
        let renewed = make_record(
            b"/renewed-hot.txt",
            14_999_000,
            EventKind::Update,
            SyncTrack::Scan,
        );
        let outcome = process_event(&renewed, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/renewed-hot.txt").expect("renewed node");
        assert!(node.is_currently_suspect());
        assert_eq!(node.modified_time_us, 14_999_000);
        assert!(node.blind_spot);
    }

    #[test]
    fn test_scan_hot_file_sets_blind_spot_and_suspect_after_epoch_one() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        let rt = make_record(
            b"/hot.txt",
            1_000_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);

        clock.advance(10_000_000);
        let scan = make_record(b"/hot.txt", 9_999_000, EventKind::Update, SyncTrack::Scan);
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/hot.txt").expect("hot node");
        assert!(!node.monitoring_attested);
        assert!(node.blind_spot);
        assert!(node.suspect_until.is_some());
    }

    #[test]
    fn test_new_scan_hot_file_sets_blind_spot_and_suspect_after_epoch_one() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        clock.advance(10_000_000);

        let scan = make_record(
            b"/fresh-hot.txt",
            9_999_000,
            EventKind::Update,
            SyncTrack::Scan,
        );
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertCreated);

        let node = tree.get(b"/fresh-hot.txt").expect("new hot node");
        assert!(!node.monitoring_attested);
        assert!(node.blind_spot);
        assert!(node.suspect_until.is_some());
    }

    #[test]
    fn test_new_scan_old_file_is_not_suspect_but_still_blind_spot_after_epoch_one() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        clock.advance(60_000_000);

        let scan = make_record(b"/cold.txt", 1_000_000, EventKind::Update, SyncTrack::Scan);
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertCreated);

        let node = tree.get(b"/cold.txt").expect("cold node");
        assert!(!node.monitoring_attested);
        assert!(node.blind_spot);
        assert_eq!(node.suspect_until, None);
    }

    #[test]
    fn test_new_scan_future_mtime_is_marked_suspect_via_shadow_clock() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        clock.advance(10_000_000);

        let scan = make_record(
            b"/future.txt",
            40_000_000,
            EventKind::Update,
            SyncTrack::Scan,
        );
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertCreated);

        let node = tree.get(b"/future.txt").expect("future node");
        assert!(node.suspect_until.is_some());
        assert!(node.blind_spot);
    }

    #[test]
    fn test_realtime_atomic_write_clears_suspect_and_blind_spot_after_scan_degradation() {
        let mut tree = MaterializedTree::new();
        let mut clock = SinkClock::new();
        let rt = make_record(
            b"/hot.txt",
            1_000_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);

        clock.advance(10_000_000);
        let scan = make_record(b"/hot.txt", 9_999_000, EventKind::Update, SyncTrack::Scan);
        process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 1);

        let rt_atomic = make_record(
            b"/hot.txt",
            10_000_001,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        let outcome = process_event(&rt_atomic, &mut tree, &clock, TombstonePolicy::default(), 1);
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/hot.txt").expect("hot node");
        assert!(node.monitoring_attested);
        assert!(!node.blind_spot);
        assert_eq!(node.suspect_until, None);
    }

    #[test]
    fn test_realtime_partial_writes_keep_suspect_and_update_size_until_atomic_close() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let mut first = make_record(
            b"/streaming.txt",
            1_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        first.is_atomic_write = false;
        first.unix_stat.size = 128;
        let outcome = process_event(&first, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::UpsertCreated);

        let mut second = make_record(
            b"/streaming.txt",
            2_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        second.is_atomic_write = false;
        second.unix_stat.size = 256;
        let outcome = process_event(&second, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/streaming.txt").expect("partial write node");
        assert_eq!(node.size, 256);
        assert_eq!(node.modified_time_us, 2_000);
        assert!(node.suspect_until.is_some());
        assert!(node.monitoring_attested);

        let mut final_write = make_record(
            b"/streaming.txt",
            3_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        final_write.is_atomic_write = true;
        final_write.unix_stat.size = 512;
        let outcome = process_event(
            &final_write,
            &mut tree,
            &clock,
            TombstonePolicy::default(),
            0,
        );
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/streaming.txt").expect("final write node");
        assert_eq!(node.size, 512);
        assert_eq!(node.modified_time_us, 3_000);
        assert_eq!(node.suspect_until, None);
        assert!(!node.blind_spot);
        assert!(node.monitoring_attested);
    }

    #[test]
    fn test_delete_tombstones() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let create = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        let delete = make_record(b"/a.txt", 1000, EventKind::Delete, SyncTrack::Realtime);
        process_event(&create, &mut tree, &clock, TombstonePolicy::default(), 0);
        process_event(&delete, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").unwrap().is_tombstoned);
        // Tombstone should have physical expiry
        assert!(tree.get(b"/a.txt").unwrap().tombstone_expires_at.is_some());
    }

    #[test]
    fn test_scan_delete_hard_removes() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let create = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Scan);
        let delete = make_record(b"/a.txt", 1000, EventKind::Delete, SyncTrack::Scan);
        process_event(&create, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").is_some());
        process_event(&delete, &mut tree, &clock, TombstonePolicy::default(), 0);
        // Scan delete should hard-remove, not tombstone
        assert!(tree.get(b"/a.txt").is_none());
    }

    #[test]
    fn test_scan_delete_lww_rejects_stale() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        // Insert with mtime=2000 via Realtime
        let create = make_record(b"/a.txt", 2000, EventKind::Update, SyncTrack::Realtime);
        process_event(&create, &mut tree, &clock, TombstonePolicy::default(), 0);
        // Scan delete with mtime=1000 (stale) should be rejected
        let delete = make_record(b"/a.txt", 1000, EventKind::Delete, SyncTrack::Scan);
        process_event(&delete, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/a.txt").is_some()); // Still present
        assert!(!tree.get(b"/a.txt").unwrap().is_tombstoned);
    }

    #[test]
    fn test_configurable_tombstone_policy_applies() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let policy = TombstonePolicy {
            ttl: Duration::from_secs(1),
            tolerance_us: 10,
        };

        let create = make_record(b"/a.txt", 1000, EventKind::Update, SyncTrack::Realtime);
        let delete = make_record(b"/a.txt", 1000, EventKind::Delete, SyncTrack::Realtime);
        process_event(&create, &mut tree, &clock, policy, 0);
        process_event(&delete, &mut tree, &clock, policy, 0);
        let expiry = tree
            .get(b"/a.txt")
            .and_then(|node| node.tombstone_expires_at)
            .expect("tombstone expiry");
        assert!(expiry > Instant::now());
        assert!(expiry <= Instant::now() + Duration::from_secs(2));
    }

    #[test]
    fn test_scan_under_tombstoned_ancestor_is_rejected() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let mut dir = make_record(b"/A", 1_000, EventKind::Update, SyncTrack::Realtime);
        dir.unix_stat.is_dir = true;
        process_event(&dir, &mut tree, &clock, TombstonePolicy::default(), 0);

        let mut del = make_record(b"/A", 1_001, EventKind::Delete, SyncTrack::Realtime);
        del.unix_stat.is_dir = true;
        process_event(&del, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/A").is_some_and(|n| n.is_tombstoned));

        let mut child = make_record(b"/A/B/file.txt", 1_002, EventKind::Update, SyncTrack::Scan);
        child.parent_path = b"/A/B".to_vec();
        let outcome = process_event(&child, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::Ignored);
        assert!(tree.get(b"/A/B/file.txt").is_none());
    }

    #[test]
    fn test_parent_mtime_staleness_rejects_new_scan_insert() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let mut parent = make_record(b"/dir", 2_000, EventKind::Update, SyncTrack::Realtime);
        parent.unix_stat.is_dir = true;
        process_event(&parent, &mut tree, &clock, TombstonePolicy::default(), 0);

        let mut child = make_record(b"/dir/file.txt", 1_900, EventKind::Update, SyncTrack::Scan);
        child.parent_path = b"/dir".to_vec();
        child.parent_mtime_us = 1_000;
        let outcome = process_event(&child, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::Ignored);
        assert!(tree.get(b"/dir/file.txt").is_none());
    }

    #[test]
    fn test_realtime_atomic_overrides_scan_even_with_older_mtime() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let scan = make_record(b"/a.txt", 1_000, EventKind::Update, SyncTrack::Scan);
        process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 0);

        let mut rt = make_record(b"/a.txt", 900, EventKind::Update, SyncTrack::Realtime);
        rt.is_atomic_write = true;
        let outcome = process_event(&rt, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::UpsertModified);

        let node = tree.get(b"/a.txt").expect("node");
        assert_eq!(node.modified_time_us, 900);
        assert!(node.monitoring_attested);
    }

    #[test]
    fn test_scan_zombie_rejected_within_tombstone_tolerance() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let create = make_record(b"/z.txt", 1_000, EventKind::Update, SyncTrack::Realtime);
        let delete = make_record(b"/z.txt", 1_000, EventKind::Delete, SyncTrack::Realtime);
        process_event(&create, &mut tree, &clock, TombstonePolicy::default(), 0);
        process_event(&delete, &mut tree, &clock, TombstonePolicy::default(), 0);

        let scan = make_record(b"/z.txt", 1_000, EventKind::Update, SyncTrack::Scan);
        let outcome = process_event(&scan, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert_eq!(outcome, ProcessOutcome::Ignored);
        assert!(tree.get(b"/z.txt").is_some_and(|n| n.is_tombstoned));
    }

    #[test]
    fn test_scan_newer_than_tombstone_resurrects_node() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let policy = TombstonePolicy {
            ttl: Duration::from_secs(90),
            tolerance_us: 1,
        };

        let create = make_record(
            b"/revive.txt",
            1_000,
            EventKind::Update,
            SyncTrack::Realtime,
        );
        let delete = make_record(
            b"/revive.txt",
            1_000,
            EventKind::Delete,
            SyncTrack::Realtime,
        );
        process_event(&create, &mut tree, &clock, policy, 0);
        process_event(&delete, &mut tree, &clock, policy, 0);
        assert!(tree.get(b"/revive.txt").is_some_and(|n| n.is_tombstoned));

        let scan = make_record(b"/revive.txt", 1_003, EventKind::Update, SyncTrack::Scan);
        let outcome = process_event(&scan, &mut tree, &clock, policy, 1);
        assert_eq!(outcome, ProcessOutcome::UpsertCreated);

        let node = tree.get(b"/revive.txt").expect("revived node");
        assert!(!node.is_tombstoned);
        assert_eq!(node.modified_time_us, 1_003);
    }

    #[test]
    fn test_type_mutation_dir_to_file_purges_descendants() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();
        let mut dir = make_record(b"/swap", 1_000, EventKind::Update, SyncTrack::Realtime);
        dir.unix_stat.is_dir = true;
        process_event(&dir, &mut tree, &clock, TombstonePolicy::default(), 0);
        process_event(
            &make_record(
                b"/swap/child.txt",
                1_001,
                EventKind::Update,
                SyncTrack::Realtime,
            ),
            &mut tree,
            &clock,
            TombstonePolicy::default(),
            0,
        );
        assert!(tree.get(b"/swap/child.txt").is_some());

        let mut file = make_record(b"/swap", 1_002, EventKind::Update, SyncTrack::Realtime);
        file.unix_stat.is_dir = false;
        process_event(&file, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/swap/child.txt").is_none());
        assert!(tree.get(b"/swap").is_some_and(|n| !n.is_dir));
    }

    #[test]
    fn test_type_mutation_file_to_dir_purges_descendants() {
        let mut tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let mut file = make_record(b"/swap", 1_000, EventKind::Update, SyncTrack::Realtime);
        file.unix_stat.is_dir = false;
        process_event(&file, &mut tree, &clock, TombstonePolicy::default(), 0);

        // Simulate stale/corrupted orphan descendants under a file path.
        process_event(
            &make_record(b"/swap/child.txt", 999, EventKind::Update, SyncTrack::Scan),
            &mut tree,
            &clock,
            TombstonePolicy::default(),
            0,
        );
        assert!(tree.get(b"/swap/child.txt").is_some());

        let mut dir = make_record(b"/swap", 1_001, EventKind::Update, SyncTrack::Realtime);
        dir.unix_stat.is_dir = true;
        process_event(&dir, &mut tree, &clock, TombstonePolicy::default(), 0);
        assert!(tree.get(b"/swap/child.txt").is_none());
        assert!(tree.get(b"/swap").is_some_and(|n| n.is_dir));
    }
}
