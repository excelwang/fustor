//! Epoch management and Missing Item Detection.
//!
//! Processes in-band `ControlEvent` signals (EpochStart/End)
//! and triggers MID on epoch boundaries.

use std::collections::HashSet;
use std::time::Instant;

use capanix_host_fs_types::{ControlEvent, EpochType};

use crate::sink::tree::MaterializedTree;

/// Epoch lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochState {
    /// No active audit scan.
    Idle,
    /// Audit scan in progress for the given epoch.
    Active(u64),
}

/// Epoch manager handling audit lifecycle.
pub struct EpochManager {
    pub state: EpochState,
    pub completed_epochs: u64,
    /// Physical time when the current audit epoch started.
    /// Used by Stale Evidence Guard to protect nodes confirmed by Realtime after audit began.
    pub audit_start_time: Option<Instant>,
    audit_skipped_paths: HashSet<Vec<u8>>,
}

impl EpochManager {
    pub fn new() -> Self {
        Self {
            state: EpochState::Idle,
            completed_epochs: 0,
            audit_start_time: None,
            audit_skipped_paths: HashSet::new(),
        }
    }

    /// Process a control event. Returns `Some((epoch_id, start_time))` if MID should be triggered.
    pub fn process_control_event(&mut self, event: &ControlEvent) -> Option<(u64, Instant)> {
        match event {
            ControlEvent::EpochStart {
                epoch_id,
                epoch_type: EpochType::Audit,
            } => {
                self.state = EpochState::Active(*epoch_id);
                self.audit_start_time = Some(Instant::now());
                self.audit_skipped_paths.clear();
                log::info!("Epoch {} started (Audit)", epoch_id);
                None
            }
            ControlEvent::EpochEnd {
                epoch_id,
                epoch_type: EpochType::Audit,
            } => {
                let start_time = self.audit_start_time.take();
                self.state = EpochState::Idle;
                self.completed_epochs += 1;
                log::info!(
                    "Epoch {} ended (Audit), total completed: {}",
                    epoch_id,
                    self.completed_epochs
                );
                // Only trigger MID for epoch >= 1 (not for baseline)
                if *epoch_id >= 1 {
                    // Pass the audit start time for Stale Evidence Guard
                    Some((*epoch_id, start_time.unwrap_or_else(Instant::now)))
                } else {
                    None
                }
            }
            ControlEvent::WatchOverflow => None,
        }
    }

    /// Current epoch ID (0 if none active).
    pub fn current_epoch(&self) -> u64 {
        match self.state {
            EpochState::Active(id) => id,
            EpochState::Idle => self.completed_epochs,
        }
    }

    pub fn mark_audit_skipped(&mut self, path: Vec<u8>) {
        self.audit_skipped_paths.insert(path);
    }

    pub fn is_audit_skipped(&self, path: &[u8]) -> bool {
        self.audit_skipped_paths.contains(path)
    }

    pub fn has_audit_skipped_ancestor(&self, path: &[u8]) -> bool {
        if path == b"/" {
            return false;
        }
        let mut current = path;
        loop {
            let Some(pos) = current.iter().rposition(|b| *b == b'/') else {
                return false;
            };
            current = if pos == 0 { b"/" } else { &current[..pos] };
            if self.is_audit_skipped(current) {
                return true;
            }
            if current == b"/" {
                return false;
            }
        }
    }

    pub fn clear_completed_audit_skip_state(&mut self) {
        self.audit_skipped_paths.clear();
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Missing Item Detection: identify nodes not seen in the current audit epoch.
///
/// Called after `EpochEnd` for epoch >= 1. Nodes that were not refreshed
/// by the audit scan are considered deleted and removed from the tree.
///
/// Protections:
/// - Skip tombstoned nodes
/// - Skip audit_skipped nodes and their descendants
/// - **Stale Evidence Guard**: skip nodes confirmed by Realtime AFTER audit started
/// - Tombstone GC: remove expired tombstones
pub fn missing_item_detection(
    tree: &mut MaterializedTree,
    completed_epoch: u64,
    audit_start_time: Instant,
    epoch_manager: &EpochManager,
) -> Vec<Vec<u8>> {
    let now = Instant::now();

    let stale_paths: Vec<Vec<u8>> = tree
        .iter()
        .filter(|(path, node)| {
            // Skip tombstoned nodes
            if node.is_tombstoned {
                return false;
            }
            // Skip audit-skipped directories
            if epoch_manager.is_audit_skipped(path) {
                return false;
            }
            // Skip nodes under audit-skipped ancestor directories
            if epoch_manager.has_audit_skipped_ancestor(path) {
                return false;
            }
            // Stale Evidence Guard: skip nodes confirmed by Realtime AFTER audit started
            if let Some(confirmed_at) = node.last_confirmed_at {
                if confirmed_at > audit_start_time {
                    return false; // Realtime says this file exists NOW
                }
            }
            // Node not seen in completed epoch
            node.last_seen_epoch < completed_epoch
        })
        .map(|(path, _)| path.to_vec())
        .collect();

    // Remove stale nodes (spec says tree.remove, NOT tombstone)
    for path in &stale_paths {
        log::debug!(
            "MID: removing {:?} (last_seen_epoch < {})",
            String::from_utf8_lossy(path),
            completed_epoch
        );
        tree.remove(path);
    }

    // Tombstone TTL cleanup: remove expired tombstones
    let expired_tombstones: Vec<Vec<u8>> = tree
        .iter()
        .filter(|(_, node)| {
            node.is_tombstoned
                && node
                    .tombstone_expires_at
                    .map_or(true, |expiry| now >= expiry)
        })
        .map(|(path, _)| path.to_vec())
        .collect();

    for path in &expired_tombstones {
        log::debug!(
            "MID: GC expired tombstone {:?}",
            String::from_utf8_lossy(path)
        );
        tree.remove(path);
    }

    stale_paths
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use capanix_host_fs_types::{ControlEvent, EpochType, SyncTrack};

    use super::*;
    use crate::sink::tree::FileMetaNode;

    fn make_node(_path: &[u8], is_dir: bool, last_seen_epoch: u64) -> FileMetaNode {
        FileMetaNode {
            size: 0,
            modified_time_us: 1_000,
            created_time_us: 1_000,
            is_dir,
            source: SyncTrack::Scan,
            monitoring_attested: false,
            last_confirmed_at: None,
            suspect_until: None,
            blind_spot: false,
            is_tombstoned: false,
            tombstone_expires_at: None,
            last_seen_epoch,
            subtree_last_write_significant_change_at: None,
        }
    }

    #[test]
    fn test_epoch_manager_triggers_mid_only_for_epoch_ge_1() {
        let mut mgr = EpochManager::new();

        assert!(
            mgr.process_control_event(&ControlEvent::EpochStart {
                epoch_id: 0,
                epoch_type: EpochType::Audit
            })
            .is_none()
        );
        assert!(
            mgr.process_control_event(&ControlEvent::EpochEnd {
                epoch_id: 0,
                epoch_type: EpochType::Audit
            })
            .is_none()
        );

        assert!(
            mgr.process_control_event(&ControlEvent::EpochStart {
                epoch_id: 1,
                epoch_type: EpochType::Audit
            })
            .is_none()
        );
        let mid = mgr.process_control_event(&ControlEvent::EpochEnd {
            epoch_id: 1,
            epoch_type: EpochType::Audit,
        });
        assert!(mid.is_some());
        assert_eq!(mgr.completed_epochs, 2);
        assert_eq!(mgr.state, EpochState::Idle);
    }

    #[test]
    fn test_mid_skips_audit_skipped_and_descendants_and_realtime_confirmed_nodes() {
        let mut tree = MaterializedTree::new();
        let mut mgr = EpochManager::new();
        let audit_start = Instant::now();

        // Stale ordinary node should be removed.
        tree.insert(b"/stale.txt".to_vec(), make_node(b"/stale.txt", false, 0));

        // audit_skipped directory and child should survive MID.
        let skipped_dir = make_node(b"/stable", true, 0);
        tree.insert(b"/stable".to_vec(), skipped_dir);
        mgr.mark_audit_skipped(b"/stable".to_vec());
        tree.insert(
            b"/stable/child.txt".to_vec(),
            make_node(b"/stable/child.txt", false, 0),
        );

        // Realtime confirmed after audit start should survive MID.
        let mut confirmed = make_node(b"/confirmed.txt", false, 0);
        confirmed.last_confirmed_at = Some(audit_start + Duration::from_millis(1));
        tree.insert(b"/confirmed.txt".to_vec(), confirmed);

        let removed = missing_item_detection(&mut tree, 1, audit_start, &mgr);
        assert_eq!(removed, vec![b"/stale.txt".to_vec()]);
        assert!(tree.get(b"/stale.txt").is_none());
        assert!(tree.get(b"/stable").is_some());
        assert!(tree.get(b"/stable/child.txt").is_some());
        assert!(tree.get(b"/confirmed.txt").is_some());
    }

    #[test]
    fn test_mid_gc_removes_expired_tombstones() {
        let mut tree = MaterializedTree::new();
        let mut expired = make_node(b"/expired", false, 1);
        expired.is_tombstoned = true;
        expired.tombstone_expires_at = Some(Instant::now() - Duration::from_millis(1));
        tree.insert(b"/expired".to_vec(), expired);

        let mut active = make_node(b"/active", false, 1);
        active.is_tombstoned = true;
        active.tombstone_expires_at = Some(Instant::now() + Duration::from_secs(10));
        tree.insert(b"/active".to_vec(), active);

        let removed = missing_item_detection(&mut tree, 1, Instant::now(), &EpochManager::new());
        assert!(removed.is_empty());
        assert!(tree.get(b"/expired").is_none());
        assert!(tree.get(b"/active").is_some());
    }

    #[test]
    fn test_mid_gc_removes_tombstone_at_exact_expiry_boundary() {
        let mut tree = MaterializedTree::new();
        let mut boundary = make_node(b"/boundary", false, 1);
        boundary.is_tombstoned = true;
        boundary.tombstone_expires_at = Some(Instant::now());
        tree.insert(b"/boundary".to_vec(), boundary);

        let removed = missing_item_detection(
            &mut tree,
            1,
            Instant::now() - Duration::from_secs(1),
            &EpochManager::new(),
        );
        assert!(removed.is_empty());
        assert!(tree.get(b"/boundary").is_none());
    }

    #[test]
    fn test_mid_preserves_tombstone_created_after_audit_start_when_not_expired() {
        let mut tree = MaterializedTree::new();
        let mut fresh = make_node(b"/fresh-after-audit", false, 1);
        fresh.is_tombstoned = true;
        fresh.tombstone_expires_at = Some(Instant::now() + Duration::from_secs(5));
        tree.insert(b"/fresh-after-audit".to_vec(), fresh);

        let removed = missing_item_detection(
            &mut tree,
            1,
            Instant::now() - Duration::from_secs(1),
            &EpochManager::new(),
        );
        assert!(removed.is_empty());
        assert!(tree.get(b"/fresh-after-audit").is_some());
    }
}
