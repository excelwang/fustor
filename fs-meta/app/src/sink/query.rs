//! Query engine for the materialized tree.
//!
//! Provides structured query APIs for the current fs-meta contract interface:
//! - `get_directory_tree`: recursive directory listing
//! - `get_subtree_stats`: aggregate stats for a subtree
//! - `get_health_stats`: reliability metrics

use crate::query::models::{HealthStats, QueryNode, SubtreeStats};
use crate::query::path::{
    normalized_path_for_query, parent_path as parent_path_bytes, relative_depth_under_root,
    root_file_name_bytes,
};
use crate::query::reliability::{GroupReliability, ReliabilityAccumulator};
use crate::query::result_ops::RawQueryResult;
use crate::query::tree::{
    ReadClass, StabilityState, TreeGroupPayload, TreePageEntry, TreePageRoot, TreeStability,
};
use crate::sink::clock::SinkClock;
use crate::sink::tree::{FileMetaNode, MaterializedTree};

use crate::shared_types::query::{StabilityMode, UnreliableReason};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const TRUSTED_READ_QUIET_WINDOW_MS: u64 = 5_000;

pub(crate) fn query_node_from_node(path: &[u8], node: &FileMetaNode) -> QueryNode {
    QueryNode {
        path: path.to_vec(),
        file_name: root_file_name_bytes(path),
        size: node.size,
        modified_time_us: node.modified_time_us,
        is_dir: node.is_dir,
        monitoring_attested: node.monitoring_attested,
        is_suspect: node.is_currently_suspect(),
        is_blind_spot: node.blind_spot,
    }
}

/// Get directory tree listing.
pub fn get_directory_tree(
    tree: &MaterializedTree,
    dir_path: &[u8],
    _clock: &SinkClock,
    overflow_pending_audit: bool,
    recursive: bool,
    max_depth: Option<usize>,
) -> RawQueryResult {
    let descendants = tree.descendants_of(dir_path);

    let mut nodes = Vec::new();
    let mut reliability = ReliabilityAccumulator::new();

    let mut consider_node = |path: &[u8], node: &FileMetaNode| {
        if node.is_tombstoned {
            return;
        }
        let qn = query_node_from_node(path, node);
        reliability.observe_flags(
            node.monitoring_attested,
            node.is_currently_suspect(),
            node.blind_spot,
        );
        nodes.push(qn);
    };

    // Include the query root itself when it exists.
    if let Some(root) = tree.get(dir_path) {
        consider_node(dir_path, root);
    }

    let depth_limit = if recursive {
        max_depth.unwrap_or(usize::MAX)
    } else {
        1usize
    };

    // Include descendants under the query root with depth semantics.
    for (path, node) in &descendants {
        let Some(depth) = relative_depth_under_root(path, dir_path) else {
            continue;
        };
        if depth > depth_limit {
            continue;
        }
        consider_node(path, node);
    }

    reliability.observe_overflow(overflow_pending_audit);
    let reliability = reliability.finish();

    RawQueryResult {
        nodes,
        reliable: reliability.reliable,
        unreliable_reason: reliability.unreliable_reason,
    }
}

fn blocked_reason(reason: &UnreliableReason) -> &'static str {
    match reason {
        UnreliableReason::Unattested => "unattested_nodes",
        UnreliableReason::SuspectNodes => "suspect_nodes",
        UnreliableReason::BlindSpotsDetected => "blind_spots_detected",
        UnreliableReason::WatchOverflowPendingAudit => "watch_overflow_pending_audit",
    }
}

fn quiet_anchor_for_direct_query(tree: &MaterializedTree, dir_path: &[u8]) -> Option<Instant> {
    if let Some(node) = tree.get(dir_path) {
        return node.subtree_last_write_significant_change_at;
    }

    let latest = tree
        .descendant_iter(dir_path)
        .filter_map(|(_, node)| node.subtree_last_write_significant_change_at)
        .max();
    if latest.is_some() || dir_path == b"/" {
        return latest;
    }

    let mut current = dir_path;
    loop {
        let Some(pos) = current.iter().rposition(|b| *b == b'/') else {
            return latest;
        };
        current = if pos == 0 { b"/" } else { &current[..pos] };
        if let Some(node) = tree.get(current)
            && node.subtree_last_write_significant_change_at.is_some()
        {
            return node.subtree_last_write_significant_change_at;
        }
        if current == b"/" {
            return latest;
        }
    }
}

fn evaluate_tree_stability(
    response: &RawQueryResult,
    overflow_pending_audit: bool,
    stability_mode: StabilityMode,
    quiet_window_ms: Option<u64>,
    quiet_anchor: Option<Instant>,
    last_coverage_recovered_at: Option<Instant>,
) -> TreeStability {
    if stability_mode == StabilityMode::None {
        return TreeStability::not_evaluated();
    }

    let mut blocked_reasons = Vec::<String>::new();
    let mut state = StabilityState::Stable;
    if overflow_pending_audit {
        blocked_reasons.push("watch_overflow_pending_audit".to_string());
        state = StabilityState::Unknown;
    }
    if let Some(reason) = response.unreliable_reason.as_ref() {
        blocked_reasons.push(blocked_reason(reason).to_string());
        state = match reason {
            UnreliableReason::SuspectNodes => StabilityState::Unstable,
            UnreliableReason::Unattested
            | UnreliableReason::BlindSpotsDetected
            | UnreliableReason::WatchOverflowPendingAudit => StabilityState::Unknown,
        };
    }

    let quiet_window_ms = quiet_window_ms.or(Some(0));
    let quiet_anchor = quiet_anchor
        .into_iter()
        .chain(last_coverage_recovered_at)
        .max();
    let observed_quiet_for_ms = quiet_anchor
        .map(|anchor| Instant::now().saturating_duration_since(anchor).as_millis() as u64);
    let remaining_ms = observed_quiet_for_ms
        .zip(quiet_window_ms)
        .map(|(observed, window)| window.saturating_sub(observed));

    if state == StabilityState::Stable {
        let window = quiet_window_ms.unwrap_or(0);
        let observed = observed_quiet_for_ms.unwrap_or(0);
        state = if observed >= window {
            StabilityState::Stable
        } else {
            StabilityState::Unstable
        };
    }

    TreeStability {
        mode: stability_mode,
        state,
        quiet_window_ms,
        observed_quiet_for_ms,
        remaining_ms,
        blocked_reasons,
    }
}

fn normalized_node_path_for_query(node_path: &[u8], query_path: &[u8]) -> Option<Vec<u8>> {
    normalized_path_for_query(node_path, query_path)
}

#[derive(Clone)]
struct RebasedNode<'a> {
    path: Vec<u8>,
    node: &'a FileMetaNode,
}

fn collect_rebased_nodes<'a>(
    tree: &'a MaterializedTree,
    query_path: &[u8],
) -> Vec<RebasedNode<'a>> {
    let mut by_path = HashMap::<Vec<u8>, &'a FileMetaNode>::new();
    for (path, node) in tree.iter() {
        if node.is_tombstoned {
            continue;
        }
        let Some(normalized_path) = normalized_node_path_for_query(path, query_path) else {
            continue;
        };
        by_path.insert(normalized_path, node);
    }
    let mut nodes = by_path
        .into_iter()
        .map(|(path, node)| RebasedNode { path, node })
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| a.path.cmp(&b.path));
    nodes
}

fn quiet_anchor_for_rebased_nodes(nodes: &[RebasedNode<'_>]) -> Option<Instant> {
    nodes
        .iter()
        .filter_map(|mapped| mapped.node.subtree_last_write_significant_change_at)
        .max()
}

fn page_root_from_tree(tree: &MaterializedTree, dir_path: &[u8]) -> TreePageRoot {
    if let Some(node) = tree.get(dir_path)
        && !node.is_tombstoned
    {
        return TreePageRoot {
            path: dir_path.to_vec(),
            size: node.size,
            modified_time_us: node.modified_time_us,
            is_dir: node.is_dir,
            exists: true,
            has_children: node.is_dir && tree.has_live_child(dir_path),
        };
    }

    let subtree_has_live_nodes = tree
        .aggregate_at(dir_path)
        .map(|aggregate| aggregate.total_nodes > 0)
        .unwrap_or(false);

    TreePageRoot {
        path: if dir_path.is_empty() {
            b"/".to_vec()
        } else {
            dir_path.to_vec()
        },
        size: 0,
        modified_time_us: 0,
        is_dir: true,
        exists: subtree_has_live_nodes,
        has_children: tree.has_live_child(dir_path),
    }
}

fn page_root_from_rebased_nodes(nodes: &[RebasedNode<'_>], query_path: &[u8]) -> TreePageRoot {
    let root_node = nodes
        .iter()
        .find(|mapped| mapped.path.as_slice() == query_path);
    let has_children = nodes
        .iter()
        .any(|candidate| parent_path_bytes(&candidate.path).as_deref() == Some(query_path));
    if let Some(root) = root_node {
        return TreePageRoot {
            path: root.path.clone(),
            size: root.node.size,
            modified_time_us: root.node.modified_time_us,
            is_dir: root.node.is_dir,
            exists: true,
            has_children,
        };
    }

    TreePageRoot {
        path: if query_path.is_empty() {
            b"/".to_vec()
        } else {
            query_path.to_vec()
        },
        size: 0,
        modified_time_us: 0,
        is_dir: true,
        exists: !nodes.is_empty(),
        has_children,
    }
}

fn entry_from_node(
    query_root: &[u8],
    path: &[u8],
    node: &FileMetaNode,
    tree: &MaterializedTree,
) -> TreePageEntry {
    TreePageEntry {
        path: path.to_vec(),
        depth: relative_depth_under_root(path, query_root).unwrap_or(0),
        size: node.size,
        modified_time_us: node.modified_time_us,
        is_dir: node.is_dir,
        has_children: node.is_dir && tree.has_live_child(path),
    }
}

fn entry_from_rebased_node(query_root: &[u8], path: &[u8], node: &FileMetaNode) -> TreePageEntry {
    TreePageEntry {
        path: path.to_vec(),
        depth: relative_depth_under_root(path, query_root).unwrap_or(0),
        size: node.size,
        modified_time_us: node.modified_time_us,
        is_dir: node.is_dir,
        has_children: false,
    }
}

fn page_entries_from_tree(
    tree: &MaterializedTree,
    dir_path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
) -> Vec<TreePageEntry> {
    let mut entries = Vec::<TreePageEntry>::new();
    let depth_limit = if recursive {
        max_depth.unwrap_or(usize::MAX)
    } else {
        1usize
    };

    if recursive {
        for (path, node) in tree.descendant_iter(dir_path) {
            if node.is_tombstoned {
                continue;
            }
            let Some(depth) = relative_depth_under_root(path, dir_path) else {
                continue;
            };
            if depth == 0 || depth > depth_limit {
                continue;
            }
            entries.push(entry_from_node(dir_path, path, node, tree));
        }
    } else {
        for (path, node) in tree.child_iter(dir_path) {
            if node.is_tombstoned {
                continue;
            }
            entries.push(entry_from_node(dir_path, path, node, tree));
        }
    }

    entries
}

fn page_entries_from_rebased_nodes(
    nodes: &[RebasedNode<'_>],
    dir_path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
) -> Vec<TreePageEntry> {
    let depth_limit = if recursive {
        max_depth.unwrap_or(usize::MAX)
    } else {
        1usize
    };
    let has_children_by_path = nodes
        .iter()
        .filter_map(|mapped| parent_path_bytes(&mapped.path))
        .collect::<HashSet<_>>();
    let mut entries = Vec::<TreePageEntry>::new();

    for mapped in nodes {
        if mapped.path.as_slice() == dir_path {
            continue;
        }
        let Some(depth) = relative_depth_under_root(&mapped.path, dir_path) else {
            continue;
        };
        if depth == 0 || depth > depth_limit {
            continue;
        }
        let mut entry = entry_from_rebased_node(dir_path, &mapped.path, mapped.node);
        entry.has_children = has_children_by_path.contains(&mapped.path);
        entries.push(entry);
    }

    entries
}

fn query_response_from_rebased_nodes(
    nodes: &[RebasedNode<'_>],
    query_root: &[u8],
    overflow_pending_audit: bool,
    recursive: bool,
    max_depth: Option<usize>,
) -> RawQueryResult {
    let depth_limit = if recursive {
        max_depth.unwrap_or(usize::MAX)
    } else {
        1usize
    };
    let mut response_nodes = Vec::<QueryNode>::new();
    let mut reliability = ReliabilityAccumulator::new();

    let mut consider_node = |path: &[u8], node: &FileMetaNode| {
        reliability.observe_flags(
            node.monitoring_attested,
            node.is_currently_suspect(),
            node.blind_spot,
        );
        response_nodes.push(QueryNode {
            path: path.to_vec(),
            file_name: root_file_name_bytes(path),
            size: node.size,
            modified_time_us: node.modified_time_us,
            is_dir: node.is_dir,
            monitoring_attested: node.monitoring_attested,
            is_suspect: node.is_currently_suspect(),
            is_blind_spot: node.blind_spot,
        });
    };

    for mapped in nodes {
        if mapped.path.as_slice() == query_root {
            consider_node(&mapped.path, mapped.node);
            continue;
        }
        let Some(depth) = relative_depth_under_root(&mapped.path, query_root) else {
            continue;
        };
        if depth == 0 || depth > depth_limit {
            continue;
        }
        consider_node(&mapped.path, mapped.node);
    }

    reliability.observe_overflow(overflow_pending_audit);
    let reliability = reliability.finish();

    RawQueryResult {
        nodes: response_nodes,
        reliable: reliability.reliable,
        unreliable_reason: reliability.unreliable_reason,
    }
}

pub fn get_materialized_tree_payload(
    tree: &MaterializedTree,
    dir_path: &[u8],
    clock: &SinkClock,
    overflow_pending_audit: bool,
    recursive: bool,
    max_depth: Option<usize>,
    read_class: ReadClass,
    last_coverage_recovered_at: Option<Instant>,
) -> TreeGroupPayload {
    let (stability_mode, quiet_window_ms) = match read_class {
        ReadClass::Fresh => (StabilityMode::None, None),
        ReadClass::Materialized | ReadClass::TrustedMaterialized => (
            StabilityMode::QuietWindow,
            Some(TRUSTED_READ_QUIET_WINDOW_MS),
        ),
    };
    let direct_query = tree.has_path(dir_path);
    if direct_query {
        let direct = get_directory_tree(
            tree,
            dir_path,
            clock,
            overflow_pending_audit,
            recursive,
            max_depth,
        );
        let stability = evaluate_tree_stability(
            &direct,
            overflow_pending_audit,
            stability_mode,
            quiet_window_ms,
            quiet_anchor_for_direct_query(tree, dir_path),
            last_coverage_recovered_at,
        );
        let root = page_root_from_tree(tree, dir_path);
        let entries = page_entries_from_tree(tree, dir_path, recursive, max_depth);
        let reliability = GroupReliability {
            reliable: direct.reliable,
            unreliable_reason: direct.unreliable_reason.clone(),
        };
        return TreeGroupPayload {
            reliability,
            stability,
            root,
            entries,
        };
    }

    let rebased = collect_rebased_nodes(tree, dir_path);
    let query = query_response_from_rebased_nodes(
        &rebased,
        dir_path,
        overflow_pending_audit,
        recursive,
        max_depth,
    );
    let stability = evaluate_tree_stability(
        &query,
        overflow_pending_audit,
        stability_mode,
        quiet_window_ms,
        quiet_anchor_for_rebased_nodes(&rebased),
        last_coverage_recovered_at,
    );
    let root = page_root_from_rebased_nodes(&rebased, dir_path);
    let entries = page_entries_from_rebased_nodes(&rebased, dir_path, recursive, max_depth);
    let reliability = GroupReliability {
        reliable: query.reliable,
        unreliable_reason: query.unreliable_reason.clone(),
    };
    TreeGroupPayload {
        reliability,
        stability,
        root,
        entries,
    }
}

/// Get aggregate stats for a subtree.
///
/// Used by `SinkFileMeta::subtree_stats()` and RPC query service.
pub fn get_subtree_stats(tree: &MaterializedTree, dir_path: &[u8]) -> SubtreeStats {
    let mut stats = tree
        .aggregate_at(dir_path)
        .map(|aggregate| SubtreeStats {
            total_nodes: aggregate.total_nodes,
            total_files: aggregate.total_files,
            total_dirs: aggregate.total_dirs,
            total_size: aggregate.total_size,
            latest_file_mtime_us: aggregate.latest_file_mtime_us,
            attested_count: aggregate.attested_count,
            blind_spot_count: aggregate.blind_spot_count,
        })
        .unwrap_or_default();
    if let Some(root) = tree.get(dir_path) {
        subtract_node_contribution(&mut stats, root);
    }
    stats
}

fn accumulate_subtree_stats(stats: &mut SubtreeStats, node: &FileMetaNode) {
    if node.is_tombstoned {
        return;
    }
    stats.total_nodes += 1;
    if node.is_dir {
        stats.total_dirs += 1;
    } else {
        stats.total_files += 1;
        stats.total_size += node.size;
        stats.latest_file_mtime_us = Some(
            stats
                .latest_file_mtime_us
                .map_or(node.modified_time_us, |current| {
                    current.max(node.modified_time_us)
                }),
        );
    }
    if node.monitoring_attested {
        stats.attested_count += 1;
    }
    if node.blind_spot {
        stats.blind_spot_count += 1;
    }
}

fn subtract_node_contribution(stats: &mut SubtreeStats, node: &FileMetaNode) {
    if node.is_tombstoned {
        return;
    }
    stats.total_nodes = stats.total_nodes.saturating_sub(1);
    if node.is_dir {
        stats.total_dirs = stats.total_dirs.saturating_sub(1);
    } else {
        stats.total_files = stats.total_files.saturating_sub(1);
        stats.total_size = stats.total_size.saturating_sub(node.size);
        if stats.latest_file_mtime_us == Some(node.modified_time_us) {
            stats.latest_file_mtime_us = None;
        }
    }
    if node.monitoring_attested {
        stats.attested_count = stats.attested_count.saturating_sub(1);
    }
    if node.blind_spot {
        stats.blind_spot_count = stats.blind_spot_count.saturating_sub(1);
    }
}

pub fn get_subtree_stats_for_query(tree: &MaterializedTree, dir_path: &[u8]) -> SubtreeStats {
    if tree.has_path(dir_path) || dir_path.is_empty() || dir_path == b"/" {
        return get_subtree_stats(tree, dir_path);
    }
    let rebased = collect_rebased_nodes(tree, dir_path);
    let mut stats = SubtreeStats::default();
    for mapped in rebased {
        accumulate_subtree_stats(&mut stats, mapped.node);
    }
    stats
}

/// Health statistics for the entire sink.
pub fn get_health_stats(tree: &MaterializedTree, clock: &SinkClock) -> HealthStats {
    let mut stats = HealthStats::default();
    let subtree = get_subtree_stats(tree, b"/");
    stats.live_nodes = subtree.total_nodes;
    stats.attested_count = subtree.attested_count;
    stats.blind_spot_count = subtree.blind_spot_count;

    for (_path, node) in tree.iter() {
        if node.is_tombstoned {
            stats.tombstoned_count += 1;
            continue;
        }
        if node.is_currently_suspect() {
            stats.suspect_count += 1;
        }
    }

    stats.shadow_time_us = clock.now_us();
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SyncTrack;

    fn insert_node(tree: &mut MaterializedTree, path: &[u8], is_dir: bool, size: u64, mtime: u64) {
        tree.insert(
            path.to_vec(),
            FileMetaNode {
                size,
                modified_time_us: mtime,
                created_time_us: mtime,
                is_dir,
                source: SyncTrack::Scan,
                monitoring_attested: true,
                last_confirmed_at: None,
                suspect_until: None,
                blind_spot: false,
                is_tombstoned: false,
                tombstone_expires_at: None,
                last_seen_epoch: 1,
                subtree_last_write_significant_change_at: None,
            },
        );
    }

    fn build_tree_for_query_tests() -> MaterializedTree {
        let mut tree = MaterializedTree::new();
        insert_node(&mut tree, b"/data", true, 0, 1_000_000);
        insert_node(&mut tree, b"/data/report.csv", false, 100, 2_000_000);
        insert_node(&mut tree, b"/data/archive", true, 0, 1_500_000);
        insert_node(&mut tree, b"/data/archive/old.log", false, 50, 1_500_000);
        tree
    }

    fn build_prefixed_tree_for_query_tests() -> MaterializedTree {
        let mut tree = MaterializedTree::new();
        insert_node(
            &mut tree,
            b"/tmp/capanix/data/nfs1/qf-e2e-job/nested",
            true,
            0,
            1_000_000,
        );
        insert_node(
            &mut tree,
            b"/tmp/capanix/data/nfs1/qf-e2e-job/nested/child",
            true,
            0,
            1_100_000,
        );
        tree
    }

    fn build_prefixed_tree_with_structural_placeholder_root() -> MaterializedTree {
        let mut tree = MaterializedTree::new();
        insert_node(
            &mut tree,
            b"/tmp/capanix/data/nfs1/qf-e2e-job/nested/peer.txt",
            false,
            7,
            1_200_000,
        );
        tree
    }

    fn build_tree_with_structural_placeholder_root() -> MaterializedTree {
        let mut tree = MaterializedTree::new();
        insert_node(&mut tree, b"/nested/peer.txt", false, 7, 1_200_000);
        tree
    }

    #[test]
    fn empty_tree_is_reliable_on_cold_start() {
        let tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/", &clock, false, true, None);
        assert!(resp.reliable);
        assert_eq!(resp.unreliable_reason, None);
        assert!(resp.nodes.is_empty());
    }

    #[test]
    fn overflow_marker_makes_empty_tree_unreliable() {
        let tree = MaterializedTree::new();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/", &clock, true, true, None);
        assert!(!resp.reliable);
        assert_eq!(
            resp.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );
        assert!(resp.nodes.is_empty());
    }

    #[test]
    fn rebased_directory_tree_matches_root_relative_query_path() {
        let tree = build_prefixed_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_materialized_tree_payload(
            &tree,
            b"/nested",
            &clock,
            false,
            true,
            None,
            ReadClass::Materialized,
            None,
        );
        let mut paths = Vec::new();
        if resp.root.exists {
            paths.push(String::from_utf8_lossy(&resp.root.path).to_string());
        }
        paths.extend(
            resp.entries
                .iter()
                .map(|node| String::from_utf8_lossy(&node.path).to_string()),
        );
        paths.sort();
        assert_eq!(
            paths,
            vec!["/nested".to_string(), "/nested/child".to_string()]
        );
    }

    #[test]
    fn rebased_directory_tree_synthesizes_existing_root_for_structural_placeholder_with_live_descendants(
    ) {
        let tree = build_prefixed_tree_with_structural_placeholder_root();
        let clock = SinkClock::new();

        let resp = get_materialized_tree_payload(
            &tree,
            b"/nested",
            &clock,
            false,
            true,
            Some(1),
            ReadClass::Materialized,
            None,
        );

        assert!(
            resp.root.exists,
            "rebased query root must stay logically present when a structural placeholder has live descendants: {:?}",
            resp.root
        );
        assert_eq!(resp.root.path, b"/nested".to_vec());
        assert!(resp.root.is_dir);
        assert!(
            resp.entries
                .iter()
                .any(|entry| entry.path == b"/nested/peer.txt".to_vec()),
            "rebased query must still expose live descendants when the query root only exists as a structural placeholder: {:?}",
            resp.entries
        );
    }

    #[test]
    fn direct_directory_tree_synthesizes_existing_root_for_structural_placeholder_with_live_descendants(
    ) {
        let tree = build_tree_with_structural_placeholder_root();
        let clock = SinkClock::new();

        let resp = get_materialized_tree_payload(
            &tree,
            b"/nested",
            &clock,
            false,
            true,
            Some(1),
            ReadClass::Materialized,
            None,
        );

        assert!(
            resp.root.exists,
            "direct query root must stay logically present when a structural placeholder has live descendants: {:?}",
            resp.root
        );
        assert_eq!(resp.root.path, b"/nested".to_vec());
        assert!(resp.root.is_dir);
        assert!(
            resp.entries
                .iter()
                .any(|entry| entry.path == b"/nested/peer.txt".to_vec()),
            "direct query must still expose live descendants when the query root only exists as a structural placeholder: {:?}",
            resp.entries
        );
    }

    #[test]
    fn rebased_subtree_stats_match_root_relative_query_path() {
        let tree = build_prefixed_tree_for_query_tests();
        let stats = get_subtree_stats_for_query(&tree, b"/nested");
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.total_dirs, 2);
    }

    #[test]
    fn recursive_query_includes_root_and_descendants() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/data", &clock, false, true, None);
        let paths: Vec<Vec<u8>> = resp.nodes.iter().map(|n| n.path.clone()).collect();

        assert!(resp.reliable);
        assert_eq!(paths.len(), 4);
        assert!(paths.contains(&b"/data".to_vec()));
        assert!(paths.contains(&b"/data/report.csv".to_vec()));
        assert!(paths.contains(&b"/data/archive".to_vec()));
        assert!(paths.contains(&b"/data/archive/old.log".to_vec()));
    }

    #[test]
    fn subdirectory_query_includes_requested_root_and_descendants() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/data/archive", &clock, false, true, None);
        let paths: Vec<Vec<u8>> = resp.nodes.iter().map(|n| n.path.clone()).collect();

        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&b"/data/archive".to_vec()));
        assert!(paths.contains(&b"/data/archive/old.log".to_vec()));
        assert!(!paths.contains(&b"/data".to_vec()));
    }

    #[test]
    fn recursive_query_with_max_depth_zero_returns_only_root() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/data", &clock, false, true, Some(0));
        let paths: Vec<Vec<u8>> = resp.nodes.iter().map(|n| n.path.clone()).collect();

        assert_eq!(paths, vec![b"/data".to_vec()]);
    }

    #[test]
    fn non_recursive_query_limits_to_direct_children() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/data", &clock, false, false, None);
        let paths: Vec<Vec<u8>> = resp.nodes.iter().map(|n| n.path.clone()).collect();

        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&b"/data".to_vec()));
        assert!(paths.contains(&b"/data/report.csv".to_vec()));
        assert!(paths.contains(&b"/data/archive".to_vec()));
        assert!(!paths.contains(&b"/data/archive/old.log".to_vec()));
    }

    #[test]
    fn recursive_query_respects_max_depth() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/data", &clock, false, true, Some(1));
        let paths: Vec<Vec<u8>> = resp.nodes.iter().map(|n| n.path.clone()).collect();

        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&b"/data".to_vec()));
        assert!(paths.contains(&b"/data/report.csv".to_vec()));
        assert!(paths.contains(&b"/data/archive".to_vec()));
        assert!(!paths.contains(&b"/data/archive/old.log".to_vec()));
    }

    #[test]
    fn missing_path_query_returns_empty_reliable_response() {
        let tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        let resp = get_directory_tree(&tree, b"/does-not-exist", &clock, false, true, None);

        assert!(resp.reliable);
        assert_eq!(resp.unreliable_reason, None);
        assert!(resp.nodes.is_empty());
    }

    #[test]
    fn query_response_exposes_integrity_flags_on_nodes() {
        let mut tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        tree.with_node_mut(b"/data/report.csv", |node| {
            node.monitoring_attested = false;
            node.blind_spot = true;
            node.suspect_until =
                Some(std::time::Instant::now() + std::time::Duration::from_secs(1));
        })
        .expect("report node should exist");

        let resp = get_directory_tree(&tree, b"/data", &clock, false, true, None);
        let report = resp
            .nodes
            .iter()
            .find(|n| n.path == b"/data/report.csv".to_vec())
            .expect("report query node");

        assert!(!report.monitoring_attested);
        assert!(report.is_blind_spot);
        assert!(report.is_suspect);
        assert!(!resp.reliable);
        assert_eq!(
            resp.unreliable_reason,
            Some(UnreliableReason::BlindSpotsDetected)
        );
    }

    #[test]
    fn query_reliability_prefers_blind_spot_over_suspect_and_unattested() {
        let mut tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        tree.with_node_mut(b"/data", |node| {
            node.monitoring_attested = false;
        })
        .expect("root node");
        tree.with_node_mut(b"/data/archive/old.log", |node| {
            node.suspect_until =
                Some(std::time::Instant::now() + std::time::Duration::from_secs(1));
        })
        .expect("old log node");
        tree.with_node_mut(b"/data/report.csv", |blind| {
            blind.monitoring_attested = false;
            blind.blind_spot = true;
        })
        .expect("report node should exist");

        let resp = get_directory_tree(&tree, b"/data", &clock, false, true, None);
        assert!(!resp.reliable);
        assert_eq!(
            resp.unreliable_reason,
            Some(UnreliableReason::BlindSpotsDetected)
        );
    }

    #[test]
    fn overflow_marker_overrides_node_level_reliability_reasons() {
        let mut tree = build_tree_for_query_tests();
        let clock = SinkClock::new();

        tree.with_node_mut(b"/data/report.csv", |blind| {
            blind.monitoring_attested = false;
            blind.blind_spot = true;
        })
        .expect("report node should exist");

        let resp = get_directory_tree(&tree, b"/data", &clock, true, true, None);
        assert!(!resp.reliable);
        assert_eq!(
            resp.unreliable_reason,
            Some(UnreliableReason::WatchOverflowPendingAudit)
        );
    }

    #[test]
    fn subtree_stats_counts_dirs_files_size_and_latest_mtime() {
        let tree = build_tree_for_query_tests();
        let stats = get_subtree_stats(&tree, b"/data");

        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.total_dirs, 1);
        assert_eq!(stats.total_files, 2);
        assert_eq!(stats.total_size, 150);
        assert_eq!(stats.latest_file_mtime_us, Some(2_000_000));
        assert_eq!(stats.attested_count, 3);
        assert_eq!(stats.blind_spot_count, 0);
    }

    #[test]
    fn subtree_stats_skips_tombstoned_nodes() {
        let mut tree = build_tree_for_query_tests();
        tree.with_node_mut(b"/data/report.csv", |tombstoned| {
            tombstoned.is_tombstoned = true;
        })
        .expect("report node should exist");

        let stats = get_subtree_stats(&tree, b"/data");
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.total_files, 1);
        assert_eq!(stats.total_size, 50);
        assert_eq!(stats.latest_file_mtime_us, Some(1_500_000));
    }

    #[test]
    fn health_stats_counts_suspect_and_tombstoned_nodes() {
        let mut tree = build_tree_for_query_tests();
        {
            tree.with_node_mut(b"/data/archive/old.log", |suspect| {
                suspect.suspect_until =
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(1));
            })
            .expect("old.log node should exist");
        }
        {
            tree.with_node_mut(b"/data/report.csv", |tombstoned| {
                tombstoned.is_tombstoned = true;
            })
            .expect("report node should exist");
        }
        let clock = SinkClock::new();

        let stats = get_health_stats(&tree, &clock);
        assert_eq!(stats.live_nodes, 3);
        assert_eq!(stats.tombstoned_count, 1);
        assert_eq!(stats.suspect_count, 1);
        assert_eq!(stats.attested_count, 3);
        assert_eq!(stats.blind_spot_count, 0);
    }
}
