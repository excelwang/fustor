use std::collections::{BTreeMap, HashMap};

use capanix_app_sdk::{CnxError, Event, Result};
use capanix_host_fs_types::EventKind;
use capanix_host_fs_types::FileMetaRecord;
use capanix_host_fs_types::query::UnreliableReason;

use crate::query::models::{QueryNode, SubtreeStats};
use crate::query::path::{normalized_path_for_query, parent_path, relative_depth_under_root};
use crate::query::reliability::ReliabilityAccumulator;
use crate::query::tree::{TreeGroupPayload, TreePageEntry, TreePageRoot, TreeStability};

#[derive(Debug, Clone)]
pub(crate) struct RawQueryResult {
    pub nodes: Vec<QueryNode>,
    pub reliable: bool,
    pub unreliable_reason: Option<UnreliableReason>,
}

pub(crate) fn query_response_from_records(
    records: HashMap<Vec<u8>, FileMetaRecord>,
) -> RawQueryResult {
    let mut nodes: Vec<QueryNode> = records
        .into_values()
        .filter(|record| !matches!(record.event_kind, capanix_host_fs_types::EventKind::Delete))
        .map(|record| QueryNode {
            path: record.path,
            file_name: record.file_name,
            size: record.unix_stat.size,
            modified_time_us: record.unix_stat.mtime_us,
            is_dir: record.unix_stat.is_dir,
            monitoring_attested: true,
            is_suspect: false,
            is_blind_spot: false,
        })
        .collect();
    nodes.sort_by(|a, b| a.path.len().cmp(&b.path.len()).then(a.path.cmp(&b.path)));
    RawQueryResult {
        nodes,
        reliable: true,
        unreliable_reason: None,
    }
}

pub(crate) fn raw_query_results_by_origin_from_source_events(
    events: &[Event],
    query_path: &[u8],
) -> Result<BTreeMap<String, RawQueryResult>> {
    let mut latest_by_origin_path: BTreeMap<String, HashMap<Vec<u8>, FileMetaRecord>> =
        BTreeMap::new();

    for event in events {
        let origin = event.metadata().origin_id.0.clone();
        let record = match rmp_serde::from_slice::<FileMetaRecord>(event.payload_bytes()) {
            Ok(record) => record,
            Err(err) => {
                return Err(CnxError::InvalidInput(format!(
                    "invalid source event payload: {err}"
                )));
            }
        };

        let mut record = record;
        let Some(normalized_path) = normalized_path_for_query(&record.path, query_path) else {
            continue;
        };
        record.path = normalized_path;

        latest_by_origin_path
            .entry(origin)
            .or_default()
            .entry(record.path.clone())
            .and_modify(|existing| {
                let is_newer = record.unix_stat.mtime_us > existing.unix_stat.mtime_us;
                let delete_wins_on_tie = record.unix_stat.mtime_us == existing.unix_stat.mtime_us
                    && matches!(record.event_kind, EventKind::Delete)
                    && !matches!(existing.event_kind, EventKind::Delete);
                if is_newer || delete_wins_on_tie {
                    *existing = record.clone();
                }
            })
            .or_insert(record);
    }

    Ok(latest_by_origin_path
        .into_iter()
        .map(|(origin, records)| (origin, query_response_from_records(records)))
        .collect())
}

pub(crate) fn subtree_stats_from_query_response(response: &RawQueryResult) -> SubtreeStats {
    let mut stats = SubtreeStats::default();
    for node in &response.nodes {
        stats.total_nodes += 1;
        if node.is_dir {
            stats.total_dirs += 1;
        } else {
            stats.total_files += 1;
            stats.total_size += node.size;
            stats.latest_file_mtime_us = Some(
                stats
                    .latest_file_mtime_us
                    .map_or(node.modified_time_us, |v| v.max(node.modified_time_us)),
            );
        }
        if node.monitoring_attested {
            stats.attested_count += 1;
        }
        if node.is_blind_spot {
            stats.blind_spot_count += 1;
        }
    }
    stats
}

pub(crate) fn merge_query_responses(responses: Vec<RawQueryResult>) -> RawQueryResult {
    let mut by_path: HashMap<Vec<u8>, QueryNode> = HashMap::new();
    let mut reliability = ReliabilityAccumulator::new();

    for response in responses {
        if let Some(reason) = response.unreliable_reason {
            reliability.observe_reason(reason);
        }
        for node in response.nodes {
            by_path.insert(node.path.clone(), node);
        }
    }

    let mut nodes: Vec<_> = by_path.into_values().collect();
    nodes.sort_by(|a, b| a.path.len().cmp(&b.path.len()).then(a.path.cmp(&b.path)));
    let reliability = reliability.finish();

    RawQueryResult {
        nodes,
        reliable: reliability.reliable,
        unreliable_reason: reliability.unreliable_reason,
    }
}

pub(crate) fn tree_group_payload_from_query_response(
    response: &RawQueryResult,
    query_path: &[u8],
    recursive: bool,
    max_depth: Option<usize>,
) -> TreeGroupPayload {
    let mut sorted_nodes = response.nodes.iter().collect::<Vec<_>>();
    sorted_nodes.sort_by(|a, b| a.path.cmp(&b.path));

    let mut has_children_by_path = std::collections::HashSet::<Vec<u8>>::new();
    for node in &sorted_nodes {
        if let Some(parent) = parent_path(&node.path) {
            has_children_by_path.insert(parent);
        }
    }

    let root_node = sorted_nodes
        .iter()
        .find(|node| node.path.as_slice() == query_path)
        .copied();
    let root = if let Some(root_node) = root_node {
        TreePageRoot {
            path: root_node.path.clone(),
            size: root_node.size,
            modified_time_us: root_node.modified_time_us,
            is_dir: root_node.is_dir,
            exists: true,
            has_children: has_children_by_path.contains(root_node.path.as_slice()),
        }
    } else {
        TreePageRoot {
            path: query_path.to_vec(),
            size: 0,
            modified_time_us: 0,
            is_dir: true,
            exists: false,
            has_children: sorted_nodes
                .iter()
                .any(|node| node.path.as_slice() != query_path),
        }
    };

    let depth_limit = if recursive {
        max_depth.unwrap_or(usize::MAX)
    } else {
        1usize
    };
    let mut entries = Vec::<TreePageEntry>::new();
    for node in sorted_nodes {
        if node.path.as_slice() == query_path {
            continue;
        }
        let Some(depth) = relative_depth_under_root(&node.path, query_path) else {
            continue;
        };
        if depth == 0 || depth > depth_limit {
            continue;
        }
        entries.push(TreePageEntry {
            path: node.path.clone(),
            depth,
            size: node.size,
            modified_time_us: node.modified_time_us,
            is_dir: node.is_dir,
            has_children: has_children_by_path.contains(node.path.as_slice()),
        });
    }

    TreeGroupPayload {
        reliability: crate::query::reliability::GroupReliability {
            reliable: response.reliable,
            unreliable_reason: response.unreliable_reason.clone(),
        },
        stability: TreeStability::not_evaluated(),
        root,
        entries,
    }
}
