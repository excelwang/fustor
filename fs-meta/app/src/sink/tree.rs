//! Query-ready materialized tree.
//!
//! The live tree is stored as an arena keyed by exact path. Every path segment may
//! exist as a structural placeholder so subtree traversal and ancestor checks work
//! even when only descendants are materialized. Query-facing data stays close to
//! the stored node so `/tree`, `/stats`, and PIT creation can read it directly.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use crate::SyncTrack;
use crate::query::path::parent_path;

type NodeId = usize;
type SharedPath = Arc<[u8]>;

/// A single materialized metadata node.
#[derive(Debug, Clone)]
pub struct FileMetaNode {
    pub size: u64,
    pub modified_time_us: u64,
    pub created_time_us: u64,
    pub is_dir: bool,
    pub source: SyncTrack,

    // ── Integrity flags ──
    /// True if actively monitored by host watch backend (Realtime-confirmed).
    pub monitoring_attested: bool,
    /// Physical time of last Realtime confirmation. None = never confirmed.
    /// ONLY updated by Realtime events.
    pub last_confirmed_at: Option<Instant>,
    /// Monotonic expiry for suspect state. Node is suspect if Instant::now() < suspect_until.
    /// None = not suspect. Set on non-atomic writes, cleared on atomic writes.
    pub suspect_until: Option<Instant>,
    /// True if outside host watch coverage (detected by Scan).
    pub blind_spot: bool,
    /// True if this node is tombstoned (deleted).
    pub is_tombstoned: bool,
    /// Physical time when tombstone expires. After this, reincarnation from Scan is allowed.
    /// None = no tombstone or no expiry.
    pub tombstone_expires_at: Option<Instant>,
    /// Epoch in which this node was last seen by an audit scan.
    pub last_seen_epoch: u64,
    /// Monotonic instant when any descendant-or-self last changed in a write-significant way.
    pub subtree_last_write_significant_change_at: Option<Instant>,
}

impl FileMetaNode {
    /// Check if node is currently suspect (monotonic time evaluation).
    pub fn is_currently_suspect(&self) -> bool {
        match self.suspect_until {
            Some(expiry) => Instant::now() < expiry,
            None => false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirAggregate {
    pub total_nodes: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_size: u64,
    pub latest_file_mtime_us: Option<u64>,
    pub attested_count: u64,
    pub blind_spot_count: u64,
}

impl DirAggregate {
    fn merge(&mut self, other: &DirAggregate) {
        self.total_nodes += other.total_nodes;
        self.total_files += other.total_files;
        self.total_dirs += other.total_dirs;
        self.total_size += other.total_size;
        self.attested_count += other.attested_count;
        self.blind_spot_count += other.blind_spot_count;
        if let Some(latest) = other.latest_file_mtime_us {
            self.latest_file_mtime_us = Some(
                self.latest_file_mtime_us
                    .map_or(latest, |current| current.max(latest)),
            );
        }
    }

    fn from_node(node: &FileMetaNode) -> Self {
        if node.is_tombstoned {
            return Self::default();
        }
        let mut agg = Self {
            total_nodes: 1,
            attested_count: u64::from(node.monitoring_attested),
            blind_spot_count: u64::from(node.blind_spot),
            ..Self::default()
        };
        if node.is_dir {
            agg.total_dirs = 1;
        } else {
            agg.total_files = 1;
            agg.total_size = node.size;
            agg.latest_file_mtime_us = Some(node.modified_time_us);
        }
        agg
    }
}

#[derive(Debug, Clone)]
struct TreeNode {
    path: SharedPath,
    parent: Option<NodeId>,
    children: Vec<NodeId>,
    meta: Option<FileMetaNode>,
    aggregate: DirAggregate,
}

/// In-memory materialized tree backed by an exact-path index and arena nodes.
pub struct MaterializedTree {
    nodes: Vec<Option<TreeNode>>,
    path_index: HashMap<SharedPath, NodeId>,
    meta_count: usize,
}

struct TreeIter<'a> {
    tree: &'a MaterializedTree,
    stack: Vec<NodeId>,
}

struct ChildIter<'a> {
    tree: &'a MaterializedTree,
    child_ids: Vec<NodeId>,
    index: usize,
}

impl<'a> Iterator for TreeIter<'a> {
    type Item = (&'a [u8], &'a FileMetaNode);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(id) = self.stack.pop() {
            let node = self.tree.node(id);
            for child in node.children.iter().rev() {
                self.stack.push(*child);
            }
            if let Some(meta) = node.meta.as_ref() {
                return Some((node.path.as_ref(), meta));
            }
        }
        None
    }
}

impl<'a> Iterator for ChildIter<'a> {
    type Item = (&'a [u8], &'a FileMetaNode);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.child_ids.len() {
            let id = self.child_ids[self.index];
            self.index += 1;
            let node = self.tree.node(id);
            if let Some(meta) = node.meta.as_ref() {
                return Some((node.path.as_ref(), meta));
            }
        }
        None
    }
}

impl MaterializedTree {
    pub fn new() -> Self {
        let root_path: SharedPath = Arc::from(&b"/"[..]);
        Self {
            nodes: vec![Some(TreeNode {
                path: root_path.clone(),
                parent: None,
                children: Vec::new(),
                meta: None,
                aggregate: DirAggregate::default(),
            })],
            path_index: HashMap::from([(root_path, 0)]),
            meta_count: 0,
        }
    }

    pub fn get(&self, path: &[u8]) -> Option<&FileMetaNode> {
        let id = self.lookup_id(path)?;
        self.node(id).meta.as_ref()
    }

    pub fn has_path(&self, path: &[u8]) -> bool {
        self.lookup_id(path).is_some()
    }

    pub fn aggregate_at(&self, path: &[u8]) -> Option<DirAggregate> {
        self.lookup_id(path).map(|id| self.node(id).aggregate)
    }

    pub fn insert(&mut self, path: Vec<u8>, node: FileMetaNode) {
        let id = self.ensure_path_node(&path);
        let had_meta = self.node(id).meta.is_some();
        self.node_mut(id).meta = Some(node);
        if !had_meta {
            self.meta_count += 1;
        }
        self.refresh_aggregate_chain(id);
    }

    pub fn remove(&mut self, path: &[u8]) -> Option<FileMetaNode> {
        let id = self.lookup_id(path)?;
        let removed = self.node_mut(id).meta.take()?;
        self.meta_count = self.meta_count.saturating_sub(1);
        let refresh_from = if self.node(id).children.is_empty() {
            self.node(id).parent
        } else {
            Some(id)
        };
        self.prune_empty_chain(id);
        if let Some(start) = refresh_from {
            self.refresh_aggregate_chain(start);
        }
        Some(removed)
    }

    pub fn with_node_mut<R>(
        &mut self,
        path: &[u8],
        update: impl FnOnce(&mut FileMetaNode) -> R,
    ) -> Option<R> {
        let id = self.lookup_id(path)?;
        let result = {
            let meta = self.node_mut(id).meta.as_mut()?;
            update(meta)
        };
        self.refresh_aggregate_chain(id);
        Some(result)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.meta_count
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.meta_count == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &FileMetaNode)> {
        TreeIter {
            tree: self,
            stack: vec![0],
        }
    }

    pub fn node_count(&self) -> usize {
        self.meta_count
    }

    pub fn estimated_heap_bytes(&self) -> u64 {
        let node_bytes = self
            .nodes
            .iter()
            .filter_map(|node| node.as_ref())
            .map(|node| {
                let meta_bytes = node
                    .meta
                    .as_ref()
                    .map(|_| size_of::<FileMetaNode>())
                    .unwrap_or(0);
                (size_of::<TreeNode>()
                    + node.children.capacity() * size_of::<NodeId>()
                    + meta_bytes) as u64
            })
            .sum::<u64>();
        let index_bytes =
            self.path_index
                .len()
                .saturating_mul(size_of::<SharedPath>() + size_of::<NodeId>()) as u64;
        let shared_path_bytes = self
            .nodes
            .iter()
            .filter_map(|node| node.as_ref())
            .map(|node| node.path.len() as u64)
            .sum::<u64>();
        node_bytes + index_bytes + shared_path_bytes
    }

    #[cfg(test)]
    pub fn children_of<'a>(&'a self, dir_path: &[u8]) -> Vec<(&'a [u8], &'a FileMetaNode)> {
        self.child_iter(dir_path).collect()
    }

    pub fn descendants_of<'a>(&'a self, dir_path: &[u8]) -> Vec<(&'a [u8], &'a FileMetaNode)> {
        self.descendant_iter(dir_path).collect()
    }

    pub fn descendant_iter<'a>(
        &'a self,
        dir_path: &[u8],
    ) -> impl Iterator<Item = (&'a [u8], &'a FileMetaNode)> + 'a {
        let stack = self
            .lookup_id(dir_path)
            .map(|id| self.node(id).children.iter().rev().copied().collect())
            .unwrap_or_default();
        TreeIter { tree: self, stack }
    }

    pub fn child_iter<'a>(
        &'a self,
        dir_path: &[u8],
    ) -> impl Iterator<Item = (&'a [u8], &'a FileMetaNode)> + 'a {
        let child_ids = self
            .lookup_id(dir_path)
            .map(|id| self.node(id).children.clone())
            .unwrap_or_default();
        ChildIter {
            tree: self,
            child_ids,
            index: 0,
        }
    }

    pub fn has_live_child(&self, dir_path: &[u8]) -> bool {
        self.child_iter(dir_path)
            .any(|(_, node)| !node.is_tombstoned)
    }

    pub fn has_tombstoned_ancestor(&self, path: &[u8]) -> bool {
        self.any_ancestor_matches(path, |node| node.is_tombstoned)
    }

    pub fn purge_descendants(&mut self, dir_path: &[u8]) {
        let Some(id) = self.lookup_id(dir_path) else {
            return;
        };
        let children = self.node(id).children.clone();
        for child in children {
            self.remove_subtree(child);
        }
        self.node_mut(id).children.clear();
        self.refresh_aggregate_chain(id);
    }

    pub fn mark_write_significant_change(&mut self, path: &[u8], at: Instant) {
        if let Some(id) = self.lookup_id(path)
            && let Some(node) = self.node_mut(id).meta.as_mut()
        {
            node.subtree_last_write_significant_change_at = Some(at);
            let mut current = self.node(id).parent;
            while let Some(ancestor) = current {
                let parent = self.node(ancestor).parent;
                if let Some(parent_node) = self.node_mut(ancestor).meta.as_mut() {
                    parent_node.subtree_last_write_significant_change_at = Some(at);
                }
                current = parent;
            }
        }
    }

    fn any_ancestor_matches<F>(&self, path: &[u8], mut predicate: F) -> bool
    where
        F: FnMut(&FileMetaNode) -> bool,
    {
        if path == b"/" {
            return false;
        }
        let mut current = path;
        loop {
            let Some(pos) = rfind_slash(current) else {
                return false;
            };
            current = if pos == 0 { b"/" } else { &current[..pos] };
            if let Some(node) = self.get(current)
                && predicate(node)
            {
                return true;
            }
            if current == b"/" {
                return false;
            }
        }
    }

    fn lookup_id(&self, path: &[u8]) -> Option<NodeId> {
        self.path_index.get(path).copied()
    }

    fn node(&self, id: NodeId) -> &TreeNode {
        self.nodes[id]
            .as_ref()
            .expect("materialized tree node should exist")
    }

    fn node_mut(&mut self, id: NodeId) -> &mut TreeNode {
        self.nodes[id]
            .as_mut()
            .expect("materialized tree node should exist")
    }

    fn ensure_path_node(&mut self, path: &[u8]) -> NodeId {
        if let Some(id) = self.lookup_id(path) {
            return id;
        }
        let parent_path = parent_path(path).unwrap_or_else(|| b"/".to_vec());
        let parent_id = if path == b"/" {
            0
        } else {
            self.ensure_path_node(&parent_path)
        };
        let id = self.nodes.len();
        let shared_path: SharedPath = Arc::from(path);
        self.nodes.push(Some(TreeNode {
            path: shared_path.clone(),
            parent: Some(parent_id),
            children: Vec::new(),
            meta: None,
            aggregate: DirAggregate::default(),
        }));
        self.path_index.insert(shared_path, id);
        self.insert_child_sorted(parent_id, id);
        id
    }

    fn insert_child_sorted(&mut self, parent_id: NodeId, child_id: NodeId) {
        let child_path = self.node(child_id).path.clone();
        let insert_at = {
            let children = &self.node(parent_id).children;
            children
                .binary_search_by(|existing| {
                    self.node(*existing).path.as_ref().cmp(child_path.as_ref())
                })
                .unwrap_or_else(|idx| idx)
        };
        self.node_mut(parent_id)
            .children
            .insert(insert_at, child_id);
    }

    fn remove_child_link(&mut self, parent_id: NodeId, child_id: NodeId) {
        self.node_mut(parent_id)
            .children
            .retain(|existing| *existing != child_id);
    }

    fn recompute_aggregate(&mut self, id: NodeId) {
        let mut aggregate = self
            .node(id)
            .meta
            .as_ref()
            .map(DirAggregate::from_node)
            .unwrap_or_default();
        let children = self.node(id).children.clone();
        for child in children {
            let child_agg = self.node(child).aggregate;
            aggregate.merge(&child_agg);
        }
        self.node_mut(id).aggregate = aggregate;
    }

    fn refresh_aggregate_chain(&mut self, start_id: NodeId) {
        let mut current = Some(start_id);
        while let Some(id) = current {
            let parent = self.node(id).parent;
            self.recompute_aggregate(id);
            current = parent;
        }
    }

    fn prune_empty_chain(&mut self, mut id: NodeId) {
        while id != 0 {
            let should_prune = {
                let node = self.node(id);
                node.meta.is_none() && node.children.is_empty()
            };
            if !should_prune {
                break;
            }
            let parent_id = self
                .node(id)
                .parent
                .expect("non-root node should have parent");
            let path = self.node(id).path.clone();
            self.remove_child_link(parent_id, id);
            self.path_index.remove(path.as_ref());
            self.nodes[id] = None;
            self.recompute_aggregate(parent_id);
            id = parent_id;
        }
    }

    fn remove_subtree(&mut self, id: NodeId) {
        let children = self.node(id).children.clone();
        for child in children {
            self.remove_subtree(child);
        }
        let path = self.node(id).path.clone();
        if self.node(id).meta.is_some() {
            self.meta_count = self.meta_count.saturating_sub(1);
        }
        self.path_index.remove(path.as_ref());
        self.nodes[id] = None;
    }
}

impl Default for MaterializedTree {
    fn default() -> Self {
        Self::new()
    }
}

/// Find the last '/' in the path.
fn rfind_slash(path: &[u8]) -> Option<usize> {
    path.iter().rposition(|&b| b == b'/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(_path: &[u8], is_dir: bool) -> FileMetaNode {
        FileMetaNode {
            size: 0,
            modified_time_us: 1_000_000,
            created_time_us: 1_000_000,
            is_dir,
            source: SyncTrack::Scan,
            monitoring_attested: false,
            last_confirmed_at: None,
            suspect_until: None,
            blind_spot: false,
            is_tombstoned: false,
            tombstone_expires_at: None,
            last_seen_epoch: 0,
            subtree_last_write_significant_change_at: None,
        }
    }

    #[test]
    fn test_is_currently_suspect_reflects_expired_window() {
        let mut node = make_node(b"/hot.txt", false);
        node.suspect_until = Some(Instant::now() + std::time::Duration::from_secs(1));
        assert!(node.is_currently_suspect());

        node.suspect_until = Some(Instant::now() - std::time::Duration::from_secs(1));
        assert!(!node.is_currently_suspect());
    }

    #[test]
    fn children_and_descendants_keep_path_lex_order() {
        let mut tree = MaterializedTree::new();
        tree.insert(b"/a".to_vec(), make_node(b"/a", true));
        tree.insert(b"/a/b".to_vec(), make_node(b"/a/b", true));
        tree.insert(b"/a/b/c".to_vec(), make_node(b"/a/b/c", false));
        tree.insert(b"/a/d".to_vec(), make_node(b"/a/d", false));
        tree.insert(b"/e".to_vec(), make_node(b"/e", false));

        let children: Vec<_> = tree.children_of(b"/a");
        let child_paths: Vec<&[u8]> = children.iter().map(|(path, _)| *path).collect();
        assert_eq!(child_paths, vec![b"/a/b".as_slice(), b"/a/d".as_slice()]);

        let descendants: Vec<_> = tree.descendants_of(b"/a");
        let desc_paths: Vec<&[u8]> = descendants.iter().map(|(path, _)| *path).collect();
        assert_eq!(
            desc_paths,
            vec![b"/a/b".as_slice(), b"/a/b/c".as_slice(), b"/a/d".as_slice()]
        );
    }

    #[test]
    fn tombstoned_ancestor_detection_uses_materialized_nodes_only() {
        let mut tree = MaterializedTree::new();
        let mut node = make_node(b"/foo", true);
        node.is_tombstoned = true;
        tree.insert(b"/foo".to_vec(), node);
        tree.insert(b"/foo/bar".to_vec(), make_node(b"/foo/bar", false));

        assert!(tree.has_tombstoned_ancestor(b"/foo/bar"));
        assert!(tree.has_tombstoned_ancestor(b"/foo/bar/baz"));
        assert!(!tree.has_tombstoned_ancestor(b"/other"));
    }

    #[test]
    fn purge_descendants_preserves_directory_node_and_updates_aggregate() {
        let mut tree = MaterializedTree::new();
        tree.insert(b"/a".to_vec(), make_node(b"/a", true));
        tree.insert(b"/a/b".to_vec(), make_node(b"/a/b", true));
        tree.insert(b"/a/b/c".to_vec(), make_node(b"/a/b/c", false));
        tree.insert(b"/a/d".to_vec(), make_node(b"/a/d", false));

        tree.purge_descendants(b"/a");

        assert!(tree.get(b"/a").is_some());
        assert!(tree.get(b"/a/b").is_none());
        assert!(tree.get(b"/a/b/c").is_none());
        assert!(tree.get(b"/a/d").is_none());
        let agg = tree.aggregate_at(b"/a").expect("aggregate for /a");
        assert_eq!(agg.total_nodes, 1);
        assert_eq!(agg.total_dirs, 1);
        assert_eq!(agg.total_files, 0);
    }

    #[test]
    fn aggregate_tracks_subtree_counts_and_file_mtime() {
        let mut tree = MaterializedTree::new();
        let mut dir = make_node(b"/data", true);
        dir.monitoring_attested = true;
        tree.insert(b"/data".to_vec(), dir);
        let mut file_a = make_node(b"/data/a.txt", false);
        file_a.size = 10;
        file_a.modified_time_us = 100;
        file_a.monitoring_attested = true;
        tree.insert(b"/data/a.txt".to_vec(), file_a);
        let mut file_b = make_node(b"/data/b.txt", false);
        file_b.size = 20;
        file_b.modified_time_us = 200;
        file_b.blind_spot = true;
        tree.insert(b"/data/b.txt".to_vec(), file_b);

        let agg = tree.aggregate_at(b"/data").expect("aggregate for /data");
        assert_eq!(agg.total_nodes, 3);
        assert_eq!(agg.total_dirs, 1);
        assert_eq!(agg.total_files, 2);
        assert_eq!(agg.total_size, 30);
        assert_eq!(agg.latest_file_mtime_us, Some(200));
        assert_eq!(agg.attested_count, 2);
        assert_eq!(agg.blind_spot_count, 1);
    }

    #[test]
    fn with_node_mut_refreshes_aggregate_after_tombstone() {
        let mut tree = MaterializedTree::new();
        tree.insert(b"/data".to_vec(), make_node(b"/data", true));
        let mut file = make_node(b"/data/a.txt", false);
        file.size = 10;
        tree.insert(b"/data/a.txt".to_vec(), file);

        tree.with_node_mut(b"/data/a.txt", |node| {
            node.is_tombstoned = true;
        });

        let agg = tree.aggregate_at(b"/data").expect("aggregate for /data");
        assert_eq!(agg.total_nodes, 1);
        assert_eq!(agg.total_files, 0);
        assert_eq!(agg.total_size, 0);
    }
}
