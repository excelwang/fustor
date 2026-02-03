import os
import logging
from typing import Dict, List, Optional, Any
from .state import FSState

logger = logging.getLogger(__name__)

class FSViewQuery:
    """
    Handles read-only queries against the FS State.
    """
    def __init__(self, state: FSState):
        self.state = state

    def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        node = self.state.directory_path_map.get(path) or self.state.file_path_map.get(path)
        if node:
            return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
        return None
    
    def get_blind_spot_list(self) -> Dict[str, Any]:
        additions = []
        for path in self.state.blind_spot_additions:
            node = self.state.file_path_map.get(path)
            if node:
                additions.append(node.to_dict())
        
        return {
            "additions_count": len(additions),
            "additions": additions,
            "deletion_count": len(self.state.blind_spot_deletions),
            "deletions": list(self.state.blind_spot_deletions)
        }

    def search_files(self, query: str) -> List[Dict[str, Any]]:
        results = []
        q = query.lower()
        for node in self.state.file_path_map.values():
            if q in node.name.lower():
                results.append(node.to_dict())
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Collects metrics for health reporting."""
        oldest_dir = None
        if self.state.directory_path_map:
            dirs = [d for d in self.state.directory_path_map.values() if d.path != "/"]
            if dirs:
                oldest_node = min(dirs, key=lambda x: x.modified_time)
                oldest_dir = {"path": oldest_node.path, "timestamp": oldest_node.modified_time}

        logical_now = self.state.logical_clock.get_watermark()
        staleness = 0.0
        if oldest_dir:
            staleness = max(0.0, logical_now - oldest_dir["timestamp"])

        suspect_count = sum(1 for node in self.state.file_path_map.values() if node.integrity_suspect)

        return {
             "item_count": len(self.state.file_path_map) + len(self.state.directory_path_map),
             "total_files": len(self.state.file_path_map),
             "total_directories": len(self.state.directory_path_map),
             "latency_ms": self.state.last_event_latency,
             "staleness_seconds": staleness,
             "oldest_item_path": oldest_dir["path"] if oldest_dir else None,
             "has_blind_spot": (len(self.state.blind_spot_additions) + len(self.state.blind_spot_deletions)) > 0,
             "suspect_file_count": suspect_count,
             "logical_now": logical_now,
             "last_audit_finished_at": self.state.last_audit_finished_at,
             "audit_cycle_count": self.state.audit_cycle_count
        }
