import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import os
from fustor_core.clock import LogicalClock
from .nodes import DirectoryNode, FileNode

logger = logging.getLogger(__name__)

class FSState:
    """
    Holds the in-memory state for a File System View.
    This replaces the implicit state shared via Mixins.
    """
    def __init__(self, view_id: str, config: Optional[Dict[str, Any]] = None):
        self.view_id = view_id
        self.config = config or {}
        self._root = DirectoryNode("", "/")
        self.directory_path_map: Dict[str, DirectoryNode] = {"/": self._root}
        self.file_path_map: Dict[str, FileNode] = {}
        
        # Consistency State
        # tombstone_list: Path -> (logical_timestamp, physical_timestamp)
        # - logical_timestamp: 用于转世判定 (mtime comparison)
        # - physical_timestamp: 用于 TTL 清理 (1 hour expiry)
        self.tombstone_list: Dict[str, Tuple[float, float]] = {}
        self.suspect_list: Dict[str, Tuple[float, float]] = {} # Path -> (expiry_monotonic, recorded_mtime)
        self.suspect_heap: List[Tuple[float, str]] = [] # (expiry_monotonic, path)
        
        self.last_audit_start: Optional[float] = None
        self.last_audit_finished_at: float = 0.0
        self.audit_cycle_count: int = 0
        self.audit_seen_paths: Set[str] = set()
        
        self.blind_spot_deletions: Set[str] = set()
        self.blind_spot_additions: Set[str] = set()
        
        self.logical_clock = LogicalClock()
        
        self.last_event_latency = 0.0
        self.last_suspect_cleanup_time = 0.0

    def get_node(self, path: str) -> Optional[Any]:
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        if path in self.directory_path_map:
            return self.directory_path_map[path]
        return self.file_path_map.get(path)

    def reset(self):
        """Reset state to initial empty tree."""
        self._root = DirectoryNode("", "/")
        self.directory_path_map = {"/": self._root}
        self.file_path_map = {}
        self.tombstone_list.clear()
        self.suspect_list.clear()
        self.suspect_heap.clear()
        self.last_audit_start = None
        self.last_audit_finished_at = 0.0
        self.audit_cycle_count = 0
        self.audit_seen_paths.clear()
        self.blind_spot_deletions.clear()
        self.blind_spot_additions.clear()
        self.logical_clock.reset(0.0)
        self.last_event_latency = 0.0
