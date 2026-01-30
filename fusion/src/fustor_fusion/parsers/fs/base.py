import logging
import asyncio
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import os
from contextlib import asynccontextmanager

from fustor_common.logical_clock import LogicalClock
from ...config import fusion_config
from .nodes import DirectoryNode, FileNode

logger = logging.getLogger(__name__)

class ParserBase:
    """Base class providing shared state and concurrency primitives for the FS parser."""
    def __init__(self, datastore_id: int):
        self.datastore_id = datastore_id
        self.logger = logging.getLogger(f"fustor_fusion.parser.fs.{datastore_id}")
        self.hot_file_threshold = fusion_config.FUSTOR_FUSION_SUSPECT_TTL_SECONDS
        self._root = DirectoryNode("", "/")
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self._root}
        self._file_path_map: Dict[str, FileNode] = {}
        
        # Concurrency management
        self._MAX_READERS = 1000
        self._global_semaphore = asyncio.Semaphore(self._MAX_READERS)
        self._segment_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        
        self._last_event_latency = 0.0
        
        # Consistency State
        self._tombstone_list: Dict[str, float] = {}
        self._suspect_list: Dict[str, Tuple[float, float]] = {}
        self._last_audit_start: Optional[float] = None
        self._audit_seen_paths: Set[str] = set()
        self._last_suspect_cleanup_time = 0.0
        self._suspect_cleanup_interval = 0.5 
        self._blind_spot_deletions: Set[str] = set()
        self._blind_spot_additions: Set[str] = set()
        self._current_session_id: Optional[str] = None
        self._logical_clock = LogicalClock()
        self._suspect_heap: List[Tuple[float, str]] = []

    def _get_segment_lock(self, path: str) -> asyncio.Lock:
        parts = path.strip('/').split('/')
        segment = parts[0] if parts and parts[0] else "/"
        return self._segment_locks[segment]

    def _get_node(self, path: str) -> Optional[Any]:
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        if path in self._directory_path_map:
            return self._directory_path_map[path]
        return self._file_path_map.get(path)

    def _check_cache_invalidation(self, path: str):
        pass

    @asynccontextmanager
    async def _global_exclusive_lock(self):
        """Context manager to acquire the global semaphore exclusively."""
        for _ in range(self._MAX_READERS):
            await self._global_semaphore.acquire()
        try:
            yield
        finally:
            for _ in range(self._MAX_READERS):
                self._global_semaphore.release()
