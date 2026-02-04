import logging
import asyncio
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import os
from contextlib import asynccontextmanager

from fustor_core.drivers import ViewDriver
from fustor_core.clock import LogicalClock
from .nodes import DirectoryNode, FileNode

logger = logging.getLogger(__name__)

class FSViewBase(ViewDriver):
    """
    Base class for FS View Providers, inheriting from the core ViewDriver ABC.
    Provides shared state and concurrency primitives.
    """
    
    def __init__(self, id: str, view_id: str, config: Optional[Dict[str, Any]] = None, hot_file_threshold: float = 30.0):
        # Allow config to override argument
        final_config = config or {}
        # Support both keys, prefer item
        threshold = final_config.get("hot_file_threshold") or final_config.get("hot_file_threshold") or hot_file_threshold
        
        # Ensure config has at least one valid key for upstream
        final_config.setdefault("hot_file_threshold", threshold)
        
        super().__init__(id, view_id, final_config)
        
        self.logger = logging.getLogger(f"fustor_view.fs.{view_id}")
        self.hot_file_threshold = float(threshold)
        
        # Concurrency management
        self._MAX_READERS = 1000
        self._global_semaphore = asyncio.Semaphore(self._MAX_READERS)
        self._segment_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _get_segment_lock(self, path: str) -> asyncio.Lock:
        parts = path.strip('/').split('/')
        segment = parts[0] if parts and parts[0] else "/"
        return self._segment_locks[segment]

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
