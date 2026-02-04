import logging
import asyncio
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import os
from contextlib import asynccontextmanager

from fustor_core.pipeline.handler import ViewHandler
from fustor_core.clock import LogicalClock
from .nodes import DirectoryNode, FileNode

logger = logging.getLogger(__name__)

class FSViewBase(ViewHandler):
    """
    Base class for FS View Providers, inheriting from the core ViewHandler ABC.
    Provides shared state and concurrency primitives.
    """
    
    # Schema identifier
    schema_name = "fs"
    schema_version = "2.0"
    
    def __init__(self, id: str, config: Dict[str, Any]):
        super().__init__(id, config)
        
        # Legacy config support
        self.view_id = config.get("view_id", id)
        
        threshold = config.get("hot_file_threshold", 30.0)
        self.hot_file_threshold = float(threshold)
        
        self.logger = logging.getLogger(f"fustor_view.fs.{self.view_id}")
        
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
