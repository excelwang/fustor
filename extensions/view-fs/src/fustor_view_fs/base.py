import logging
import asyncio
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import defaultdict
import os
from contextlib import asynccontextmanager

from fustor_core.drivers import ViewDriver
from fustor_core.clock import LogicalClock
from .nodes import DirectoryNode, FileNode
from .rwlock import AsyncRWLock

logger = logging.getLogger(__name__)

class FSViewBase(ViewDriver):
    """
    Base class for FS View Drivers, inheriting from the core ViewDriver ABC.
    Provides shared state and concurrency primitives.
    
    Concurrency Model (asyncio single-threaded):
    - Concurrent operations (queries, process_event) use read_lock. 
      process_event mutates state but is safe under asyncio's cooperative scheduling
      since mutations complete atomically between await points.
    - Exclusive operations (audit_start/end, reset, session_start) use write_lock.
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
        
        # Concurrency management: AsyncRWLock replaces Semaphore×1000
        self._rwlock = AsyncRWLock()
        self._segment_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _get_segment_lock(self, path: str) -> asyncio.Lock:
        parts = path.strip('/').split('/')
        segment = parts[0] if parts and parts[0] else "/"
        return self._segment_locks[segment]

    def _check_cache_invalidation(self, path: str):
        pass

    @asynccontextmanager
    async def _global_exclusive_lock(self):
        """Acquire exclusive write lock (blocks all readers)."""
        async with self._rwlock.write_lock():
            yield

    @asynccontextmanager
    async def _global_read_lock(self):
        """Acquire shared read lock (concurrent with other readers)."""
        async with self._rwlock.read_lock():
            yield
