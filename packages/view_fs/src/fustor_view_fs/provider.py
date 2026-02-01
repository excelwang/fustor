from typing import Dict, Any, Optional, List
import asyncio
import heapq
from .base import FSViewBase
from .state import FSState
from .tree import TreeManager
from .arbitrator import FSArbitrator
from .audit import AuditManager
from .query import FSViewQuery

class FSViewProvider(FSViewBase):
    """
    Consistent File System View Provider.
    Refactored with Composition over Inheritance.
    Coordinates various components to maintain a fused, consistent view 
    of the FS using Smart Merge arbitration logic.
    """
    def __init__(self, view_id: str, datastore_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(view_id, datastore_id, config)
        
        # Composition Root
        self.state = FSState(datastore_id)
        self.tree_manager = TreeManager(self.state)
        self.arbitrator = FSArbitrator(
            self.state, 
            self.tree_manager, 
            hot_file_threshold=self.hot_file_threshold
        )
        self.audit_manager = AuditManager(self.state, self.tree_manager)
        self.query = FSViewQuery(self.state)

    async def process_event(self, event: Any) -> bool:
        """Entry point for all events (Realtime, Snapshot, Audit)."""
        async with self._global_semaphore:
            return await self.arbitrator.process_event(event)

    async def handle_audit_start(self):
        """Called when an Audit cycle begins."""
        async with self._global_exclusive_lock():
            await self.audit_manager.handle_start()

    async def handle_audit_end(self):
        """Called when an Audit cycle ends."""
        async with self._global_exclusive_lock():
            await self.audit_manager.handle_end()

    async def cleanup_expired_suspects(self):
        """Periodic background task to clear 'hot' status from stable files."""
        async with self._global_semaphore:
            return self.arbitrator.cleanup_expired_suspects()

    async def on_session_start(self):
        """Handles new session lifecycle."""
        async with self._global_exclusive_lock():
            # If we were in an audit, it's now invalid
            self.state.last_audit_start = None
            self.state.audit_seen_paths.clear()
            
            # Per CONSISTENCY_DESIGN.md §4.4: Clear blind-spot lists on new session
            # Blind spots may be rediscovered by the new session
            self.state.blind_spot_additions.clear()
            self.state.blind_spot_deletions.clear()
            
            self.logger.info(f"New session sequence started. Cleared audit buffer and blind-spot lists.")

    async def on_session_close(self):
        """Handles session closure."""
        # Generic FS provider doesn't reset on session close usually,
        # unless configured as 'live' only.
        pass

    async def reset(self):
        """Full reset of the in-memory view."""
        async with self._global_exclusive_lock():
            # 1. Clear view-specific in-memory state
            self.state.reset()
            
            self.logger.info(f"FS View state for datastore {self.datastore_id} has been reset. Global sessions and ingestion state remain unaffected.")

    # --- Query Delegation ---

    async def get_directory_tree(self, path: str = "/", **kwargs) -> Optional[Dict[str, Any]]:
        async with self._global_semaphore:
            return self.query.get_directory_tree(path=path, **kwargs)

    async def get_blind_spot_list(self) -> Dict[str, Any]:
        async with self._global_semaphore:
            return self.query.get_blind_spot_list()

    async def get_suspect_list(self) -> Dict[str, float]:
        async with self._global_semaphore:
             suspects = {path: expires_at for path, (expires_at, _) in self.state.suspect_list.items()}
             return suspects

    async def search_files(self, query: str) -> List[Dict[str, Any]]:
        async with self._global_semaphore:
            return self.query.search_files(query)

    async def get_directory_stats(self) -> Dict[str, Any]:
        async with self._global_semaphore:
            return self.query.get_stats()

    async def update_suspect(self, path: str, mtime: float):
        """Update suspect status from sentinel feedback."""
        async with self._global_semaphore:
            self.state.logical_clock.update(mtime)
            node = self.state.get_node(path)
            if not node: return

            old_mtime = node.modified_time
            node.modified_time = mtime
            watermark = self.state.logical_clock.get_watermark()
            
            if abs(old_mtime - mtime) > 1e-6:
                if (watermark - mtime) < self.hot_file_threshold:
                    node.integrity_suspect = True
                    if path not in self.state.suspect_list:
                         expiry = time.monotonic() + self.hot_file_threshold
                         self.state.suspect_list[path] = (expiry, mtime)
                         heapq.heappush(self.state.suspect_heap, (expiry, path))
                else:
                    node.integrity_suspect = False
                    self.state.suspect_list.pop(path, None)
            else:
                if (watermark - mtime) >= self.hot_file_threshold:
                    node.integrity_suspect = False
                    self.state.suspect_list.pop(path, None)

    async def get_data_view(self, **kwargs) -> dict:
        """Required by the ViewDriver ABC."""
        return await self.get_directory_tree(**kwargs) # type: ignore

    # --- Legacy Aliases for White-box Tests ---
    @property
    def _last_audit_start(self): return self.state.last_audit_start
    @_last_audit_start.setter
    def _last_audit_start(self, v): self.state.last_audit_start = v

    @property
    def _audit_seen_paths(self): return self.state.audit_seen_paths
    
    @property
    def _logical_clock(self): return self.state.logical_clock
    
    @property
    def _suspect_list(self): return self.state.suspect_list
    
    @property
    def _suspect_heap(self): return self.state.suspect_heap

    @property
    def _tombstone_list(self): return self.state.tombstone_list
    @_tombstone_list.setter
    def _tombstone_list(self, v): self.state.tombstone_list = v

    @property
    def _blind_spot_deletions(self): return self.state.blind_spot_deletions
    
    @property
    def _blind_spot_additions(self): return self.state.blind_spot_additions

    def _get_node(self, path): return self.state.get_node(path)
    
    def _cleanup_expired_suspects_unlocked(self):
        return self.arbitrator.cleanup_expired_suspects()
