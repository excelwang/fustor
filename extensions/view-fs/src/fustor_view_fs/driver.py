from typing import Dict, Any, Optional, List, Tuple
import asyncio
import heapq
import time
from .base import FSViewBase
from .state import FSState
from .tree import TreeManager
from .arbitrator import FSArbitrator
from .audit import AuditManager
from .query import FSViewQuery

class FSViewDriver(FSViewBase):
    """
    Consistent File System View Driver.
    Refactored with Composition over Inheritance.
    Coordinates various components to maintain a fused, consistent view 
    of the FS using Smart Merge arbitration logic.
    
    Live-mode: data is entirely built from real-time agent pushes.
    When all sessions close, state is reset and queries return 503.
    """
    target_schema = "fs"
    
    # Live-mode flag: triggers full state reset when all sessions close
    requires_full_reset_on_session_close = True

    def __init__(self, id: str, view_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(id, view_id, config) 
        
        # Composition Root
        self.state = FSState(view_id, config=self.config) # FSState uses group view_id
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
        async with self._global_read_lock():
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
        async with self._global_read_lock():
            return self.arbitrator.cleanup_expired_suspects()

    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Determine session role with standard (global) leader election.
        """
        from fustor_fusion.view_state_manager import view_state_manager
        
        # Standard View behavior: Use the global view_id for election
        is_leader = await view_state_manager.try_become_leader(self.view_id, session_id)
        
        if is_leader:
            await view_state_manager.set_authoritative_session(self.view_id, session_id)
            # Lock for concurrent push if needed (typically handled by bridge based on pipe config, 
            # but we can enforce it here if strictly required by driver)
            
        return {
            "role": "leader" if is_leader else "follower",
            "election_key": self.view_id
        }

    async def on_session_start(self, **kwargs):
        """Handles new session lifecycle."""
        async with self._global_exclusive_lock():
            # If we were in an audit, it's now invalid
            self.state.last_audit_start = None
            self.state.audit_seen_paths.clear()
            
            # Per CONSISTENCY_DESIGN.md §4.4: Clear blind-spot lists on new session
            # Blind spots will be rediscovered by the new session's snapshot
            self.state.blind_spot_additions.clear()
            self.state.blind_spot_deletions.clear()
            
            self.logger.debug(f"New session sequence started. Cleared audit buffer and blind-spot lists.")

    async def on_session_close(self, **kwargs):
        """
        Handle individual session close.
        
        Full state reset (when ALL sessions close) is handled by
        SessionManager._check_if_view_live() which reads
        requires_full_reset_on_session_close and calls reset_views().
        """
        self.logger.info(f"Session closed for view {self.view_id}")

    async def reset(self):
        """Full reset of the in-memory view."""
        async with self._global_exclusive_lock():
            # 1. Clear view-specific in-memory state
            self.state.reset()
            
            self.logger.debug(f"FS View state for {self.view_id} has been reset. Global sessions and ingestion state remain unaffected.")

    # --- Query Delegation ---

    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        # Removed on_demand_scan param to re-enable TypeError-based fallback trigger for E2E tests.
        # Use trigger_on_demand_scan via on_command_fallback.

        async with self._global_read_lock():
            return self.query.get_directory_tree(path=path, recursive=recursive, max_depth=max_depth, only_path=only_path)

    async def get_blind_spot_list(self) -> Dict[str, Any]:
        async with self._global_read_lock():
            return self.query.get_blind_spot_list()

    async def get_suspect_list(self) -> Dict[str, float]:
        async with self._global_read_lock():
             suspects = {path: expires_at for path, (expires_at, _) in self.state.suspect_list.items()}
             return suspects

    async def search_files(self, query: str) -> List[Dict[str, Any]]:
        async with self._global_read_lock():
            return self.query.search_files(query)

    async def get_directory_stats(self) -> Dict[str, Any]:
        async with self._global_read_lock():
            return self.query.get_stats()

    async def get_subtree_stats(self, path: str) -> Dict[str, Any]:
        """Calculates stats for a subtree (file_count, dir_count, total_size, latest_mtime) without serialization."""
        async with self._global_read_lock():
            return self.tree_manager.get_subtree_stats(path)

    async def update_suspect(self, path: str, mtime: float, size: Optional[int] = None):
        """Update suspect status from sentinel feedback.
        
        Per Spec §4.3 Stability-based Model:
        - Stable (mtime unchanged AND size unchanged) + TTL expired -> clear suspect
        - Active (mtime changed OR size changed) -> renew TTL
        """
        async with self._global_read_lock():
            # Fix: Do NOT sample skew from Sentinel feedback (often old files).
            # This prevents polluting the Skew Histogram with "Lag" from passive verification.
            self.state.logical_clock.update(mtime, can_sample_skew=False)
            
            node = self.state.get_node(path)
            if not node: return

            old_mtime = node.modified_time
            # Keep mtime as reported for now, but we might normalize it if skewed
            
            skew = self.state.logical_clock.get_skew()
            
            # Stability Check: Allow match on Raw Mtime OR Skew-Corrected Mtime
            # Agent A (Skewed +2h) reports mtime=+2h. True mtime=0h. Skew=-2h.
            # check 1: 0 == 2? False.
            # check 2: 0 == 2 + (-2)? True.
            is_raw_stable = abs(old_mtime - mtime) < 1e-6
            is_skew_stable = abs(old_mtime - (mtime + skew)) < 1e-6
            
            is_mtime_stable = False

            if is_skew_stable and not is_raw_stable:
                self.logger.debug(f"Sentinel reported SKEWED mtime for {path} (Reported: {mtime}, Skew: {skew}, Corrected: {mtime+skew}). Treating as STABLE.")
                # Normalize the mtime to the stable one for updates
                mtime = old_mtime
                is_mtime_stable = True
            else:
                is_mtime_stable = is_raw_stable

            # Check size stability if provided
            is_size_stable = True
            if size is not None and hasattr(node, "size"):
                # If node has size, check it.
                # Note: node.size might be None? usually not for files.
                if node.size is not None:
                     is_size_stable = (node.size == size)
            
            is_stable = is_mtime_stable and is_size_stable

            # Age Calculation: same-domain (watermark and mtime both in NFS time, Spec §5.2)
            watermark = self.state.logical_clock.get_watermark()
            age = watermark - mtime
            
            # Hot Check
            is_hot = age < self.hot_file_threshold
            
            self.logger.debug(f"SENT_CHECK: {path} mtime={mtime:.1f} stable={is_stable} hot={is_hot} age={age:.1f}")

            if path not in self.state.suspect_list:
                # Not in suspect list - nothing to do
                return
                
            if is_stable:
                # Stable!
                if not is_hot:
                    # Stable AND Cold -> Clear immediately (Accelerate convergence)
                    self.logger.debug(f"Sentinel check STABLE & COLD: {path}. Clearing suspect flag immediately.")
                    self.state.suspect_list.pop(path, None)
                    node.integrity_suspect = False
                else:
                    # Stable but still Hot -> Keep until TTL expires to prove stability over time.
                    self.logger.debug(f"Sentinel check STABLE (still hot): {path}. Keeping suspect until TTL.")
                return
            else:
                # Active: Update node and renew TTL
                node.modified_time = mtime
                if size is not None:
                     node.size = size
                node.integrity_suspect = True
                
                expiry = time.monotonic() + self.hot_file_threshold
                self.state.suspect_list[path] = (expiry, mtime)
                heapq.heappush(self.state.suspect_heap, (expiry, path))
                self.logger.debug(f"Suspect RENEWED (mismatch via Sentinel): {path} (Mtime: {mtime}, Size: {size})")



    async def trigger_on_demand_scan(self, path: str, recursive: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Triggers an on-demand scan on the agent side by broadcasting a command to ALL pipes.
        Events produced are MessageSource.ON_DEMAND_JOB (Tier 3 compensatory, see §4.5).
        Returns: (success, job_id)
        """
        from fustor_fusion.core.session_manager import session_manager
        
        async with self._global_read_lock():
            # 1. Get ALL active sessions for this view
            active_sessions = await session_manager.get_view_sessions(self.view_id)
            if not active_sessions:
                self.logger.warning(f"No active sessions for view {self.view_id}. Cannot trigger on-demand scan.")
                return False, None
            
            session_ids = list(active_sessions.keys())
            
            # 2. Create a unified agent job record for tracking all sessions
            job_id = await session_manager.create_agent_job(self.view_id, path, session_ids)
            
            # 3. Queue the command for EACH session (Broadcast)
            command = {
                "type": "scan",
                "path": path,
                "recursive": recursive,
                "job_id": job_id,
                "created_at": time.time()
            }
            
            success_count = 0
            for session_id in session_ids:
                if await session_manager.queue_command(self.view_id, session_id, command):
                    success_count += 1
            
            self.logger.info(f"Broadcasted on-demand scan {job_id} to {success_count}/{len(session_ids)} sessions for path {path}")
            return success_count > 0, job_id

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
