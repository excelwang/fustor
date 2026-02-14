import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

from fustor_core.drivers import ViewDriver
from fustor_view_fs.driver import FSViewDriver
from fustor_view_fs.nodes import DirectoryNode

logger = logging.getLogger(__name__)

class ForestFSViewDriver(ViewDriver):
    """
    Forest View Driver: Aggregates multiple FSViewDriver instances (trees).
    
    Routing:
    - Events are routed to specific internal trees based on `pipe_id`.
    - `pipe_id` is injected into event metadata by the Generic Pipe before routing.
    
    Lifecycle:
    - Internal trees are created lazily upon first event from a pipe_id.
    - An internal tree is a standard FSViewDriver, reusing all logic 
      (Consistency, Audit, Tombstones) but scoped to that pipe's data stream.
    """
    target_schema = "fs"
    # Removed use_scoped_session_leader flag as logic is now internal

    def __init__(self, id: str, view_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(id, view_id, config)
        self.logger = logger
        
        # Map: pipe_id -> FSViewDriver instance
        self._trees: Dict[str, FSViewDriver] = {}
        self._tree_lock = asyncio.Lock()
        
        # We share configuration with sub-trees, but might want to inject overrides
        # Sub-trees will have view_id = "{forest_id}:{pipe_id}"
        pass

    async def initialize(self):
        """Initialize the forest driver itself."""
        # Nothing specific to init for the forest container yet.
        pass

    async def resolve_session_role(self, session_id: str, pipe_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Determine session role with SCOPED leader election (per-tree).
        """
        from fustor_fusion.view_state_manager import view_state_manager
        election_id = f"{self.view_id}:{pipe_id}"
            
        # Register session mapping internally
        if pipe_id:
             # Ensure tree exists so it's ready for events
             await self._get_or_create_tree(pipe_id)

        is_leader = await view_state_manager.try_become_leader(election_id, session_id)
        
        if is_leader:
            await view_state_manager.set_authoritative_session(election_id, session_id)
            
        return {
            "role": "leader" if is_leader else "follower",
            "election_key": election_id
        }

    async def process_event(self, event: Any) -> bool:
        """
        Route event to the correct internal tree based on pipe_id.
        """
        metadata = getattr(event, "metadata", {}) or {}
        pipe_id = metadata.get("pipe_id")
        
        if not pipe_id:
            # Metadata injection failed or legacy event?
            # We cannot route without pipe_id in Forest mode.
            self.logger.warning(f"ForestView received event without 'pipe_id' metadata. Dropping. Event ID: {getattr(event, 'event_id', 'unknown')}")
            return False

        tree = await self._get_or_create_tree(pipe_id)
        return await tree.process_event(event)

    async def _get_or_create_tree(self, pipe_id: str) -> FSViewDriver:
        """Lazy initialization of internal tree for a given pipe."""
        if pipe_id not in self._trees:
            async with self._tree_lock:
                if pipe_id not in self._trees:
                    self.logger.info(f"Creating new internal FS Tree for pipe_id='{pipe_id}'")
                    
                    # Construct a scoped ID for the sub-tree
                    # Use a colon separator which is standard for namespacing
                    sub_view_id = f"{self.view_id}:{pipe_id}"
                    
                    # Create the sub-driver
                    tree = FSViewDriver(
                        id=f"{self.id}:{pipe_id}",
                        view_id=sub_view_id,
                        config=self.config
                    )
                    
                    # Initialize it immediately
                    await tree.initialize()
                    self._trees[pipe_id] = tree
                    
        return self._trees[pipe_id]

    async def get_data_view(self, **kwargs) -> Any:
        """
        Default aggregation view (usually required by ABC).
        For Forest, this might default to full tree aggregation.
        """
        return await self.get_directory_tree(**kwargs)

    # --- Aggregation API Methods ---

    async def get_subtree_stats_agg(self, path: str = "/") -> Dict[str, Any]:
        """
        Aggregate stats from all trees.
        Returns explicit per-member stats for comparison.
        """
        results = {}
        
        # Snapshot current keys to iterate safely
        active_pipes = list(self._trees.items())
        
        # Execute in parallel
        async def fetch(pid, tree):
            try:
                stats = await tree.get_subtree_stats(path)
                return pid, {"status": "ok", **stats}
            except Exception as e:
                self.logger.exception(f"Error fetching stats from tree {pid}")
                return pid, {"status": "error", "error": str(e)}

        if not active_pipes:
            return {"path": path, "members": [], "best": None}

        tasks = [fetch(pid, tree) for pid, tree in active_pipes]
        responses = await asyncio.gather(*tasks)
        
        members = []
        for pid, res in responses:
            res["view_id"] = pid # API expects view_id/member_id identification
            members.append(res)
            
        # Determine "Best" (default strategy: file_count)
        best = None
        max_val = -1
        best_reason = "file_count"
        
        for m in members:
            if m.get("status") == "ok":
                val = m.get("file_count", 0)
                if val > max_val:
                    max_val = val
                    best = {
                        "view_id": m["view_id"],
                        "reason": best_reason,
                        "value": val
                    }

        return {
            "path": path,
            "members": members,
            "best": best
        }

    async def get_directory_tree(self, path: str = "/", best: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Get directory tree.
        If 'best' strategy is provided, only return the tree from the best member.
        Otherwise, return all member trees map.
        """
        # 1. If optimization requested, run stats first to find winner
        target_pipe_ids = list(self._trees.keys())
        
        if best:
            stats = await self.get_subtree_stats_agg(path)
            if stats.get("best"):
                # "view_id" in stats response corresponds to our pipe_id
                winner_pipe = stats["best"]["view_id"]
                if winner_pipe in self._trees:
                    target_pipe_ids = [winner_pipe]
                else:
                    return {"path": path, "members": {}, "error": "Best view not found"}
            else:
                return {"path": path, "members": {}, "error": "No valid views found for strategy"}

        # 2. Fetch trees
        member_results = {}
        
        async def fetch_tree(pid):
            tree = self._trees[pid]
            try:
                # Delegate to FSViewDriver.get_directory_tree
                # It returns the node structure (dict) or raises/returns None
                data = await tree.get_directory_tree(path=path, **kwargs)
                return pid, {"status": "ok", "data": data}
            except Exception as e:
                return pid, {"status": "error", "error": str(e)}

        tasks = [fetch_tree(pid) for pid in target_pipe_ids]
        if tasks:
            results = await asyncio.gather(*tasks)
            for pid, res in results:
                member_results[pid] = res

        result = {
            "path": path,
            "members": member_results
        }
        
        if best and target_pipe_ids:
            # Add metadata about why this one was chosen
            result["best_view_selected"] = target_pipe_ids[0]
            
        return result

    # --- Passthrough for other lifecycle methods if needed ---
    
    async def cleanup_expired_suspects(self):
        """Periodic cleanup: delegate to all sub-trees."""
        tasks = [tree.cleanup_expired_suspects() for tree in self._trees.values()]
        if tasks:
            await asyncio.gather(*tasks)

    async def reset(self):
        """Reset all sub-trees."""
        async with self._tree_lock:
             tasks = [tree.reset() for tree in self._trees.values()]
             if tasks:
                 await asyncio.gather(*tasks)
             self._trees.clear()
