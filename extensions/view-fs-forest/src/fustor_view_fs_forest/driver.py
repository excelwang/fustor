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
    - Events are routed to specific internal trees based on `fusion_pipe_id`.
    - `fusion_pipe_id` is injected into event metadata by the Generic Pipe before routing.
    
    Lifecycle:
    - Internal trees are created lazily upon first event from a fusion_pipe_id.
    - An internal tree is a standard FSViewDriver, reusing all logic 
      (Consistency, Audit, Tombstones) but scoped to that pipe's data stream.
    """
    target_schema = "fs"
    # Live-mode flag: triggers full state reset when all sessions close
    requires_full_reset_on_session_close = True
    
    def __init__(self, id: str, view_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(id, view_id, config)
        self.logger = logger
        
        # Map: fusion_pipe_id -> FSViewDriver instance
        self._trees: Dict[str, FSViewDriver] = {}
        self._tree_lock = asyncio.Lock()
        
        # Map: session_id -> fusion_pipe_id (for lifecycle routing)
        self._session_to_fusion_pipe: Dict[str, str] = {}
        
        # We share configuration with sub-trees, but might want to inject overrides
        # Sub-trees will have view_id = "{forest_id}:{fusion_pipe_id}"
        pass

    async def initialize(self):
        """Initialize the forest driver itself."""
        # Nothing specific to init for the forest container yet.
        pass

    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Determine session role with SCOPED leader election (per-tree).
        """
        fusion_pipe_id = kwargs.get("fusion_pipe_id") or kwargs.get("pipe_id")
        if not fusion_pipe_id:
            # Try to infer or fallback
            return {"role": "follower", "error": "fusion_pipe_id required for ForestView session"}

        from fustor_fusion.view_state_manager import view_state_manager
        election_id = f"{self.view_id}:{fusion_pipe_id}"
            
        # Register session mapping internally
        self._session_to_fusion_pipe[session_id] = fusion_pipe_id
        
        # Ensure tree exists so it's ready for events
        await self._get_or_create_tree(fusion_pipe_id)

        is_leader = await view_state_manager.try_become_leader(election_id, session_id)
        
        if is_leader:
            await view_state_manager.set_authoritative_session(election_id, session_id)
            
        return {
            "role": "leader" if is_leader else "follower",
            "election_key": election_id
        }

    async def process_event(self, event: Any) -> bool:
        """
        Route event to the correct internal tree based on fusion_pipe_id.
        """
        metadata = getattr(event, "metadata", {}) or {}
        fusion_pipe_id = metadata.get("fusion_pipe_id") or metadata.get("pipe_id")
        
        if not fusion_pipe_id:
            # Metadata injection failed or legacy event?
            # We cannot route without fusion_pipe_id in Forest mode.
            self.logger.warning(f"ForestView received event without 'fusion_pipe_id' metadata. Dropping. Event ID: {getattr(event, 'event_id', 'unknown')}")
            return False

        tree = await self._get_or_create_tree(fusion_pipe_id)
        return await tree.process_event(event)

    async def _get_or_create_tree(self, fusion_pipe_id: str) -> FSViewDriver:
        """Lazy initialization of internal tree for a given pipe."""
        if fusion_pipe_id not in self._trees:
            async with self._tree_lock:
                if fusion_pipe_id not in self._trees:
                    self.logger.info(f"Creating new internal FS Tree for fusion_pipe_id='{fusion_pipe_id}'")
                    
                    # Construct a scoped ID for the sub-tree
                    # Use a colon separator which is standard for namespacing
                    sub_view_id = f"{self.view_id}:{fusion_pipe_id}"
                    
                    # Create the sub-driver
                    tree = FSViewDriver(
                        id=f"{self.id}:{fusion_pipe_id}",
                        view_id=sub_view_id,
                        config=self.config
                    )
                    
                    # Initialize it immediately
                    await tree.initialize()
                    self._trees[fusion_pipe_id] = tree
                    
        return self._trees[fusion_pipe_id]

    async def get_data_view(self, **kwargs) -> Any:
        """
        Default aggregation view (usually required by ABC).
        For Forest, this might default to full tree aggregation.
        """
        return await self.get_directory_tree(**kwargs)

    async def get_directory_stats(self, strategy: str = "best") -> Dict[str, Any]:
        """
        Get directory statistics.
        
        Args:
            strategy: "best" (default) returns tree with max items,
                     "aggregate" returns sum of all trees.
        """
        current_trees = list(self._trees.values())
        if not current_trees:
            return {
                "item_count": 0, "total_size": 0, "latency_ms": 0.0,
                "staleness_seconds": 0.0, "suspect_file_count": 0, "tree_count": 0
            }

        if strategy == "best":
            # For "best", we pick the tree that seems most complete/largest
            best_stats = None
            for tree in current_trees:
                try:
                    stats = await tree.get_directory_stats()
                    if best_stats is None or stats.get("item_count", 0) > best_stats.get("item_count", 0):
                        best_stats = stats
                except Exception as e:
                    self.logger.warning(f"Failed to get stats from sub-tree: {e}")
            
            if best_stats:
                best_stats["tree_count"] = len(current_trees)
                return best_stats
        
        # Default/Aggregate logic
        total_items = 0
        total_size = 0
        max_latency = 0.0
        max_staleness = 0.0
        suspect_count = 0
        
        for tree in current_trees:
            try:
                stats = await tree.get_directory_stats()
                total_items += stats.get("item_count", 0)
                total_size += stats.get("total_size", 0)
                max_latency = max(max_latency, stats.get("latency_ms", 0.0))
                max_staleness = max(max_staleness, stats.get("staleness_seconds", 0.0))
                suspect_count += stats.get("suspect_file_count", 0)
            except Exception as e:
                self.logger.warning(f"Failed to get stats from sub-tree: {e}")
                
        return {
            "item_count": total_items,
            "total_size": total_size,
            "latency_ms": max_latency,
            "staleness_seconds": max_staleness,
            "suspect_file_count": suspect_count,
            "tree_count": len(current_trees)
        }

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

    # --- Lifecycle and Audit Delegation ---
    
    async def on_session_start(self, **kwargs):
        """Delegate session start to the specific tree for this session."""
        session_id = kwargs.get("session_id")
        if not session_id:
             # Broadcast if no session_id (unlikely in new architecture)
             for tree in self._trees.values():
                 await tree.on_session_start(**kwargs)
             return

        fusion_pipe_id = self._session_to_fusion_pipe.get(session_id) or kwargs.get("fusion_pipe_id") or kwargs.get("pipe_id")
        if fusion_pipe_id:
             self._session_to_fusion_pipe[session_id] = fusion_pipe_id
             tree = await self._get_or_create_tree(fusion_pipe_id)
             await tree.on_session_start(**kwargs)
        else:
             self.logger.warning(f"ForestView {self.view_id}: session {session_id} has no fusion_pipe_id mapping, cannot route to tree.")

    async def on_session_close(self, **kwargs):
        """Delegate session close and cleanup mapping."""
        session_id = kwargs.get("session_id")
        if not session_id:
             for tree in self._trees.values():
                 await tree.on_session_close(**kwargs)
             return

        fusion_pipe_id = self._session_to_fusion_pipe.pop(session_id, None)
        if fusion_pipe_id and fusion_pipe_id in self._trees:
            await self._trees[fusion_pipe_id].on_session_close(**kwargs)
    
    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None:
        """Mark scoped view key for this session's sub-tree."""
        fusion_pipe_id = (kwargs.get("metadata") or {}).get("fusion_pipe_id") or (kwargs.get("metadata") or {}).get("pipe_id") or self._session_to_fusion_pipe.get(session_id)
        if fusion_pipe_id:
            from fustor_fusion.view_state_manager import view_state_manager
            scoped_key = f"{self.view_id}:{fusion_pipe_id}"
            await view_state_manager.set_snapshot_complete(scoped_key, session_id)
            self.logger.debug(f"ForestView {self.view_id}: Marked scoped tree {scoped_key} snapshot complete")

    async def handle_audit_start(self):
        """Broadcast audit start to all trees."""
        for tree in self._trees.values():
            await tree.handle_audit_start()

    async def handle_audit_end(self):
        """Broadcast audit end to all trees."""
        for tree in self._trees.values():
            await tree.handle_audit_end()

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
             self._session_to_fusion_pipe.clear()
