import logging
import time
from .state import FSState
from .tree import TreeManager

logger = logging.getLogger(__name__)

class AuditManager:
    """
    Manages the Audit Sync lifecycle and Missing Item Detection.
    """
    def __init__(self, state: FSState, tree_manager: TreeManager):
        self.state = state
        self.tree_manager = tree_manager
        self.logger = logging.getLogger(f"fustor_view.fs.audit.{state.view_id}")

    async def handle_start(self):
        """Prepares state for a new audit cycle."""
        now = time.time()
        is_late_start = False
        
        # If we received another start signal very recently, don't clear flags
        if self.state.last_audit_start is not None and (now - self.state.last_audit_start) < 5.0 and self.state.audit_seen_paths:
            self.logger.info("Audit Start signal received late. Preserving observed flags.")
            is_late_start = True
        
        self.state.last_audit_start = now
        if not is_late_start:
             self.state.audit_seen_paths.clear()
        
        self.logger.info(f"Audit started at local time {now}. late_start={is_late_start}")

    async def handle_end(self):
        """Finalizes audit cycle, performs Tombstone cleanup and Missing Item Detection."""
        if self.state.last_audit_start is None:
            return

        # 1. Tombstone Cleanup (Rule: Purge tombstones older than 1 hour per §6.3)
        # Use physical local time for TTL calculation to be stable against logical clock jumps
        # Reference: CONSISTENCY_DESIGN.md §6.3
        tombstone_ttl_seconds = self.state.config.get("tombstone_ttl_seconds", 3600.0)
        now_physical = time.time()
        before = len(self.state.tombstone_list)
        
        self.state.tombstone_list = {
            path: (l_ts, p_ts) for path, (l_ts, p_ts) in self.state.tombstone_list.items()
            if (now_physical - p_ts) < tombstone_ttl_seconds
        }
        cleaned = before - len(self.state.tombstone_list)
        if cleaned > 0:
            self.logger.info(f"Tombstone CLEANUP: removed {cleaned} items (TTL > 1hr).")

        # 2. Optimized Missing File Detection
        missing_count = 0
        if self.state.audit_seen_paths:
            paths_to_delete = []
            for path in self.state.audit_seen_paths:
                dir_node = self.state.directory_path_map.get(path)
                
                # If directory was NOT skipped (i.e. it was fully scanned)
                if dir_node and not getattr(dir_node, 'audit_skipped', False):
                    for child_name, child_node in list(dir_node.children.items()):
                        if child_node.path not in self.state.audit_seen_paths:
                            # Child not seen in audit, let's check if it should be deleted
                            
                            # Skip if protected by a newer Tombstone
                            if child_node.path in self.state.tombstone_list:
                                continue
                            
                            # Stale Evidence Protection: If node updated AFTER audit started, don't delete
                            if child_node.last_updated_at > self.state.last_audit_start:
                                self.logger.debug(f"Preserving node from audit deletion (Stale): {child_node.path}")
                                continue

                            self.logger.info(f"Blind-spot deletion detected: {child_node.path}")
                            paths_to_delete.append(child_node.path)
            
            missing_count = len(paths_to_delete)
            for path in paths_to_delete:
                await self.tree_manager.delete_node(path)
                self.state.blind_spot_deletions.add(path)
                self.state.blind_spot_additions.discard(path)
        
        self.logger.info(f"Audit ended. Tombstones cleaned: {cleaned}, Missing items deleted: {missing_count}")
        self.state.last_audit_finished_at = now_physical
        self.state.audit_cycle_count += 1
        self.state.last_audit_start = None
        self.state.audit_seen_paths.clear()
