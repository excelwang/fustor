from .base import ParserBase

class AuditMixin(ParserBase):
    async def handle_audit_start(self):
        """Called when an Audit cycle begins."""
        async with self._global_exclusive_lock():
            now = self._logical_clock.get_watermark()
            is_late_start = False
            
            if self._last_audit_start is not None and (now - self._last_audit_start) < 5.0 and self._audit_seen_paths:
                self.logger.info(f"Audit Start signal received late. Preserving observed flags.")
                is_late_start = True
            
            self._last_audit_start = now
            
            if not is_late_start:
                 self._audit_seen_paths.clear()
            
            self.logger.info(f"Audit started for datastore {self.datastore_id}. Flags preserved. late_start={is_late_start}")
    
    async def handle_audit_end(self):
        """Called when an Audit cycle ends."""
        async with self._global_exclusive_lock():
            if self._last_audit_start is None:
                return
            
            # 1. Tombstone cleanup
            before_count = len(self._tombstone_list)
            old_tombstones = list(self._tombstone_list.keys())
            
            self._tombstone_list = {
                path: ts for path, ts in self._tombstone_list.items()
                if ts >= self._last_audit_start
            }
            
            tombstones_cleaned = before_count - len(self._tombstone_list)
            if tombstones_cleaned > 0:
                dropped = [p for p in old_tombstones if p not in self._tombstone_list]
                self.logger.info(f"Tombstone CLEANUP: removed {tombstones_cleaned} items. Removed: {dropped}")
            
            # 2. Optimized Missing File Detection
            missing_detected = 0
            paths_to_delete = []

            if self._audit_seen_paths:
                for path in self._audit_seen_paths:
                    dir_node = self._directory_path_map.get(path)
                    
                    if dir_node and not getattr(dir_node, 'audit_skipped', False):
                        for child_name, child_node in list(dir_node.children.items()):
                            if child_node.path not in self._audit_seen_paths:
                                if child_node.path in self._tombstone_list:
                                    continue
                                
                                if child_node.last_updated_at > self._last_audit_start:
                                    self.logger.info(f"Preserving node from audit deletion (Stale): {child_node.path}")
                                    continue

                                self.logger.debug(f"Blind-spot deletion detected (Optimized): {child_node.path}")
                                paths_to_delete.append(child_node.path)
            
                missing_detected = len(paths_to_delete)
                for path in paths_to_delete:
                    await self._process_delete_in_memory(path)
                    self._blind_spot_deletions.add(path)
                    self._blind_spot_additions.discard(path)
            
            self.logger.info(
                f"Audit ended for datastore {self.datastore_id}. "
                f"Tombstones cleaned: {tombstones_cleaned}, Blind-spot deletions: {missing_detected}"
            )
            
            self._last_audit_start = None
            self._audit_seen_paths.clear()
