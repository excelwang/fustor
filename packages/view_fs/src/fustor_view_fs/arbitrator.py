import asyncio
import os
import time
import heapq
import logging
from typing import Any, Dict, Tuple
from fustor_event_model.models import MessageSource, EventType
from .state import FSState
from .tree import TreeManager

logger = logging.getLogger(__name__)

class FSArbitrator:
    """
    Core arbitration logic for FS Views.
    Implements Smart Merge, Tombstone Protection, and Clock Synchronization.
    """
    def __init__(self, state: FSState, tree_manager: TreeManager, hot_file_threshold: float):
        self.state = state
        self.tree_manager = tree_manager
        self.hot_file_threshold = hot_file_threshold
        self.logger = logging.getLogger(f"fustor_view.fs.arbitrator.{state.datastore_id}")
        self.suspect_cleanup_interval = 0.5

    async def process_event(self, event: Any) -> bool:
        """Process an event using Smart Merge logic."""
        if not event.rows:
            return False

        message_source = self._get_message_source(event)
        is_realtime = (message_source == MessageSource.REALTIME)
        is_audit = (message_source == MessageSource.AUDIT)
        
        # Problem 2 Fix: Strictly update clock using event.index if available
        # event.index is expected to be milliseconds
        if event.index > 0:
            agent_watermark = event.index / 1000.0
            self.state.logical_clock.update(agent_watermark)
            
            # Latency tracking
            logical_now_ms = self.state.logical_clock.get_watermark() * 1000
            self.state.last_event_latency = max(0.0, logical_now_ms - event.index)
        
        # Audit Start Auto-detection
        if is_audit and self.state.last_audit_start is None and event.index > 0:
            self.state.last_audit_start = self.state.logical_clock.get_watermark()
            self.logger.info(f"Auto-detected Audit Start logical time: {self.state.last_audit_start}")

        self.state.current_session_id = getattr(event, "session_id", self.state.current_session_id)

        rows_processed = 0
        for payload in event.rows:
            path = self._normalize_path(payload.get('path') or payload.get('file_path'))
            if not path: continue
            
            rows_processed += 1
            if rows_processed % 100 == 0: await asyncio.sleep(0)

            # Update clock with row mtime
            mtime = payload.get('modified_time', 0.0) or 0.0
            self.state.logical_clock.update(mtime)
            
            if is_audit:
                self.state.audit_seen_paths.add(path)
                self.state.blind_spot_deletions.discard(path)

            if event.event_type == EventType.DELETE:
                await self._handle_delete(path, is_realtime, mtime)
            elif event.event_type in [EventType.INSERT, EventType.UPDATE]:
                await self._handle_upsert(path, payload, event, message_source, mtime)
        
        return True

    async def _handle_delete(self, path: str, is_realtime: bool, mtime: float):
        if is_realtime:
            existing = self.state.get_node(path)
            existing_mtime = existing.modified_time if existing else 0.0
            
            await self.tree_manager.delete_node(path)
            
            # Update clock with the time of deletion (either index or existing mtime)
            self.state.logical_clock.update(existing_mtime)
            
            # Create Tombstone
            ts = self.state.logical_clock.get_watermark()
            self.state.tombstone_list[path] = ts
            self.logger.info(f"Tombstone CREATED for {path} at {ts}")
            
            self.state.suspect_list.pop(path, None)
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
        else:
            # Audit/Snapshot delete: subject to Tombstone protection
            if path not in self.state.tombstone_list:
                await self.tree_manager.delete_node(path)
                self.state.blind_spot_deletions.discard(path)
                self.state.blind_spot_additions.discard(path)

    async def _handle_upsert(self, path: str, payload: Dict, event: Any, source: MessageSource, mtime: float):
        # 1. Tombstone Protection
        if path in self.state.tombstone_list:
            tombstone_ts = self.state.tombstone_list[path]
            is_realtime = (source == MessageSource.REALTIME)
            
            # Check if this is "New activity" after deletion
            event_ref_ts = (event.index / 1000.0) if event.index > 0 else mtime
            if event_ref_ts > (tombstone_ts + 1e-5) or mtime > (tombstone_ts + 1e-5):
                self.logger.info(f"TOMBSTONE_CLEARED for {path}")
                self.state.tombstone_list.pop(path, None)
            else:
                self.logger.debug(f"TOMBSTONE_BLOCK: {path}")
                return

        # 2. Smart Merge Arbitration
        existing = self.state.get_node(path)
        is_audit = (source == MessageSource.AUDIT)
        is_realtime = (source == MessageSource.REALTIME)
        
        if not is_realtime:
            # Snapshot/Audit arbitration
            if existing:
                # If mtime is not newer, ignore (unless audit_skipped which we use for heartbeats/protection)
                if not payload.get('audit_skipped') and existing.modified_time >= mtime:
                    return
            
            if is_audit and existing is None:
                # Rule 3: Parent Mtime Check
                parent_path = payload.get('parent_path')
                parent_mtime_audit = payload.get('parent_mtime')
                memory_parent = self.state.directory_path_map.get(parent_path)
                if memory_parent and memory_parent.modified_time > (parent_mtime_audit or 0):
                    # Memory parent is newer than what audit saw -> audit result is stale
                    return

        # Capture state before update for arbitration
        old_mtime = existing.modified_time if existing else 0.0
        
        # Perform the actual update
        await self.tree_manager.update_node(payload, path)
        node = self.state.get_node(path)
        if not node: return

        # 3. Blind Spot and Suspect Management
        if is_realtime:
            self.state.suspect_list.pop(path, None)
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
            node.integrity_suspect = False
            node.known_by_agent = True
        else:
            # Manage Suspect List (Hot Data)
            age = self.state.logical_clock.get_watermark() - mtime
            mtime_changed = (existing is None) or (abs(old_mtime - mtime) > 1e-6)
            
            if mtime_changed:
                if is_audit:
                    self.state.blind_spot_additions.add(path)
                    node.known_by_agent = False
                
                if age < self.hot_file_threshold:
                    if not getattr(node, 'known_by_agent', False) or (is_realtime and mtime_changed):
                        node.integrity_suspect = True
                        if path not in self.state.suspect_list:
                            remaining_life = self.hot_file_threshold - age
                            expiry = time.monotonic() + max(0.0, remaining_life)
                            self.state.suspect_list[path] = (expiry, mtime)
                            heapq.heappush(self.state.suspect_heap, (expiry, path))
                else:
                    node.integrity_suspect = False
                    self.state.suspect_list.pop(path, None)
            else:
                # mtime unchanged, if cold, clear suspect
                if age >= self.hot_file_threshold:
                    node.integrity_suspect = False
                    self.state.suspect_list.pop(path, None)

    def cleanup_expired_suspects(self) -> int:
        """Poll suspect heap and verify stability."""
        now_mono = time.monotonic()
        if now_mono - self.state.last_suspect_cleanup_time < self.suspect_cleanup_interval:
            return 0
        
        self.state.last_suspect_cleanup_time = now_mono
        processed = 0
        
        while self.state.suspect_heap and self.state.suspect_heap[0][0] <= now_mono:
            expires_at, path = heapq.heappop(self.state.suspect_heap)
            if path not in self.state.suspect_list: continue
            
            curr_expiry, recorded_mtime = self.state.suspect_list[path]
            if abs(curr_expiry - expires_at) > 1e-6: continue
            
            node = self.state.get_node(path)
            if not node:
                self.state.suspect_list.pop(path, None)
                continue
            
            # Stability Check: Has mtime changed since we added it to suspect list?
            if abs(node.modified_time - recorded_mtime) > 1e-6:
                # Active! Renew TTL
                new_expiry = now_mono + self.hot_file_threshold
                self.state.suspect_list[path] = (new_expiry, node.modified_time)
                heapq.heappush(self.state.suspect_heap, (new_expiry, path))
                self.logger.info(f"Suspect RENEWED (Active): {path}")
            else:
                # Stable! Cool-off complete
                self.logger.info(f"Suspect EXPIRED (Stable): {path}")
                self.state.suspect_list.pop(path, None)
                node.integrity_suspect = False
            processed += 1
        return processed

    def _normalize_path(self, raw_path: str) -> str:
        if not raw_path: return ""
        return os.path.normpath(raw_path).rstrip('/') if raw_path != '/' else '/'

    def _get_message_source(self, event: Any) -> MessageSource:
        source = getattr(event, 'message_source', MessageSource.REALTIME)
        if isinstance(source, str):
            return MessageSource(source)
        return source
