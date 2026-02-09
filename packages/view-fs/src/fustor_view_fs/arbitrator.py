import asyncio
import os
import time
import heapq
import logging

from typing import Any, Dict, Tuple, Optional
from fustor_core.event import MessageSource, EventType
from .state import FSState
from .tree import TreeManager

logger = logging.getLogger(__name__)

class FSArbitrator:
    """
    Core arbitration logic for FS Views.
    Implements Smart Merge, Tombstone Protection, and Clock Synchronization.
    """
    
    # Heuristics and Constants
    FLOAT_EPSILON = 1e-6             # Tolerance for float equality comparisons
    TOMBSTONE_EPSILON = 1e-5         # Buffer for tombstone expiration comparison
    DEFAULT_CLEANUP_INTERVAL = 0.5   # Seconds between suspect list cleanups

    def __init__(self, state: FSState, tree_manager: TreeManager, hot_file_threshold: float):
        self.state = state
        self.tree_manager = tree_manager
        self.hot_file_threshold = hot_file_threshold
        self.logger = logging.getLogger(f"fustor_fusion.view_fs.arbitrator.{state.view_id}")
        self.suspect_cleanup_interval = self.DEFAULT_CLEANUP_INTERVAL

    async def process_event(self, event: Any) -> bool:
        """Process an event using Smart Merge logic."""
        if not event.rows:
            return False

        message_source = self._get_message_source(event)
        is_realtime = (message_source == MessageSource.REALTIME)
        is_audit = (message_source == MessageSource.AUDIT)
        
        if is_audit and self.state.last_audit_start is None:
            self.state.last_audit_start = time.time()
            self.logger.info(f"Auto-detected Audit Start at local time: {self.state.last_audit_start}")

        
        rows_processed = 0
        for payload in event.rows:
            path = self._normalize_path(payload.get('path') or payload.get('file_path'))
            if not path: continue
            
            rows_processed += 1
            if rows_processed % 100 == 0: await asyncio.sleep(0)

            # Update clock with row mtime. For deletions, mtime might be None.
            mtime = payload.get('modified_time')
            
            # Use Fusion Local Time for Clock Synchronization
            # We explicitly ignore 'event.index' (Agent Time) here to avoid polluting
            # the Global Skew with Agent-specific clock drifts (e.g. faketime).
            # The LogicalClock will calculate skew = FusionTime - mtime.
            # providing a simplified, consistent time base (Reception Time).
            self.state.logical_clock.update(mtime, can_sample_skew=is_realtime)
            
            # Update latency (Lag) based on current watermark
            watermark = self.state.logical_clock.get_watermark()
            effective_mtime_for_lag = mtime if mtime is not None else watermark
            self.state.last_event_latency = max(0.0, (watermark - effective_mtime_for_lag) * 1000.0)
            
            if is_audit:
                self.state.audit_seen_paths.add(path)
                self.state.blind_spot_deletions.discard(path)

            if event.event_type == EventType.DELETE:
                await self._handle_delete(path, is_realtime, mtime)
            elif event.event_type in [EventType.INSERT, EventType.UPDATE]:
                await self._handle_upsert(path, payload, event, message_source, mtime, watermark)
        
        return True

    async def _handle_delete(self, path: str, is_realtime: bool, mtime: Optional[float]):
        self.logger.debug(f"DELETE_EVENT for {path} realtime={is_realtime}")
        if is_realtime:
            await self.tree_manager.delete_node(path)
            
            # Watermark has been updated in the caller loop using logical_clock.update()
            logical_ts = self.state.logical_clock.get_watermark()
            physical_ts = time.time()
            self.state.tombstone_list[path] = (logical_ts, physical_ts)
            self.logger.info(f"Tombstone CREATED for {path} (Logical: {logical_ts}, Physical: {physical_ts})")
            
            self.state.suspect_list.pop(path, None)
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
        else:
            # Audit/Snapshot delete logic
            if path in self.state.tombstone_list:
                self.logger.debug(f"AUDIT_DELETE_BLOCKED_BY_TOMBSTONE for {path}")
                return
            
            existing = self.state.get_node(path)
            if existing and mtime is not None:
                if existing.modified_time > mtime:
                    self.logger.info(f"AUDIT_DELETE_STALE: Ignoring delete for {path} (Memory: {existing.modified_time} > Audit: {mtime})")
                    return

            await self.tree_manager.delete_node(path)
            self.state.blind_spot_deletions.add(path)
            self.state.blind_spot_additions.discard(path)
        self.logger.debug(f"DELETE_DONE for {path}")

    async def _handle_upsert(self, path: str, payload: Dict, event: Any, source: MessageSource, mtime: float, watermark: float):
        # 1. Tombstone Protection
        if path in self.state.tombstone_list:
            tombstone_ts, _ = self.state.tombstone_list[path] # Use logical timestamp for arbitration
            is_realtime = (source == MessageSource.REALTIME)
            
            # Check if this is "New activity" after deletion
            # Use translated watermark for arbitration instead of raw index
            if mtime > (tombstone_ts + self.TOMBSTONE_EPSILON):
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
        old_last_updated_at = existing.last_updated_at if existing else 0.0
        
        # Perform the actual update
        await self.tree_manager.update_node(payload, path)
        node = self.state.get_node(path)
        if not node: return

        # Fix: Only Realtime events should update last_updated_at (Stale Evidence Protection)
        # Snapshot/Audit events should NOT update this timestamp
        if not is_realtime and old_last_updated_at > 0:
            node.last_updated_at = old_last_updated_at

        # 3. Blind Spot and Suspect Management
        if is_realtime:
            self.logger.debug(f"REALTIME_EVENT for {path}. Current blind_spot_additions: {path in self.state.blind_spot_additions}")
            self.state.suspect_list.pop(path, None)
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
            node.integrity_suspect = False
            node.known_by_agent = True
            self.logger.debug(f"REALTIME_DONE for {path}. Now agent_known={node.known_by_agent}, missing={path in self.state.blind_spot_additions}")
        else:
            # Manage Suspect List (Hot Data)
            # Use Logical Watermark as the stable reference for data age calculations.
            # This follows Spec §6.2 to avoid mixing raw physical clocks that may drift.
            # However, to handle Clock Skew (where Leader is ahead), we also check Physical Age.
            # We derive Physical Age by normalizing Watermark with the Global Skew.
            # This avoids direct reliance on 'time.time()' in favor of the Synchronized Logical Clock.
            watermark = self.state.logical_clock.get_watermark()
            skew = self.state.logical_clock.get_skew()
            
            logical_age = watermark - mtime
            # Physical Watermark = Watermark + Skew (since Skew = Ref - Observed => Ref = Observed + Skew)
            # Actually Skew = Reference(Physical) - Observed(LogicalSource)
            # So Physical = Logical + Skew.
            physical_age = (watermark + skew) - mtime
            
            age = min(logical_age, physical_age)
            mtime_changed = (existing is None) or (abs(old_mtime - mtime) > self.FLOAT_EPSILON)
            
            is_snapshot = (source == MessageSource.SNAPSHOT)
            self.logger.debug(f"NON_REALTIME {path} source={source} mtime_changed={mtime_changed} age={age:.1f}")
            
            if mtime_changed:
                if is_audit or is_snapshot:
                    self.state.blind_spot_additions.add(path)
                    node.known_by_agent = False
                
                if age < self.hot_file_threshold:
                    if not getattr(node, 'known_by_agent', False) or (is_realtime and mtime_changed):
                        node.integrity_suspect = True
                        if path not in self.state.suspect_list:
                            # Cap remaining_life to ensure even future-skewed files 
                            # are checked periodically for stability (Spec §4.3)
                            remaining_life = min(self.hot_file_threshold, self.hot_file_threshold - age)
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
            if abs(curr_expiry - expires_at) > self.FLOAT_EPSILON: continue
            
            node = self.state.get_node(path)
            if not node:
                self.state.suspect_list.pop(path, None)
                continue
            
            # Stability Check: Has mtime changed since we added it to suspect list?
            if abs(node.modified_time - recorded_mtime) > self.FLOAT_EPSILON:
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
