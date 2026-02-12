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
        source_val = message_source.value if hasattr(message_source, 'value') else str(message_source)
        
        is_realtime = (source_val == MessageSource.REALTIME.value)
        is_audit = (source_val == MessageSource.AUDIT.value)
        is_snapshot = (source_val == MessageSource.SNAPSHOT.value)
        is_on_demand = (source_val == MessageSource.ON_DEMAND_JOB.value)
        
        # Compensation sources are those that find data already on disk (not live events)
        is_compensation = is_audit or is_snapshot or is_on_demand
        
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
            
            self.state.logical_clock.update(mtime, can_sample_skew=is_realtime)
            
            # Update latency (Lag) based on current watermark
            watermark = self.state.logical_clock.get_watermark()
            effective_mtime_for_lag = mtime if mtime is not None else watermark
            self.state.last_event_latency = max(0.0, (watermark - effective_mtime_for_lag) * 1000.0)
            
            if is_audit:
                self.state.audit_seen_paths.add(path)
                # Only discard from blind_spot_deletions when audit "sees" the file (upsert)
                # Audit DELETE should NOT clear blind_spot_deletions (Spec §4.4)
                if event.event_type != EventType.DELETE:
                    self.state.blind_spot_deletions.discard(path)

            if event.event_type == EventType.DELETE:
                await self._handle_delete(path, is_realtime, mtime)
            elif event.event_type in [EventType.INSERT, EventType.UPDATE]:
                await self._handle_upsert(path, payload, event, message_source, mtime, watermark)
        
        return True

    async def _handle_delete(self, path: str, is_realtime: bool, mtime: Optional[float]):
        if is_realtime:
            await self.tree_manager.delete_node(path)
            
            # Watermark has been updated in the caller loop using logical_clock.update()
            logical_ts = self.state.logical_clock.get_watermark()
            physical_ts = time.time()
            self.state.tombstone_list[path] = (logical_ts, physical_ts)
            
            self.state.suspect_list.pop(path, None)
            alt_path = path[1:] if path.startswith('/') else '/' + path
            self.state.suspect_list.pop(alt_path, None)
            
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
        else:
            # Audit/Snapshot/OnDemand find delete logic
            if path in self.state.tombstone_list:
                return
            
            existing = self.state.get_node(path)
            if existing and mtime is not None:
                if existing.modified_time > mtime:
                    return

            await self.tree_manager.delete_node(path)
            self.state.blind_spot_deletions.add(path)
            self.state.suspect_list.pop(path, None)
            self.state.blind_spot_additions.discard(path)

    async def _handle_upsert(self, path: str, payload: Dict, event: Any, source: MessageSource, mtime: float, watermark: float):
        source_val = source.value if hasattr(source, 'value') else str(source)
        
        # 1. Tombstone Protection (same-domain: both mtime and tombstone_ts are in NFS time)
        if path in self.state.tombstone_list:
            tombstone_ts, _ = self.state.tombstone_list[path]
            
            # Single-condition reincarnation: mtime > tombstone_ts (Spec §5.2)
            if mtime > (tombstone_ts + self.TOMBSTONE_EPSILON):
                self.state.tombstone_list.pop(path, None)
            else:
                return

        # 2. Smart Merge Arbitration
        existing = self.state.get_node(path)
        is_realtime = (source_val == MessageSource.REALTIME.value)
        is_snapshot = (source_val == MessageSource.SNAPSHOT.value)
        is_compensation = (source_val in (MessageSource.AUDIT.value, MessageSource.SNAPSHOT.value, MessageSource.ON_DEMAND_JOB.value))
        
        if is_compensation:
            # Snapshot/Audit arbitration
            if existing:
                # If mtime is not newer, ignore (unless audit_skipped which we use for heartbeats/protection)
                audit_skipped_in_payload = payload.get('audit_skipped')
                # self.logger.debug(f"[DEBUG_ARB] Checking arbitration for {path}: payload_skipped={audit_skipped_in_payload}, existing_mtime={existing.modified_time}, payload_mtime={mtime}")
                
                if not audit_skipped_in_payload and existing.modified_time >= mtime:
                    return
            
            if source_val == MessageSource.AUDIT.value and existing is None:
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
        
        # Calculate target last_updated_at
        # Only Realtime (inotify/watchdog) events are fresh confirmation.
        # Snapshot/Audit/On-demand are compensatory and should NOT update this timestamp
        # to preserve Stale Evidence Protection (see CONSISTENCY_DESIGN.md §4.4).
        is_fresh_confirmation = is_realtime
        final_last_updated_at = time.time() if is_fresh_confirmation else old_last_updated_at

        # Perform the actual update (last_updated_at is set correctly by tree.py)
        await self.tree_manager.update_node(payload, path, last_updated_at=final_last_updated_at)
        node = self.state.get_node(path)
        if not node: return

        # 3. Blind Spot and Suspect Management
        # === Authority Model (see CONSISTENCY_DESIGN.md §4.5) ===
        # Only Realtime (Tier 1) events can clear suspects and blind-spots because:
        # - Realtime events are causal: kernel inotify/watchdog fires BECAUSE the change happened
        # - Realtime carries is_atomic_write to distinguish partial (Modify) from complete (Close) writes
        # On-demand/Audit/Snapshot are observational (stat()-based) and CANNOT:
        # - Distinguish NFS-synced blind-zone files from locally-monitored files
        # - Determine if a file is mid-write or stable
        if is_realtime:
            # Atomic Write Check:
            is_atomic = payload.get('is_atomic_write', True)
            
            if is_atomic:
                # Clean write (Close/Create) -> Clear suspect
                self.state.suspect_list.pop(path, None)
                node.integrity_suspect = False
            else:
                # Partial write (Modify) -> Mark/Renew suspect
                expiry = time.monotonic() + self.hot_file_threshold
                self.state.suspect_list[path] = (expiry, mtime)
                heapq.heappush(self.state.suspect_heap, (expiry, path))
                node.integrity_suspect = True
            
            self.state.blind_spot_deletions.discard(path)
            self.state.blind_spot_additions.discard(path)
            node.known_by_agent = True
        else:
            # Compensatory path: Snapshot/Audit/On-demand (all stat()-based, Tier 2-3)
            # Same-domain calculation: watermark and mtime are both in NFS time (Spec §5.2)
            watermark = self.state.logical_clock.get_watermark()
            age = watermark - mtime
            
            mtime_changed = (existing is None) or (abs(old_mtime - mtime) > self.FLOAT_EPSILON)
            
            if mtime_changed:
                if is_snapshot:
                    # Snapshot is the Agent's initial known state, not a blind spot
                    node.known_by_agent = True
                    self.state.blind_spot_additions.discard(path)
                else:
                    # Audit/On-demand discovery → blind spot (Spec §4.5 Tier 3)
                    # On-demand stat() cannot prove inotify coverage, so it also creates blind-spots
                    self.state.blind_spot_additions.add(path)
                    node.known_by_agent = False
                
                if age < self.hot_file_threshold:
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
                if is_snapshot:
                    node.known_by_agent = True
                    self.state.blind_spot_additions.discard(path)
                
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
                self.logger.debug(f"Suspect RENEWED (Active): {path}")
            else:
                # Stable! Cool-off complete
                self.logger.debug(f"Suspect EXPIRED (Stable): {path}")
                self.state.suspect_list.pop(path, None)
                node.integrity_suspect = False
            processed += 1
        return processed

    def _normalize_path(self, raw_path: str) -> str:
        if not raw_path: return ""
        # Ensure leading slash
        if not raw_path.startswith('/'):
            raw_path = '/' + raw_path
        return os.path.normpath(raw_path).rstrip('/') if raw_path != '/' else '/'

    def _get_message_source(self, event: Any) -> MessageSource:
        source = getattr(event, 'message_source', MessageSource.REALTIME)
        if isinstance(source, str):
            return MessageSource(source)
        return source
