import asyncio
import os
import time
import heapq
from typing import Any

from fustor_event_model.models import MessageSource, EventType
from .base import FSViewBase

class EventMixin(FSViewBase):
    async def process_event(self, event: Any) -> bool:
        """Processes an event using Smart Merge logic."""
        if not event.rows:
            return False

        async with self._global_semaphore:
            if event.index > 0:
                logical_now_ms = self._logical_clock.get_watermark() * 1000
                self._last_event_latency = max(0, logical_now_ms - event.index)
            
            message_source = getattr(event, 'message_source', MessageSource.REALTIME)
            if isinstance(message_source, str):
                message_source = MessageSource(message_source)
            
            is_realtime = (message_source == MessageSource.REALTIME)
            is_audit = (message_source == MessageSource.AUDIT)
            
            if is_audit and self._last_audit_start is None and event.index > 0:
                self._logical_clock.update(event.index / 1000.0)
                self._last_audit_start = self._logical_clock.get_watermark()
                self.logger.info(f"Auto-detected Audit Start logical time: {self._last_audit_start}")
    
            session_id = getattr(event, "session_id", None)
            if session_id and session_id != self._current_session_id:
                self.logger.info(f"Processing events from session: {session_id}")
                self._current_session_id = session_id

            self.logger.debug(f"Parser processing {len(event.rows)} rows from {message_source}")
    
            rows_processed = 0
            for payload in event.rows:
                raw_path = payload.get('path') or payload.get('file_path')
                if not raw_path:
                    continue
                
                path = os.path.normpath(raw_path).rstrip('/') if raw_path != '/' else '/'
                rows_processed += 1
                if rows_processed % 100 == 0:
                    await asyncio.sleep(0)

                async with self._get_segment_lock(path):
                    self.logger.debug(f"PROCESS_ROW_START: type={event.event_type} source={message_source} path={path}")

                    self._check_cache_invalidation(path)
                    mtime = payload.get('modified_time', 0.0)
                    if mtime is None: mtime = 0.0
                    
                    if event.index > 0:
                        self._logical_clock.update(event.index / 1000.0)
                    if mtime is not None:
                        self._logical_clock.update(mtime)
                    
                    if is_audit:
                        self._audit_seen_paths.add(path)
                        self._blind_spot_deletions.discard(path)
                    
                    if event.event_type == EventType.DELETE:
                        if is_realtime:
                            existing = self._get_node(path)
                            existing_mtime = existing.modified_time if existing else 0.0
                            
                            await self._process_delete_in_memory(path)
                            
                            if event.index > 0:
                                self._logical_clock.update(event.index / 1000.0)
                            self._logical_clock.update(existing_mtime)
                            
                            ts = self._logical_clock.get_watermark()
                            self._tombstone_list[path] = ts
                            self.logger.info(f"Tombstone CREATED for {path} at {ts}")
                            
                            self._suspect_list.pop(path, None)
                            self._blind_spot_deletions.discard(path)
                            self._blind_spot_additions.discard(path)
                        else:
                            if path not in self._tombstone_list:
                                await self._process_delete_in_memory(path)
                                self._blind_spot_deletions.discard(path)
                                self._blind_spot_additions.discard(path)
                                
                    elif event.event_type in [EventType.INSERT, EventType.UPDATE]:
                        if path in self._tombstone_list:
                            tombstone_ts = self._tombstone_list[path]
                            is_new_activity = False
                            if is_realtime:
                                event_ref_ts = (event.index / 1000.0) if event.index > 0 else mtime
                                is_new_activity = (event_ref_ts > (tombstone_ts + 1e-5)) or (mtime > (tombstone_ts + 1e-5))
                            else:
                                is_new_activity = (mtime > (tombstone_ts + 1e-5))
                            
                            if not is_new_activity:
                                self.logger.info(f"TOMBSTONE_BLOCK: {path}")
                                continue
                            else:
                                self.logger.info(f"TOMBSTONE_CLEARED: {path}")
                                self._tombstone_list.pop(path, None)

                        if is_realtime:
                            await self._process_create_update_in_memory(payload, path)
                            self._tombstone_list.pop(path, None) 
                            self._suspect_list.pop(path, None)
                            self._blind_spot_deletions.discard(path)
                            self._blind_spot_additions.discard(path)
                            
                            node = self._get_node(path)
                            if node:
                                node.integrity_suspect = False
                                node.known_by_agent = True
                        else:
                            existing = self._get_node(path)
                            old_mtime = None
                            is_audit_skip = is_audit and payload.get('audit_skipped') is True
                            
                            if existing:
                                old_mtime = existing.modified_time
                                if not is_audit_skip and old_mtime is not None and mtime is not None and old_mtime >= mtime:
                                    continue
                            
                            if is_audit and existing is None:
                                parent_path = payload.get('parent_path')
                                parent_mtime_from_audit = payload.get('parent_mtime')
                                memory_parent = self._directory_path_map.get(parent_path)
                                if memory_parent and memory_parent.modified_time is not None and parent_mtime_from_audit is not None and memory_parent.modified_time > parent_mtime_from_audit:
                                    continue
                            
                            await self._process_create_update_in_memory(payload, path)
                            
                            node = self._get_node(path)
                            if node:
                                age = self._logical_clock.get_watermark() - mtime
                                mtime_eps = 1.001 if is_audit else 1e-6
                                mtime_changed = (old_mtime is None) or (abs(old_mtime - mtime) > mtime_eps)
                                
                                if mtime_changed:
                                    if is_audit:
                                        self._blind_spot_additions.add(path)
                                        node.known_by_agent = False
                                    
                                    if age < self.hot_file_threshold:
                                        if not getattr(node, 'known_by_agent', False) or (is_realtime and mtime_changed):
                                            node.integrity_suspect = True
                                            if path not in self._suspect_list:
                                                remaining_life = self.hot_file_threshold - age
                                                expiry = time.monotonic() + max(0.0, remaining_life)
                                                self._suspect_list[path] = (expiry, mtime)
                                                heapq.heappush(self._suspect_heap, (expiry, path))
                                    else:
                                        node.integrity_suspect = False
                                        self._suspect_list.pop(path, None)
                                else:
                                    # mtime unchanged (Confirmed stable)
                                    # If confirmed cold by logical clock, clear suspect flag immediately
                                    if age >= self.hot_file_threshold:
                                        node.integrity_suspect = False
                                        self._suspect_list.pop(path, None)
                                    else:
                                        # Still hot, maintain current integrity_suspect flag if track exists.
                                        node.integrity_suspect = (path in self._suspect_list)
                                
                                if is_audit and not getattr(node, 'known_by_agent', False):
                                    self._blind_spot_additions.add(path)
            return True
