import os
import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import deque, defaultdict
from pathlib import Path
from contextlib import asynccontextmanager

from fustor_event_model.models import MessageSource
from ..config import fusion_config

# Import LogicalClock for hybrid time synchronization
from fustor_common.logical_clock import LogicalClock

logger = logging.getLogger(__name__)

class DirectoryNode:
    """Represents a directory node in the in-memory directory tree."""
    def __init__(self, name: str, path: str, size: int = 0, modified_time: float = 0.0, created_time: float = 0.0):
        self.name = name
        self.path = path
        self.size = size
        self.modified_time = modified_time
        self.created_time = created_time
        self.last_updated_at: float = 0.0 # Logical timestamp when node was last confirmed
        self.children: Dict[str, Any] = {} # Can contain DirectoryNode or FileNode
        # Consistency flags
        self.integrity_suspect: bool = False
        self.audit_skipped: bool = False  # Temporary flag for missing file detection

    def to_dict(self, recursive=True, max_depth=None, only_path=False):
        """Converts the directory node to a dictionary representation."""
        result = {
            'name': self.name,
            'content_type': 'directory',
            'path': self.path
        }
        
        if not only_path:
            result.update({
                'size': self.size,
                'modified_time': self.modified_time,
                'created_time': self.created_time,
                'mtime': self.modified_time,  # Alias for compatibility
                'ctime': self.created_time,   # Alias for compatibility
                'integrity_suspect': self.integrity_suspect
            })

        # Base case for recursion depth
        if max_depth is not None and max_depth == 0:
            return result

        result['children'] = []
        if recursive:
            for child in self.children.values():
                child_dict = child.to_dict(
                    recursive=True, 
                    max_depth=max_depth - 1 if max_depth is not None else None,
                    only_path=only_path
                )
                if child_dict is not None:
                    result['children'].append(child_dict)
        else:
            for child in self.children.values():
                # Non-recursive: get child metadata only
                child_dict = child.to_dict(recursive=False, max_depth=0, only_path=only_path)
                if child_dict is not None:
                    result['children'].append(child_dict)
        
        return result

class FileNode:
    """Represents a file node in the in-memory directory tree."""
    def __init__(self, name: str, path: str, size: int, modified_time: float, created_time: float):
        self.name = name
        self.path = path
        self.size = size
        self.modified_time = modified_time
        self.created_time = created_time
        self.last_updated_at: float = 0.0 # Logical timestamp when node was last confirmed
        # Consistency flags
        self.integrity_suspect: bool = False

    def to_dict(self, recursive=True, max_depth=None, only_path=False):
        """Converts the file node to a dictionary representation."""
        result = {
            'name': self.name,
            'content_type': 'file',
            'path': self.path
        }
        if not only_path:
            result.update({
                'size': self.size,
                'modified_time': self.modified_time,
                'created_time': self.created_time,
                'mtime': self.modified_time,  # Alias for compatibility
                'ctime': self.created_time,   # Alias for compatibility
                'integrity_suspect': self.integrity_suspect
            })
        return result

class DirectoryStructureParser:
    """
    Parses directory structure events and maintains an in-memory tree representation.
    Implements Smart Merge logic for consistency arbitration.
    """
    
    def __init__(self, datastore_id: int):
        self.datastore_id = datastore_id
        self.logger = logging.getLogger(f"fustor_fusion.parser.fs.{datastore_id}")
        self.hot_file_threshold = fusion_config.FUSTOR_FUSION_SUSPECT_TTL_SECONDS
        self._root = DirectoryNode("", "/")
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self._root}
        self._file_path_map: Dict[str, FileNode] = {}
        
        # Concurrency management
        # Use a large semaphore to allow multiple "readers" (row processors) but block for "writers" (global ops)
        self._MAX_READERS = 1000
        self._global_semaphore = asyncio.Semaphore(self._MAX_READERS)
        # Segmented locks for specific path branches (first segment of path)
        self._segment_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        
        self._last_event_latency = 0.0
        self._cache_invalidation_needed = False
        
        # === Consistency State ===
        # Tombstone List: path -> delete_timestamp
        # Prevents deleted files from being resurrected by delayed Snapshot/Audit
        self._tombstone_list: Dict[str, float] = {}
        
        # Suspect List: path -> suspect_until_timestamp (physical time)
        # Marks files that might still be written to
        self._suspect_list: Dict[str, float] = {}
        
        # Audit lifecycle (logical audit start timestamp)
        self._last_audit_start: Optional[float] = None
        
        # Paths seen during current audit cycle (for missing file detection)
        self._audit_seen_paths: Set[str] = set()
        
        # Rate-limiting for suspect cleanup
        self._last_suspect_cleanup_time = 0.0
        self._suspect_cleanup_interval = 0.5 # seconds
        
        # Blind-spot tracking
        # Just a set of paths. Incremental maintenance (audit-seen/realtime removes).
        # Reset ONLY when a new Agent Session connects.
        self._blind_spot_deletions: Set[str] = set()
        self._blind_spot_additions: Set[str] = set()
        self._current_session_id: Optional[str] = None
        
        # Logical clock for hybrid time synchronization
        self._logical_clock = LogicalClock()

    def _check_cache_invalidation(self, path: str):
        """Simple placeholder for more complex logic"""
        pass
    
    @asynccontextmanager
    async def _global_exclusive_lock(self):
        """Context manager to acquire the global semaphore exclusively."""
        # Note: This is a simple way to simulate an exclusive lock using a semaphore.
        # It acquires all permits.
        for _ in range(self._MAX_READERS):
            await self._global_semaphore.acquire()
        try:
            yield
        finally:
            for _ in range(self._MAX_READERS):
                self._global_semaphore.release()

    def _get_segment_lock(self, path: str) -> asyncio.Lock:
        """Get the lock for the top-level segment of the given path."""
        parts = path.strip('/').split('/')
        segment = parts[0] if parts and parts[0] else "/"
        return self._segment_locks[segment]

    def _get_node(self, path: str) -> Optional[Any]:
        """Get a node (file or directory) by path."""
        if path in self._directory_path_map:
            return self._directory_path_map[path]
        return self._file_path_map.get(path)


    async def _process_create_update_in_memory(self, payload: Dict[str, Any], path: str):
        """Update the in-memory tree with create/update event data."""
        # Standardize path
        path = path.rstrip('/') if path != '/' else '/'
        
        # DEBUG LOGGING
        skipped_val = payload.get('audit_skipped', 'NOT_SET')
        if skipped_val != 'NOT_SET':
             self.logger.info(f"DEBUG_UPDATE: path={path} has audit_skipped={skipped_val} value_type={type(skipped_val)} payload_keys={list(payload.keys())}")

        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        
        size = payload.get('size', 0)
        mtime = payload.get('modified_time', 0.0)
        ctime = payload.get('created_time', 0.0)
        is_dir = payload.get('is_dir', False)

        # 1. Ensure parent exists
        if parent_path not in self._directory_path_map and path != '/':
            # Auto-create parent nodes if they don't exist
            current_path = ""
            parts = parent_path.strip('/').split('/')
            parent_node = self._root
            for part in parts:
                if not part: continue
                current_path += "/" + part
                if current_path not in self._directory_path_map:
                    new_dir = DirectoryNode(part, current_path)
                    new_dir.last_updated_at = self._logical_clock.get_watermark()
                    parent_node.children[part] = new_dir
                    self._directory_path_map[current_path] = new_dir
                parent_node = self._directory_path_map[current_path]
        
        # 2. Update current node
        if is_dir:
            if path in self._directory_path_map:
                node = self._directory_path_map[path]
                node.size = size
                node.modified_time = mtime
                node.created_time = ctime
                audit_skipped_value = payload.get('audit_skipped', False)
                node.audit_skipped = audit_skipped_value
                if audit_skipped_value:
                    logger.info(f"Set audit_skipped=True for existing directory: {path}")
            else:
                node = DirectoryNode(name, path, size, mtime, ctime)
                audit_skipped_value = payload.get('audit_skipped', False)
                node.audit_skipped = audit_skipped_value
                if audit_skipped_value:
                    logger.info(f"Set audit_skipped=True for NEW directory: {path}")
                self._directory_path_map[path] = node
                if path != '/':
                    parent_node = self._directory_path_map.get(parent_path)
                    if parent_node:
                        parent_node.children[name] = node
            
            node.last_updated_at = self._logical_clock.get_watermark()

        else:
            if path in self._file_path_map:
                node = self._file_path_map[path]
                node.size = size
                node.modified_time = mtime
                node.created_time = ctime
            else:
                node = FileNode(name, path, size, mtime, ctime)
                self._file_path_map[path] = node
                parent_node = self._directory_path_map.get(parent_path)
                if parent_node:
                    parent_node.children[name] = node
            
            node.last_updated_at = self._logical_clock.get_watermark()

    async def _process_delete_in_memory(self, path: str):
        """Remove a node from the in-memory tree."""
        path = path.rstrip('/') if path != '/' else '/'
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)

        if path in self._directory_path_map:
            # Recursive deletion from maps
            stack = [self._directory_path_map[path]]
            while stack:
                curr = stack.pop()
                if curr.path in self._directory_path_map:
                    del self._directory_path_map[curr.path]
                for child in curr.children.values():
                    if isinstance(child, DirectoryNode):
                        stack.append(child)
                    elif isinstance(child, FileNode):
                        if child.path in self._file_path_map:
                            del self._file_path_map[child.path]
            
            # Remove from parent's children
            parent = self._directory_path_map.get(parent_path)
            if parent and name in parent.children:
                del parent.children[name]

        elif path in self._file_path_map:
            del self._file_path_map[path]
            parent = self._directory_path_map.get(parent_path)
            if parent and name in parent.children:
                del parent.children[name]

    async def process_event(self, event: Any) -> bool:
        """ 
        Processes an event using Smart Merge logic.
        
        Arbitration rules:
        1. Realtime events have highest priority - always applied
        2. Snapshot/Audit events are filtered:
           - Tombstone check: skip if file was deleted by Realtime
           - Mtime check: skip if existing data is newer
        """
        if not event.rows:
            return False

        async with self._global_semaphore:
            if event.index > 0:
                # Use logical clock (observation watermark) for latency calculation
                logical_now_ms = self._logical_clock.get_watermark() * 1000
                self._last_event_latency = max(0, logical_now_ms - event.index)
            
            from fustor_event_model.models import EventType
            event_type = event.event_type
            
            # Get message source (default to REALTIME for backward compatibility)
            message_source = getattr(event, 'message_source', MessageSource.REALTIME)
            if isinstance(message_source, str):
                message_source = MessageSource(message_source)
            
            is_realtime = (message_source == MessageSource.REALTIME)
            is_audit = (message_source == MessageSource.AUDIT)
            
            # Auto-detect audit start time
            if is_audit and self._last_audit_start is None and event.index > 0:
                self._logical_clock.update(event.index / 1000.0)
                self._last_audit_start = self._logical_clock.get_watermark()
                self.logger.info(f"Auto-detected Audit Start logical time: {self._last_audit_start} from event index {event.index}")
    
            # Session Change Detection
            session_id = getattr(event, "session_id", None)
            if session_id and session_id != self._current_session_id:
                if self._current_session_id is not None:
                    self.logger.info(f"New Agent Session detected: {session_id} (was {self._current_session_id}). Resetting Blind Spot List.")
                    self._blind_spot_deletions.clear()
                    self._blind_spot_additions.clear()
                self._current_session_id = session_id

    
            self.logger.debug(f"Parser processing {len(event.rows)} rows from {message_source} (datastore {self.datastore_id})")
    
            rows_processed = 0
            for payload in event.rows:
                path = payload.get('path') or payload.get('file_path')
                if not path:
                    continue
                
                # Yield occasionally
                rows_processed += 1
                if rows_processed % 100 == 0:
                    await asyncio.sleep(0)

                # Use segmented lock
                async with self._get_segment_lock(path):
                    # Normalize path
                    path = path.rstrip('/') if path != '/' else '/'
                    
                    self.logger.debug(f"Processing {event_type} for {path} from {message_source}")

                    self._check_cache_invalidation(path)
                    mtime = payload.get('modified_time', 0.0)
                    
                    # Update logical clock
                    if event.index > 0:
                        self._logical_clock.update(event.index / 1000.0)
                    if mtime is not None:
                        self._logical_clock.update(mtime)
                    
                    # Track paths seen during audit
                    if is_audit:
                        self._audit_seen_paths.add(path)
                        self._blind_spot_deletions.discard(path)
                    
                    if event_type == EventType.DELETE:
                        if is_realtime:
                            await self._process_delete_in_memory(path)
                            ts = self._logical_clock.get_watermark()
                            self._tombstone_list[path] = ts
                            self.logger.info(f"Tombstone CREATED for {path} at {ts} via Realtime DELETE")
                            self._suspect_list.pop(path, None)
                            self._blind_spot_deletions.discard(path)
                            self._blind_spot_additions.discard(path)
                        else:
                            if path not in self._tombstone_list:
                                await self._process_delete_in_memory(path)
                                self._blind_spot_deletions.discard(path)
                                self._blind_spot_additions.discard(path)
                                
                    elif event_type in [EventType.INSERT, EventType.UPDATE]:
                        if is_realtime:
                            await self._process_create_update_in_memory(payload, path)
                            self._tombstone_list.pop(path, None)
                            self._suspect_list.pop(path, None)
                            self._blind_spot_deletions.discard(path)
                            self._blind_spot_additions.discard(path)
                            
                            node = self._get_node(path)
                            if node:
                                node.integrity_suspect = False
                        else:
                            # Rule 1: Tombstone check
                            if path in self._tombstone_list:
                                tombstone_ts = self._tombstone_list[path]
                                if mtime is None:
                                    continue
                                if mtime > tombstone_ts:
                                    self._tombstone_list.pop(path)
                                else:
                                    continue
                            
                            # Rule 2: Mtime check
                            existing = self._get_node(path)
                            old_mtime = None
                            is_audit_skip = is_audit and payload.get('audit_skipped') is True
                            
                            if existing:
                                old_mtime = existing.modified_time
                                if not is_audit_skip and old_mtime is not None and mtime is not None and old_mtime >= mtime:
                                    continue
                            
                            # Rule 3 (Audit only): Parent Mtime Check
                            if is_audit and existing is None:
                                parent_path = payload.get('parent_path')
                                parent_mtime_from_audit = payload.get('parent_mtime')
                                memory_parent = self._directory_path_map.get(parent_path)
                                if memory_parent and memory_parent.modified_time is not None and parent_mtime_from_audit is not None and memory_parent.modified_time > parent_mtime_from_audit:
                                    continue
                            
                            # Apply update
                            await self._process_create_update_in_memory(payload, path)
                            
                            # Update node flags
                            node = self._get_node(path)
                            if node:
                                logical_now = self._logical_clock.get_watermark()
                                if (logical_now - mtime) < self.hot_file_threshold:
                                    node.integrity_suspect = True
                                    # Use monotonic time for duration-based expiry
                                    self._suspect_list[path] = time.monotonic() + self.hot_file_threshold
                                
                                # Blind-spot tracking
                                if is_audit:
                                    self._blind_spot_additions.add(path)
                                    
            return True
    
    async def cleanup_expired_suspects(self):
        """Public method to trigger periodic cleanup."""
        async with self._global_semaphore:
            self._cleanup_expired_suspects_unlocked()
    
    async def handle_audit_start(self):
        """
        Called when an Audit cycle begins.
        Per Section 4.4: Clear Blind-spot List at start of each audit.
        """
        async with self._global_exclusive_lock():
            # Check if we are already in the middle of this audit (via auto-detection from process_event)
            # If so, do NOT clear flags for files already seen.
            now = self._logical_clock.get_watermark()
            is_late_start = False
            
            # Using a 5-second window to detect race between first event and audit-start signal
            if self._last_audit_start is not None and (now - self._last_audit_start) < 5.0 and self._audit_seen_paths:
                self.logger.info(f"Audit Start signal received late (last_start={self._last_audit_start}, now={now}). Preserving observed flags.")
                is_late_start = True
            
            # Use logical clock for audit start to stay synchronized with events
            self._last_audit_start = now
            
            # Per Design Section 4.4: Blind-spot list persists across audits.
            # Flags are only cleared by Realtime events.
            # Audit events will re-confirm (set to True) or add new blind spots.
            
            # Clear paths seen tracker for missing file detection
            if is_late_start:
                 # If late start, we preserve audit_seen_paths to avoid false positives in missing detection
                 self.logger.info("Preserving audit_seen_paths due to Late Start.")
            else:
                 self._audit_seen_paths.clear()
            
            # DO NOT clear blind-spot lists here.
            # We want to persist additions and deletions if subsequent audits skip the directory due to NFS caching.
            # Additions and deletions will be removed incrementally in process_event if the file is seen again.
            # self._blind_spot_deletions.clear()
            # self._blind_spot_additions.clear()
            
            self.logger.info(f"Audit started for datastore {self.datastore_id}. Flags preserved. late_start={is_late_start}")
    
    async def handle_audit_end(self):
        """
        Called when an Audit cycle ends.
        1. Cleans up Tombstones created before this audit started.
        2. Detects missing files (Section 5.3 Scenario 2) using optimized Scanned-Directory check.
        """
        async with self._global_exclusive_lock():
            if self._last_audit_start is None:
                return
            
            # 1. Tombstone cleanup (TTL-based)
            # We must persist tombstones even if Audit confirms file is missing,
            # to protect against 'zombie' resurrection from blind spots in subsequent cycles.
            # Only remove if TTL expired.
            
            
            tombstone_ttl = 3600.0 # Default 1 hour (TODO: Make configurable via fusion_config)
            now = self._logical_clock.get_watermark()
            before_count = len(self._tombstone_list)
            old_tombstones = list(self._tombstone_list.keys())
            
            # Keep tombstone if it is within TTL
            # (Note: self._tombstone_list values are logical timestamps. Comparing with 'now' is correct.)
            self._tombstone_list = {
                path: ts for path, ts in self._tombstone_list.items()
                if (now - ts) < tombstone_ttl
            }
            
            tombstones_cleaned = before_count - len(self._tombstone_list)
            if tombstones_cleaned > 0:
                dropped = [p for p in old_tombstones if p not in self._tombstone_list]
                self.logger.info(f"Tombstone CLEANUP: removed {tombstones_cleaned} items (TTL expired). Removed: {dropped}")
            
            # 2. Optimized Missing File Detection
            # Instead of iterating ALL files to find what's missing (O(N)),
            # we iterate only directories that were ACTIVELY SCANNED in this audit.
            # If a directory was scanned (not skipped), any child in memory but not in audit is deleted.
            missing_detected = 0
            paths_to_delete = []

            if self._audit_seen_paths:
                # Iterate over all paths seen in this audit
                # We are looking for DIRECTORIES that were fully scanned (not skipped)
                for path in self._audit_seen_paths:
                    dir_node = self._directory_path_map.get(path)
                    
                    if dir_node and not getattr(dir_node, 'audit_skipped', False):
                        # This directory was fully scanned.
                        # Check its children in memory against the audit evidence.
                        for child_name, child_node in list(dir_node.children.items()):
                            if child_node.path not in self._audit_seen_paths:
                                if child_node.path in self._tombstone_list:
                                    continue
                                
                                # Rule 3: Stale Evidence Protection
                                # Only delete if the node hasn't been updated AFTER this audit started.
                                if child_node.last_updated_at > self._last_audit_start:
                                    self.logger.info(f"Preserving node from audit deletion (Stale Evidence): {child_node.path} (last_updated={child_node.last_updated_at} > audit_start={self._last_audit_start})")
                                    continue

                                self.logger.debug(f"Blind-spot deletion detected (Optimized): {child_node.path} (Parent {path} was scanned)")
                                paths_to_delete.append(child_node.path)
            
                # Execute deletions in bulk
                missing_detected = len(paths_to_delete)
                for path in paths_to_delete:
                    await self._process_delete_in_memory(path)
                    self._blind_spot_deletions.add(path)
                    # Clean up from additions list to prevent conflicting state
                    self._blind_spot_additions.discard(path)
            
            self.logger.info(
                f"Audit ended for datastore {self.datastore_id}. "
                f"Tombstones cleaned: {tombstones_cleaned}, Blind-spot deletions: {missing_detected}"
            )
            
            # Reset audit state
            self._last_audit_start = None
            self._audit_seen_paths.clear()
    
    def _cleanup_expired_suspects_unlocked(self):
        """Clean up expired suspects and their flags. Must be called with lock held."""
        if not self._suspect_list:
            return 0
            
        now_monotonic = time.monotonic()
        
        # Rate limit the cleanup logic to avoid excessive CPU usage
        if now_monotonic - self._last_suspect_cleanup_time < self._suspect_cleanup_interval:
            return 0
            
        self._last_suspect_cleanup_time = now_monotonic
        
        expired_paths = [path for path, expires_at in self._suspect_list.items() if expires_at <= now_monotonic]
        
        if expired_paths:
            for path in expired_paths:
                self.logger.info(f"Suspect EXPIRED: {path} (expired at {self._suspect_list[path]})")
                del self._suspect_list[path]
                node = self._get_node(path)
                if node:
                    node.integrity_suspect = False
        
        return len(expired_paths)

    async def get_suspect_list(self) -> Dict[str, float]:
        """Get the current Suspect List for Sentinel Sweep."""
        async with self._global_semaphore:
            return dict(self._suspect_list)
    
    async def update_suspect(self, path: str, new_mtime: float):
        """Update a suspect file's mtime from Sentinel Sweep."""
        async with self._global_semaphore:
            async with self._get_segment_lock(path):
                # Update logical clock with observed mtime
                self._logical_clock.update(new_mtime)
                logical_now = self._logical_clock.get_watermark()
                
                node = self._get_node(path)
                if node:
                    node.modified_time = new_mtime
                    # If file is now cold (using logical clock), remove from suspect list
                    if (logical_now - new_mtime) >= self.hot_file_threshold:
                        node.integrity_suspect = False
                        self._suspect_list.pop(path, None)
                    else:
                        # Still hot, extend suspect window (expiry uses monotonic clock)
                        self._suspect_list[path] = time.monotonic() + self.hot_file_threshold


    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        """Get the tree structure starting from path."""
        async with self._global_semaphore:
            # First check if it's a directory
            node = self._directory_path_map.get(path)
            if node:
                return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
            
            # Then check if it's a file
            node = self._file_path_map.get(path)
            if node:
                return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
                
            return None
    
    async def get_blind_spot_list(self) -> Dict[str, Any]:
        """Get the current Blind-spot List."""
        async with self._global_semaphore:
            # Blind spot files - optimized lookup
            # Iterate through our additions set, but cross-check with file map
            # to ensure we don't return stale paths (e.g. if logic drifted)
            agent_missing_files = []
            
            for path in self._blind_spot_additions:
                node = self._file_path_map.get(path)
                if node:
                    agent_missing_files.append(node.to_dict())
            
            return {
                "additions_count": len(agent_missing_files),
                "additions": agent_missing_files,
                "deletion_count": len(self._blind_spot_deletions),
                "deletions": list(self._blind_spot_deletions)
            }

    async def search_files(self, query: str) -> List[Dict[str, Any]]:
        """Search files by name pattern (placeholder)."""
        results = []
        async with self._global_semaphore:
            for path, node in self._file_path_map.items():
                if query.lower() in node.name.lower():
                    results.append(node.to_dict())
        return results

    async def get_directory_stats(self) -> Dict[str, Any]:
        """Return basic statistics about the parsed data."""
        async with self._global_semaphore:
            # Find the oldest directory
            oldest_dir = None
            if self._directory_path_map:
                # Filter out the root "/" if needed, or include it
                dirs = [d for d in self._directory_path_map.values() if d.path != "/"]
                if dirs:
                    oldest_node = min(dirs, key=lambda x: x.modified_time)
                    oldest_dir = {"path": oldest_node.path, "timestamp": oldest_node.modified_time}

            # Count blind spot files (Section 8: global level indicator)
            blind_spot_files = len(self._blind_spot_additions)
            suspect_files = sum(1 for node in self._file_path_map.values() if node.integrity_suspect)

            return {
                "total_directories": len(self._directory_path_map),
                "total_files": len(self._file_path_map),
                "last_event_latency_ms": self._last_event_latency,
                "oldest_directory": oldest_dir,
                # Section 8: Global level indicators
                "has_blind_spot": blind_spot_files > 0 or len(self._blind_spot_deletions) > 0,
                "blind_spot_file_count": blind_spot_files,
                "blind_spot_deletion_count": len(self._blind_spot_deletions),
                "suspect_file_count": suspect_files,
                # Logical clock for consistent staleness calculation
                "logical_now": self._logical_clock.get_watermark()
            }

    async def reset(self):
        """Clears all in-memory data for this datastore."""
        async with self._global_exclusive_lock():
            self._root = DirectoryNode("", "/")
            self._directory_path_map = {"/": self._root}
            self._file_path_map = {}
            self._tombstone_list = {}
            self._suspect_list = {}
            self._last_audit_start = None
            self._audit_seen_paths.clear()
            self._blind_spot_deletions.clear()
            self.logger.info(f"Parser state reset for datastore {self.datastore_id}")

    async def cleanup_expired_suspects(self):
        """Public method to trigger periodic cleanup."""
        async with self._global_semaphore:
            self._cleanup_expired_suspects_unlocked()