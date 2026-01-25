import os
import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Set
from collections import deque
from pathlib import Path

from fustor_event_model.models import MessageSource
from ..config import fusion_config

logger = logging.getLogger(__name__)

class DirectoryNode:
    """Represents a directory node in the in-memory directory tree."""
    def __init__(self, name: str, path: str, size: int = 0, modified_time: float = 0.0, created_time: float = 0.0):
        self.name = name
        self.path = path
        self.size = size
        self.modified_time = modified_time
        self.created_time = created_time
        self.children: Dict[str, Any] = {} # Can contain DirectoryNode or FileNode
        # Consistency flags
        self.integrity_suspect: bool = False
        self.agent_missing: bool = False
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
                'integrity_suspect': self.integrity_suspect,
                'agent_missing': self.agent_missing
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
        # Consistency flags
        self.integrity_suspect: bool = False
        self.agent_missing: bool = False

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
                'integrity_suspect': self.integrity_suspect,
                'agent_missing': self.agent_missing
            })
        return result

class DirectoryStructureParser:
    """
    Parses directory structure events and maintains an in-memory tree representation.
    Implements Smart Merge logic for consistency arbitration.
    """
    
    @property
    def hot_file_threshold(self) -> int:
        return fusion_config.FUSTOR_FUSION_SUSPECT_TTL_SECONDS
    
    def __init__(self, datastore_id: int):
        self.datastore_id = datastore_id
        self.logger = logging.getLogger(f"fustor_fusion.parser.fs.{datastore_id}")
        self._root = DirectoryNode("", "/")
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self._root}
        self._file_path_map: Dict[str, FileNode] = {}
        self._lock = asyncio.Lock()
        self._last_event_latency = 0.0
        self._cache_invalidation_needed = False
        
        # === Consistency State ===
        # Tombstone List: path -> delete_timestamp
        # Prevents deleted files from being resurrected by delayed Snapshot/Audit
        self._tombstone_list: Dict[str, float] = {}
        
        # Suspect List: path -> suspect_until_timestamp
        # Marks files that might still be written to
        self._suspect_list: Dict[str, float] = {}
        
        # Audit lifecycle
        self._last_audit_start: Optional[float] = None
        
        # Paths seen during current audit cycle (for missing file detection)
        self._audit_seen_paths: Set[str] = set()
        
        # Track blind-spot deletions for global indicator
        self._blind_spot_deletions: Set[str] = set()

    def _check_cache_invalidation(self, path: str):
        """Simple placeholder for more complex logic"""
        pass
    
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
        3. For audit events, files from blind-spot are marked with agent_missing=True
        """
        if event.table == "initial_trigger":
            return True

        if not event.rows:
            return False
        
        now = time.time()
        now_ms = now * 1000
        if event.index > 0:
            self._last_event_latency = max(0, now_ms - event.index)
        
        from fustor_event_model.models import EventType
        event_type = event.event_type
        
        # Get message source (default to REALTIME for backward compatibility)
        message_source = getattr(event, 'message_source', MessageSource.REALTIME)
        if isinstance(message_source, str):
            message_source = MessageSource(message_source)
        
        is_realtime = (message_source == MessageSource.REALTIME)
        is_audit = (message_source == MessageSource.AUDIT)
        
        # Auto-detect audit start time from the first audit event
        if is_audit and self._last_audit_start is None and event.index > 0:
            self._last_audit_start = event.index / 1000.0
            self.logger.info(f"Auto-detected Audit Start time: {self._last_audit_start} from event index {event.index}")

        self.logger.debug(f"Parser processing {len(event.rows)} rows from {message_source} (datastore {self.datastore_id})")

        async with self._lock:
            for payload in event.rows:
                path = payload.get('path') or payload.get('file_path')
                if not path:
                    continue
                    
                self._check_cache_invalidation(path)
                mtime = payload.get('modified_time', 0.0)
                
                # Track paths seen during audit for missing file detection
                if is_audit:
                    self._audit_seen_paths.add(path)
                
                if event_type == EventType.DELETE:
                    if is_realtime:
                        # Realtime Delete: unconditionally delete + add to Tombstone
                        await self._process_delete_in_memory(path)
                        self._tombstone_list[path] = now
                        self._suspect_list.pop(path, None)
                    else:
                        # Snapshot/Audit Delete: check Tombstone
                        if path not in self._tombstone_list:
                            await self._process_delete_in_memory(path)
                            
                elif event_type in [EventType.INSERT, EventType.UPDATE]:
                    if is_realtime:
                        # Realtime Update: unconditionally update + remove from Tombstone
                        await self._process_create_update_in_memory(payload, path)
                        self._tombstone_list.pop(path, None)
                        self._suspect_list.pop(path, None)
                        
                        # Update node flags
                        node = self._get_node(path)
                        if node:
                            node.integrity_suspect = False
                            node.agent_missing = False
                    else:
                        # Snapshot/Audit: apply arbitration
                        
                        # Rule 1: Tombstone check - skip resurrecting deleted files
                        if path in self._tombstone_list:
                            self.logger.debug(f"Skipping {path}: in Tombstone list")
                            continue
                        
                        # Rule 2: Mtime check - skip if existing data is newer
                        # EXCEPTION: If it's an audit event with audit_skipped=True, we MUST process it
                        # to record the skip state, even if mtime is the same.
                        existing = self._get_node(path)
                        old_mtime = None
                        is_audit_skip = is_audit and payload.get('audit_skipped') is True
                        
                        if existing:
                            old_mtime = existing.modified_time
                            self.logger.info(f"Arbitration CHECK {path}: source={message_source} incoming_mtime={mtime} existing_mtime={old_mtime} is_skip={is_audit_skip}")
                            if not is_audit_skip and old_mtime is not None and mtime is not None and old_mtime >= mtime:
                                self.logger.debug(f"Skipping {path}: existing mtime {old_mtime} >= incoming {mtime}")
                                continue
                        
                        # Rule 3 (Audit only): Parent Mtime Check - Section 5.3 of CONSISTENCY_DESIGN
                        # If file is NEW (not in memory tree) and parent mtime from audit is older
                        # than our memory's parent mtime, discard (parent was updated after audit scan)
                        if is_audit and existing is None:
                            parent_path = payload.get('parent_path')
                            parent_mtime_from_audit = payload.get('parent_mtime')
                            
                            memory_parent = self._directory_path_map.get(parent_path)
                            if memory_parent and memory_parent.modified_time is not None and parent_mtime_from_audit is not None and memory_parent.modified_time > parent_mtime_from_audit:
                                self.logger.warning(
                                    f"Arbitration SKIP {path}: parent {parent_path} mtime in memory "
                                    f"({memory_parent.modified_time}) > audit ({parent_mtime_from_audit})"
                                )
                                continue
                        
                        # Apply the update
                        await self._process_create_update_in_memory(payload, path)
                        
                        # Update consistency flags
                        node = self._get_node(path)
                        if node:
                            # Mark as suspect if hot file
                            if (now - mtime) < self.hot_file_threshold:
                                node.integrity_suspect = True
                                self._suspect_list[path] = now + self.hot_file_threshold
                            
                            # Mark as blind-spot file if from audit AND (it's new OR mtime changed)
                            if is_audit:
                                if existing is None:
                                    self.logger.info(f"Audit ADDED NEW file {path}, marking agent_missing=True")
                                    node.agent_missing = True
                                elif old_mtime is not None and mtime > old_mtime:
                                    self.logger.info(f"Audit UPDATED file {path} (mtime {old_mtime} -> {mtime}), marking agent_missing=True")
                                    node.agent_missing = True
                                else:
                                    self.logger.info(f"Audit SEE EXISTING file {path}, keeping node")
                                    node.agent_missing = True # Ensure it stays in blind-spot list if seen in audit
                            
        return True
    
    async def handle_audit_start(self):
        """
        Called when an Audit cycle begins.
        Per Section 4.4: Clear Blind-spot List at start of each audit.
        """
        async with self._lock:
            self._last_audit_start = time.time()
            
            # Clear all agent_missing flags - they will be re-set during this audit
            for node in self._file_path_map.values():
                node.agent_missing = False
            for node in self._directory_path_map.values():
                node.agent_missing = False
                node.audit_skipped = False
            
            # Clear paths seen tracker for missing file detection
            self._audit_seen_paths.clear()
            
            # Clear blind-spot deletions list for the new cycle
            self._blind_spot_deletions.clear()
            
            self.logger.info(f"Audit started for datastore {self.datastore_id}, cleared blind-spot flags and deletions")
    
    async def handle_audit_end(self):
        """
        Called when an Audit cycle ends.
        1. Cleans up Tombstones created before this audit started.
        2. Detects missing files (Section 5.3 Scenario 2).
        """
        async with self._lock:
            if self._last_audit_start is None:
                return
            
            # 1. Tombstone cleanup
            cutoff = self._last_audit_start
            before_count = len(self._tombstone_list)
            self._tombstone_list = {
                path: ts for path, ts in self._tombstone_list.items()
                if ts > cutoff
            }
            tombstones_cleaned = before_count - len(self._tombstone_list)
            
            # 2. Missing file detection (Section 5.3 Scenario 2)
            # Files in memory but NOT seen during audit = potentially deleted in blind-spot
            # Only check if we have audit seen paths (meaning audit actually ran)
            missing_detected = 0
            if self._audit_seen_paths:
                current_files = set(self._file_path_map.keys())
                missing_in_audit = current_files - self._audit_seen_paths
                
                # Collect paths to delete (can't modify dict while iterating)
                paths_to_delete = []
                
                for missing_path in missing_in_audit:
                    self.logger.debug(f"Checking potential missing file: {missing_path}")
                    # Skip files in Tombstone (already marked as deleted by realtime)
                    if missing_path in self._tombstone_list:
                        continue
                    
                    # Get parent directory to check mtime
                    parent_path = os.path.dirname(missing_path)
                    parent_node = self._directory_path_map.get(parent_path)
                    
                    # If parent was skipped in this audit, we can't conclude it's missing
                    parent_skipped = getattr(parent_node, 'audit_skipped', False) if parent_node else False

                    if parent_node and parent_skipped:
                        self.logger.debug(f"Missing file {missing_path} ignored: parent {parent_path} was skipped in audit")
                        continue
                    
                    # If parent wasn't scanned at all in this audit, we can't conclude anything
                    # (the audit might have been partial)
                    if parent_path not in self._audit_seen_paths and parent_path != "/":
                        self.logger.debug(f"Missing file {missing_path} ignored: parent {parent_path} not seen in audit")
                        continue
                    
                    # Section 5.3 Scenario 2: Delete from memory tree + mark as blind-spot
                    paths_to_delete.append(missing_path)
                    missing_detected += 1
                    self.logger.debug(f"Blind-spot deletion detected: {missing_path}")
                
                # Execute deletions
                for path in paths_to_delete:
                    await self._process_delete_in_memory(path)
                    # Add to a special "blind-spot deletions" tracking
                    self._blind_spot_deletions.add(path)
            
            self.logger.info(
                f"Audit ended for datastore {self.datastore_id}. "
                f"Tombstones cleaned: {tombstones_cleaned}, Blind-spot deletions: {missing_detected}"
            )
            
            # Reset audit state
            self._last_audit_start = None
            self._audit_seen_paths.clear()
    
    def _cleanup_expired_suspects_unlocked(self):
        """Clean up expired suspects and their flags. Must be called with lock held."""
        now = time.time()
        if self._suspect_list:
             self.logger.info(f"Suspect cleanup check: now={now} count={len(self._suspect_list)} items={self._suspect_list}")
        
        expired_paths = [path for path, ts in self._suspect_list.items() if ts <= now]
        
        for path in expired_paths:
            self.logger.info(f"Suspect EXPIRED: {path} (expired at {self._suspect_list[path]})")
            del self._suspect_list[path]
            node = self._get_node(path)
            if node:
                node.integrity_suspect = False
        
        return len(expired_paths)

    async def get_suspect_list(self) -> Dict[str, float]:
        """Get the current Suspect List for Sentinel Sweep."""
        async with self._lock:
            self._cleanup_expired_suspects_unlocked()
            return dict(self._suspect_list)
    
    async def update_suspect(self, path: str, new_mtime: float):
        """Update a suspect file's mtime from Sentinel Sweep."""
        async with self._lock:
            now = time.time()
            node = self._get_node(path)
            if node:
                node.modified_time = new_mtime
                # If file is now cold, remove from suspect list
                if (now - new_mtime) >= self.hot_file_threshold:
                    node.integrity_suspect = False
                    self._suspect_list.pop(path, None)
                else:
                    # Still hot, extend suspect window
                    self._suspect_list[path] = now + self.hot_file_threshold


    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        """Get the tree structure starting from path."""
        async with self._lock:
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
        async with self._lock:
            # Files with agent_missing flag
            agent_missing_files = [
                node.to_dict() for node in self._file_path_map.values() 
                if node.agent_missing
            ]
            
            return {
                "datastore_id": self.datastore_id,
                "agent_missing_count": len(agent_missing_files),
                "agent_missing_files": agent_missing_files,
                "deletion_count": len(self._blind_spot_deletions),
                "deletions": list(self._blind_spot_deletions)
            }

    async def search_files(self, query: str) -> List[Dict[str, Any]]:
        """Search files by name pattern (placeholder)."""
        results = []
        async with self._lock:
            for path, node in self._file_path_map.items():
                if query.lower() in node.name.lower():
                    results.append(node.to_dict())
        return results

    async def get_directory_stats(self) -> Dict[str, Any]:
        """Return basic statistics about the parsed data."""
        async with self._lock:
            # Find the oldest directory
            oldest_dir = None
            if self._directory_path_map:
                # Filter out the root "/" if needed, or include it
                dirs = [d for d in self._directory_path_map.values() if d.path != "/"]
                if dirs:
                    oldest_node = min(dirs, key=lambda x: x.modified_time)
                    oldest_dir = {"path": oldest_node.path, "timestamp": oldest_node.modified_time}

            # Count files with agent_missing flag (Section 8: global level indicator)
            blind_spot_files = sum(1 for node in self._file_path_map.values() if node.agent_missing)
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
                "suspect_file_count": suspect_files
            }

    async def reset(self):
        """Clears all in-memory data for this datastore."""
        async with self._lock:
            self._root = DirectoryNode("", "/")
            self._directory_path_map = {"/": self._root}
            self._file_path_map = {}
            self._tombstone_list = {}
            self._suspect_list = {}
            self._last_audit_start = None
            self._audit_seen_paths.clear()
            self._blind_spot_deletions.clear()
            self.logger.info(f"Parser state reset for datastore {self.datastore_id}")