"""
File directory structure parser.
Parses received file metadata events and maintains a real-time directory structure view in memory.
"""
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
import os
import time
from pathlib import Path
import json
from fustor_event_model.models import EventBase, EventType # Added EventBase and EventType import


logger = logging.getLogger(__name__)


@dataclass
class FileNode:
    """Represents a file in the directory structure"""
    path: str
    name: str
    size: int
    modified_time: datetime
    created_time: datetime
    content_type: str = "file"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self, recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Dict[str, Any]:
        result = {
            "path": self.path,
            "name": self.name,
            "size": self.size,
            "content_type": self.content_type,
        }
        if not only_path:
            result.update({
                "modified_time": self.modified_time.isoformat(),
                "created_time": self.created_time.isoformat(),
                "metadata": self.metadata
            })
        return result


@dataclass
class DirectoryNode:
    """Represents a directory in the directory structure"""
    path: str
    name: str
    children: Dict[str, Any] = field(default_factory=dict)
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    subtree_max_mtime: float = 0.0
    
    def add_child(self, name: str, node: Any) -> None:
        """Add a child node (file or directory) to this directory"""
        self.children[name] = node
        # Update directory modification time
        now = datetime.now()
        self.modified_time = now
        # Ensure subtree_max_mtime tracks its own latest modification
        self.subtree_max_mtime = max(self.subtree_max_mtime, now.timestamp())
    
    def remove_child(self, name: str) -> bool:
        """Remove a child node by name"""
        if name in self.children:
            del self.children[name]
            now = datetime.now()
            self.modified_time = now
            # Ensure subtree_max_mtime tracks its own latest modification
            self.subtree_max_mtime = max(self.subtree_max_mtime, now.timestamp())
            return True
        return False
    
    def get_child(self, name: str) -> Optional[Any]:
        """Get a child node by name"""
        return self.children.get(name)
    
    def to_dict(self, recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Dict[str, Any]:
        result = {
            "path": self.path,
            "name": self.name,
            "content_type": "directory",
        }
        
        if not only_path:
            result.update({
                "created_time": self.created_time.isoformat() if self.created_time else None,
                "modified_time": self.modified_time.isoformat() if self.modified_time else None,
                "metadata": self.metadata,
                "subtree_max_mtime": self.subtree_max_mtime
            })
        
        # If max_depth is set and reached 0, stop recursing and don't return children
        if max_depth is not None and max_depth <= 0:
            return result

        if recursive:
            children_dict = {}
            next_max_depth = max_depth - 1 if max_depth is not None else None
            for name, child in self.children.items():
                if isinstance(child, (FileNode, DirectoryNode)):
                    children_dict[name] = child.to_dict(recursive=True, max_depth=next_max_depth, only_path=only_path)
            result["children"] = children_dict
        else:
            # When not recursive, we still return the children keys but with shallow info
            children_list = []
            for name, child in self.children.items():
                if isinstance(child, (FileNode, DirectoryNode)):
                    # Return basic info for children in a list format for flatter retrieval
                    child_info = {
                        "name": child.name,
                        "path": child.path,
                        "content_type": "directory" if isinstance(child, DirectoryNode) else "file",
                    }
                    if not only_path and isinstance(child, FileNode):
                        child_info["size"] = child.size
                    children_list.append(child_info)
            result["children"] = children_list
            
        return result


class DirectoryStructureParser:
    """
    Parses file metadata events and maintains a real-time directory structure view.
    This class keeps track of the entire file system hierarchy based on received events.
    """
    
    def __init__(self, datastore_id: Optional[int] = None):
        self.root = DirectoryNode("/", "/", created_time=datetime.now(), modified_time=datetime.now())
        self._lock = asyncio.Lock() # To protect in-memory operations
        self._file_path_map: Dict[str, FileNode] = {}
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self.root}
        self.logger = logging.getLogger(__name__)
        self.datastore_id = datastore_id
        self._last_event_latency: float = 0.0
        # Cache for the oldest directory: (path, timestamp)
        self._cached_oldest_dir: Optional[Tuple[str, float]] = None

    async def process_event(self, event: EventBase) -> bool:
        """ 
        Processes an event by applying all its data rows to the in-memory cache.
        """
        # 1. Extract information from EventBase
        if event.table == "initial_trigger":
            self.logger.debug(f"Skipping initial trigger event.")
            return True

        if not event.rows:
            self.logger.warning(f"Event has no rows. Skipping. Event: {event}")
            return False
        
        # Calculate processing latency: Now - Event Timestamp (index)
        now_ms = time.time() * 1000
        if event.index > 0:
            self._last_event_latency = max(0, now_ms - event.index)
        
        event_type = event.event_type

        # 2. Apply changes to in-memory cache directly
        async with self._lock:
            for payload in event.rows:
                path = payload.get('path') or payload.get('file_path')
                if not path:
                    continue

                # Check cache invalidation before modification
                self._check_cache_invalidation(path)
                
                if event_type in [EventType.INSERT, EventType.UPDATE]:
                    await self._process_create_update_in_memory(payload, path)
                elif event_type == EventType.DELETE:
                    await self._process_delete_in_memory(path)

        return True

    def _check_cache_invalidation(self, updated_path: str):
        """
        Invalidate the oldest directory cache if the updated path is part of the cached oldest path.
        """
        if self._cached_oldest_dir:
            cached_path = self._cached_oldest_dir[0]
            # If the updated path is a prefix of the cached path (or equal),
            # it means the oldest path (or its parent) is being modified/touched.
            # This makes it potentially 'newer', so we must invalidate.
            
            u_path = updated_path.rstrip('/')
            c_path = cached_path.rstrip('/')
            
            if c_path == u_path or c_path.startswith(u_path + '/'):
                self.logger.debug(f"Invalidating oldest dir cache due to update on {updated_path}")
                self._cached_oldest_dir = None

    def _propagate_mtime_update(self, path: str, timestamp: float):
        """Propagate the modification time up the directory tree."""
        path_obj = Path(path)
        # Start bubbling from the immediate parent
        current_path = str(path_obj.parent)
        if current_path != "/" and current_path.endswith("/"):
            current_path = current_path.rstrip("/")
            
        # The wave of update starts with the child's timestamp
        wave_ts = timestamp

        while True:
            node = self._directory_path_map.get(current_path)
            if node:
                # The directory's subtree max is the maximum of:
                # 1. Its current max (already includes previous bubbles)
                # 2. The incoming wave from a child
                # 3. Its own modified_time (the 'dir.mtime' part)
                own_ts = node.modified_time.timestamp() if node.modified_time else 0
                new_max = max(wave_ts, own_ts)

                if new_max > node.subtree_max_mtime:
                    node.subtree_max_mtime = new_max
                    # The wave continues upwards, now carrying the parent's new max
                    wave_ts = new_max
                else:
                    # Optimization: If this directory is already as new or newer than the wave,
                    # its ancestors must also be, so we can stop.
                    break
            
            if current_path == "/":
                break

            path_obj = Path(current_path)
            current_path = str(path_obj.parent)
            if current_path != "/" and current_path.endswith("/"):
                current_path = current_path.rstrip("/")

    async def _process_create_update_in_memory(self, payload: Dict[str, Any], path: str):
        """Process a create/update operation directly in the in-memory cache."""
        path_obj = Path(path)
        parent_path = str(path_obj.parent)
        if parent_path != "/" and parent_path.endswith("/"):
            parent_path = parent_path.rstrip("/")

        # Determine timestamp for this event
        ts = payload.get('modified_time') or payload.get('created_time') or datetime.now().timestamp()

        await self._ensure_directory_in_memory(parent_path, timestamp=ts)
        parent_node = self._directory_path_map[parent_path]
        
        if payload.get('is_dir'):
            if path not in self._directory_path_map:
                dir_node = DirectoryNode(
                    path=path, name=path_obj.name,
                    created_time=datetime.fromtimestamp(payload.get('created_time', ts)),
                    modified_time=datetime.fromtimestamp(payload.get('modified_time', ts)),
                    metadata=payload.get('metadata', {}),
                    subtree_max_mtime=ts # Initialize with own timestamp
                )
                parent_node.add_child(path_obj.name, dir_node)
                self._directory_path_map[path] = dir_node
            else:
                # Update existing directory mtime
                node = self._directory_path_map[path]
                # Also update the node's own modified_time if payload has it
                node.modified_time = datetime.fromtimestamp(payload.get('modified_time', ts))
                if ts > node.subtree_max_mtime:
                    node.subtree_max_mtime = ts
        else:
            file_node = FileNode(
                path=path, name=path_obj.name, size=payload['size'],
                modified_time=datetime.fromtimestamp(payload.get('modified_time', ts)),
                created_time=datetime.fromtimestamp(payload.get('created_time', ts)),
                content_type="file",
                metadata=payload.get('metadata', {})
            )
            parent_node.add_child(path_obj.name, file_node)
            if path in self._file_path_map:
                del self._file_path_map[path]
            self._file_path_map[path] = file_node

        # Propagate timestamp up the tree
        self._propagate_mtime_update(path, ts)

    async def _ensure_directory_in_memory(self, dir_path: str, timestamp: float = None) -> DirectoryNode:
        """Ensures a directory path exists in the in-memory cache, creating it if necessary."""
        if dir_path in self._directory_path_map:
            return self._directory_path_map[dir_path]

        ts = timestamp if timestamp is not None else datetime.now().timestamp()

        path_obj = Path(dir_path)
        parent_path = str(path_obj.parent)
        if parent_path != "/" and parent_path.endswith("/"):
            parent_path = parent_path.rstrip("/")

        parent_node = await self._ensure_directory_in_memory(parent_path, timestamp=ts)

        new_dir_node = DirectoryNode(
            path=dir_path, 
            name=path_obj.name, 
            created_time=datetime.fromtimestamp(ts),
            modified_time=datetime.fromtimestamp(ts), # Implicit directories inherit child's timestamp
            subtree_max_mtime=ts # Implicit directories inherit child's timestamp
        )
        parent_node.add_child(path_obj.name, new_dir_node)
        self._directory_path_map[dir_path] = new_dir_node
        return new_dir_node

    async def _process_delete_in_memory(self, path: str):
        """Process a delete operation directly in the in-memory cache."""
        path_obj = Path(path)
        parent_path = str(path_obj.parent)
        if parent_path != "/" and parent_path.endswith("/"):
            parent_path = parent_path.rstrip("/")

        parent_node = self._directory_path_map.get(parent_path)
        if not parent_node:
            return # Parent doesn't exist, so child can't either

        # Remove from parent's children
        parent_node.remove_child(path_obj.name)

        # Remove from path maps (recursively for directories)
        if path in self._file_path_map:
            del self._file_path_map[path]
        elif path in self._directory_path_map:
            paths_to_remove = [p for p in self._file_path_map if p.startswith(path + '/')]
            for p in paths_to_remove:
                del self._file_path_map[p]

            dirs_to_remove = [p for p in self._directory_path_map if p.startswith(path + '/')]
            for p in dirs_to_remove:
                del self._directory_path_map[p]

            del self._directory_path_map[path]

    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        """Get the directory structure. If the path is a file, returns the file's metadata."""
        async with self._lock:
            # Normalize the path for lookup: remove trailing slash except for root
            if path != "/" and path.endswith("/"):
                lookup_path = path.rstrip("/")
            else:
                lookup_path = path
            
            # 1. Try to find it as a directory first.
            dir_node = self._directory_path_map.get(lookup_path)
            if dir_node:
                return dir_node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)

            # 2. If not a directory, check if it's a known file.
            file_node = self._file_path_map.get(lookup_path)
            if file_node:
                return file_node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)

            # 3. If it's neither a known directory nor a known file, return None.
            return None
    
    async def get_file_info(self, path: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific file"""
        async with self._lock:
            # Normalize the path for lookup: remove trailing slash except for root
            if path != "/" and path.endswith("/"):
                lookup_path = path.rstrip("/")
            else:
                lookup_path = path
            
            file_node = self._file_path_map.get(lookup_path)
            if file_node:
                return file_node.to_dict(recursive=True)
            return None
    
    async def search_files(self, pattern: str) -> List[Dict[str, Any]]:
        """Search for files matching a pattern"""
        async with self._lock:
            results = []
            for path, file_node in self._file_path_map.items():
                if pattern.lower() in path.lower():
                    results.append(file_node.to_dict(recursive=True))
            return results
    
    async def get_all_files(self) -> List[Dict[str, Any]]:
        """Get all files in the directory structure"""
        async with self._lock:
            return [file_node.to_dict(recursive=True) for file_node in self._file_path_map.values()]
    
    async def get_data_view(self, **kwargs) -> Dict[str, Any]:
        """Get the current data view - default to directory tree from root"""
        path = kwargs.get("path", "/")
        recursive = kwargs.get("recursive", True)
        max_depth = kwargs.get("max_depth")
        only_path = kwargs.get("only_path", False)
        return await self.get_directory_tree(path, recursive=recursive, max_depth=max_depth, only_path=only_path) if path else self.root.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
    
    def _find_oldest_directory_recursive(self, node: DirectoryNode) -> Tuple[Optional[str], float]:
        """
        Recursively find the oldest directory by greedily following the oldest active child path.
        Returns tuple of (path, timestamp).
        """
        # 1. Identify active directory children (ignore files and inactive dirs)
        active_subdirs = [
            child for child in node.children.values() 
            if isinstance(child, DirectoryNode) and child.subtree_max_mtime > 0
        ]
        
        if not active_subdirs:
            # No active subdirectories, so this node is the end of the line
            # Only return it if it has a valid timestamp itself
            if node.subtree_max_mtime > 0:
                return node.path, node.subtree_max_mtime
            return None, 0.0
        
        # 2. Find the child with the oldest subtree timestamp
        oldest_subdir = min(active_subdirs, key=lambda x: x.subtree_max_mtime)
        
        # 3. Recurse down
        return self._find_oldest_directory_recursive(oldest_subdir)

    async def get_directory_stats(self) -> Dict[str, Any]:
        """Get statistics about the directory structure"""
        async with self._lock:
            total_files = len(self._file_path_map)
            total_dirs = len(self._directory_path_map)
            
            # Calculate total size
            total_size = sum(file_node.size for file_node in self._file_path_map.values())
            
            # 1. Latest File Timestamp (Sync Latency)
            # Directly use the root's subtree_max_mtime which bubbled up
            latest_file_time = self.root.subtree_max_mtime
            
            # 2. Oldest Directory (Staleness) - Drill Down Strategy
            oldest_dir_path, oldest_dir_time = self._find_oldest_directory_recursive(self.root)
            
            return {
                "total_files": total_files,
                "total_directories": total_dirs,
                "total_size_bytes": total_size,
                "latest_file_timestamp": latest_file_time if latest_file_time > 0 else None,
                "last_event_latency_ms": self._last_event_latency, 
                "oldest_directory": {
                    "path": oldest_dir_path,
                    "timestamp": oldest_dir_time
                } if oldest_dir_path else None
            }