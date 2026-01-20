import os
import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Set
from collections import deque
from pathlib import Path

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
                'created_time': self.created_time
            })

        # Base case for recursion depth
        if max_depth is not None and max_depth <= 0:
            return result

        if recursive:
            result['children'] = {}
            for child_name, child in self.children.items():
                child_dict = child.to_dict(
                    recursive=True, 
                    max_depth=max_depth - 1 if max_depth is not None else None,
                    only_path=only_path
                )
                if child_dict is not None:
                    result['children'][child_name] = child_dict
        else:
            # Non-recursive mode: return children as a LIST of direct metadata
            result['children'] = []
            for child in self.children.values():
                # For non-recursive items, we don't pass recursion or depth down
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
                'created_time': self.created_time
            })
        return result

class DirectoryStructureParser:
    """
    Parses directory structure events and maintains an in-memory tree representation.
    """
    def __init__(self, datastore_id: int):
        self.datastore_id = datastore_id
        self.logger = logging.getLogger(f"fustor_fusion.parser.fs.{datastore_id}")
        self._root = DirectoryNode("", "/")
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self._root}
        self._file_path_map: Dict[str, FileNode] = {}
        self._lock = asyncio.Lock()
        self._last_event_latency = 0.0
        self._cache_invalidation_needed = False

    def _check_cache_invalidation(self, path: str):
        """Simple placeholder for more complex logic"""
        pass

    async def _process_create_update_in_memory(self, payload: Dict[str, Any], path: str):
        """Update the in-memory tree with create/update event data."""
        # Standardize path
        path = path.rstrip('/') if path != '/' else '/'
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
            else:
                node = DirectoryNode(name, path, size, mtime, ctime)
                self._directory_path_map[path] = node
                if path != '/':
                    parent_node = self._directory_path_map.get(parent_path)
                    if parent_node:
                        parent_node.children[name] = node
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
        Processes an event by applying all its data rows to the in-memory cache.
        """
        if event.table == "initial_trigger":
            return True

        if not event.rows:
            return False
        
        now_ms = time.time() * 1000
        if event.index > 0:
            self._last_event_latency = max(0, now_ms - event.index)
        
        from fustor_event_model.models import EventType
        event_type = event.event_type

        async with self._lock:
            for payload in event.rows:
                path = payload.get('path') or payload.get('file_path')
                if not path: continue
                self._check_cache_invalidation(path)
                if event_type in [EventType.INSERT, EventType.UPDATE]:
                    await self._process_create_update_in_memory(payload, path)
                elif event_type == EventType.DELETE:
                    await self._process_delete_in_memory(path)
        return True

    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        """Get the tree structure starting from path."""
        async with self._lock:
            node = self._directory_path_map.get(path)
            if node:
                return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
            return None

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

            return {
                "total_directories": len(self._directory_path_map),
                "total_files": len(self._file_path_map),
                "last_event_latency_ms": self._last_event_latency,
                "oldest_directory": oldest_dir
            }

    async def reset(self):
        """Clears all in-memory data for this datastore."""
        async with self._lock:
            self._root = DirectoryNode("", "/")
            self._directory_path_map = {"/": self._root}
            self._file_path_map = {}
            self.logger.info(f"Parser state reset for datastore {self.datastore_id}")