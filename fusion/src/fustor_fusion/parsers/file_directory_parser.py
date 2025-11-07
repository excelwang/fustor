"""
File directory structure parser.
Parses received file metadata events and maintains a real-time directory structure view in memory.
"""
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
import os
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
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "name": self.name,
            "size": self.size,
            "modified_time": self.modified_time.isoformat(),
            "created_time": self.created_time.isoformat(),
            "content_type": self.content_type,
            "metadata": self.metadata
        }


@dataclass
class DirectoryNode:
    """Represents a directory in the directory structure"""
    path: str
    name: str
    children: Dict[str, Any] = field(default_factory=dict)
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_child(self, name: str, node: Any) -> None:
        """Add a child node (file or directory) to this directory"""
        self.children[name] = node
        # Update directory modification time
        self.modified_time = datetime.now()
    
    def remove_child(self, name: str) -> bool:
        """Remove a child node by name"""
        if name in self.children:
            del self.children[name]
            self.modified_time = datetime.now()
            return True
        return False
    
    def get_child(self, name: str) -> Optional[Any]:
        """Get a child node by name"""
        return self.children.get(name)
    
    def to_dict(self) -> Dict[str, Any]:
        children_dict = {}
        for name, child in self.children.items():
            if isinstance(child, (FileNode, DirectoryNode)):
                children_dict[name] = child.to_dict()
        
        return {
            "path": self.path,
            "name": self.name,
            "content_type": "directory",
            "children": children_dict,
            "created_time": self.created_time.isoformat() if self.created_time else None,
            "modified_time": self.modified_time.isoformat() if self.modified_time else None,
            "metadata": self.metadata
        }


class DirectoryStructureParser:
    """
    Parses file metadata events and maintains a real-time directory structure view.
    This class keeps track of the entire file system hierarchy based on received events.
    """
    
    def __init__(self, datastore_id: Optional[int] = None):
        self.root = DirectoryNode("/", "/", created_time=datetime.now())
        self._lock = asyncio.Lock() # To protect in-memory operations
        self._file_path_map: Dict[str, FileNode] = {}
        self._directory_path_map: Dict[str, DirectoryNode] = {"/": self.root}
        self.logger = logging.getLogger(__name__)
        self.datastore_id = datastore_id

    async def process_event(self, event: EventBase) -> bool:
        """ 
        Processes a single event by applying it directly to the in-memory cache.
        """
        self.logger.info(f"Processing event: {event}")

        # 1. Extract information from EventBase
        if event.table == "initial_trigger":
            self.logger.info(f"Skipping initial trigger event: {event}")
            return True

        if not event.rows:
            self.logger.warning(f"Event has no rows. Skipping. Event: {event}")
            return False
        
        # Assuming one row per event for file system changes
        payload = event.rows[0]
        path = payload.get('path') or payload.get('file_path')
        if not path:
            self.logger.warning(f"Path is missing in event payload. Event: {event}")
            return False

        event_type = event.event_type

        # 2. Apply change to in-memory cache directly
        async with self._lock:
            self.logger.info(f"Applying change to in-memory cache for path: {path}, type: {event_type.value}")
            if event_type in [EventType.INSERT, EventType.UPDATE]:
                await self._process_create_update_in_memory(payload, path)
            elif event_type == EventType.DELETE:
                await self._process_delete_in_memory(path)

        self.logger.info(f"Successfully processed event for path: {path}")
        return True

    def _format_timestamp(self, timestamp: Any) -> str:
        """Safely formats a timestamp into an ISO string."""
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp).isoformat()
        if isinstance(timestamp, str):
            return timestamp # Assuming it's already ISO format
        return datetime.now().isoformat()

    async def _process_create_update_in_memory(self, payload: Dict[str, Any], path: str):
        """Process a create/update operation directly in the in-memory cache."""
        path_obj = Path(path)
        parent_path = str(path_obj.parent)
        if parent_path != "/" and parent_path.endswith("/"):
            parent_path = parent_path.rstrip("/")

        await self._ensure_directory_in_memory(parent_path)
        parent_node = self._directory_path_map[parent_path]

        if payload.get('is_dir'):
            if path not in self._directory_path_map:
                dir_node = DirectoryNode(
                    path=path, name=path_obj.name,
                    created_time=datetime.fromtimestamp(payload['created_time']),
                    metadata=payload.get('metadata', {})
                )
                parent_node.add_child(path_obj.name, dir_node)
                self._directory_path_map[path] = dir_node
        else:
            file_node = FileNode(
                path=path, name=path_obj.name, size=payload['size'],
                modified_time=datetime.fromtimestamp(payload['modified_time']),
                created_time=datetime.fromtimestamp(payload['created_time']),
                content_type="file", # Set content_type to 'file' for files
                metadata=payload.get('metadata', {})
            )
            parent_node.add_child(path_obj.name, file_node)
            # Remove from file map if it exists (in case a directory was converted to file)
            if path in self._file_path_map:
                del self._file_path_map[path]
            self._file_path_map[path] = file_node

    async def _ensure_directory_in_memory(self, dir_path: str) -> DirectoryNode:
        """Ensures a directory path exists in the in-memory cache, creating it if necessary."""
        if dir_path in self._directory_path_map:
            return self._directory_path_map[dir_path]

        path_obj = Path(dir_path)
        parent_path = str(path_obj.parent)
        if parent_path != "/" and parent_path.endswith("/"):
            parent_path = parent_path.rstrip("/")

        parent_node = await self._ensure_directory_in_memory(parent_path)

        new_dir_node = DirectoryNode(path=dir_path, name=path_obj.name, created_time=datetime.now())
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

    async def get_directory_tree(self, path: str = "/") -> Optional[Dict[str, Any]]:
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
                return dir_node.to_dict()

            # 2. If not a directory, check if it's a known file.
            file_node = self._file_path_map.get(lookup_path)
            if file_node:
                return file_node.to_dict()

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
                return file_node.to_dict()
            return None
    
    async def search_files(self, pattern: str) -> List[Dict[str, Any]]:
        """Search for files matching a pattern"""
        async with self._lock:
            results = []
            for path, file_node in self._file_path_map.items():
                if pattern.lower() in path.lower():
                    results.append(file_node.to_dict())
            return results
    
    async def get_all_files(self) -> List[Dict[str, Any]]:
        """Get all files in the directory structure"""
        async with self._lock:
            return [file_node.to_dict() for file_node in self._file_path_map.values()]
    
    async def get_data_view(self, **kwargs) -> Dict[str, Any]:
        """Get the current data view - default to directory tree from root"""
        path = kwargs.get("path", "/")
        return await self.get_directory_tree(path) if path else self.root.to_dict()
    
    async def get_directory_stats(self) -> Dict[str, Any]:
        """Get statistics about the directory structure"""
        async with self._lock:
            total_files = len(self._file_path_map)
            total_dirs = len(self._directory_path_map)
            
            # Calculate total size
            total_size = sum(file_node.size for file_node in self._file_path_map.values())
            
            return {
                "total_files": total_files,
                "total_directories": total_dirs,
                "total_size_bytes": total_size,
                "root": self.root.to_dict()
            }