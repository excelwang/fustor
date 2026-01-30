from typing import Dict, Any
import os
from .nodes import DirectoryNode, FileNode
from .base import FSViewBase

class MemoryMixin(FSViewBase):
    async def _process_create_update_in_memory(self, payload: Dict[str, Any], path: str):
        """Update the in-memory tree with create/update event data."""
        path = path.rstrip('/') if path != '/' else '/'
        
        # Aggressive path normalization
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        parent_path = os.path.normpath(os.path.dirname(path))
        name = os.path.basename(path)
        
        size = payload.get('size', 0)
        mtime = payload.get('modified_time', 0.0)
        ctime = payload.get('created_time', 0.0)
        is_dir = payload.get('is_dir', False)

        # 1. Ensure parent exists
        if parent_path not in self._directory_path_map and path != '/':
            current_path = ""
            parts = [p for p in parent_path.split('/') if p]
            parent_node = self._root
            for part in parts:
                current_path = os.path.normpath(current_path + "/" + part)
                if current_path not in self._directory_path_map:
                    new_dir = DirectoryNode(part, current_path)
                    new_dir.last_updated_at = self._logical_clock.get_watermark()
                    parent_node.children[part] = new_dir
                    self._directory_path_map[current_path] = new_dir
                parent_node = self._directory_path_map[current_path]
        
        # 2. Update current node
        if is_dir:
            # Type change protection: if it was a file, remove it first
            if path in self._file_path_map:
                await self._process_delete_in_memory(path)

            if path in self._directory_path_map:
                node = self._directory_path_map[path]
                node.size = size
                node.modified_time = mtime
                node.created_time = ctime
                node.audit_skipped = payload.get('audit_skipped', False)
            else:
                node = DirectoryNode(name, path, size, mtime, ctime)
                node.audit_skipped = payload.get('audit_skipped', False)
                self._directory_path_map[path] = node
                if path != '/':
                    parent_node = self._directory_path_map.get(parent_path)
                    if parent_node:
                        parent_node.children[name] = node
            
            node.last_updated_at = self._logical_clock.get_watermark()

        else:
            # Type change protection: if it was a directory, remove it first
            if path in self._directory_path_map:
                await self._process_delete_in_memory(path)

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
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        parent_path = os.path.normpath(os.path.dirname(path))
        name = os.path.basename(path)

        self.logger.info(f"Memory DELETE: {path} (parent={parent_path}, name={name})")

        dir_node = self._directory_path_map.get(path)
        file_node = self._file_path_map.get(path)

        if dir_node:
            stack = [dir_node]
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
            
            parent = self._directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)

        if file_node:
            del self._file_path_map[path]
            parent = self._directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)
