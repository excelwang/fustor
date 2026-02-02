import os
import logging
import time
from typing import Dict, Any
from .nodes import DirectoryNode, FileNode
from .state import FSState

logger = logging.getLogger(__name__)

class TreeManager:
    """
    Manages tree mutations and structure for the FS View.
    """
    def __init__(self, state: FSState):
        self.state = state
        self.logger = logging.getLogger(f"fustor_view.fs.tree.{state.view_id}")

    async def update_node(self, payload: Dict[str, Any], path: str):
        """Update or create a node in the tree based on event payload."""
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        parent_path = os.path.normpath(os.path.dirname(path))
        name = os.path.basename(path)
        
        size = payload.get('size', 0)
        mtime = payload.get('modified_time', 0.0)
        ctime = payload.get('created_time', 0.0)
        is_dir = payload.get('is_dir', False)

        # 1. Ensure parent exists (Recursive creation if missing)
        if parent_path not in self.state.directory_path_map and path != '/':
            self._ensure_parent_chain(parent_path)
        
        # 2. Update current node
        watermark = self.state.logical_clock.get_watermark()
        
        if is_dir:
            # Type change protection
            if path in self.state.file_path_map:
                await self.delete_node(path)

            if path in self.state.directory_path_map:
                node = self.state.directory_path_map[path]
                node.size = size
                node.modified_time = mtime
                node.created_time = ctime
                node.audit_skipped = payload.get('audit_skipped', False)
            else:
                node = DirectoryNode(name, path, size, mtime, ctime)
                node.audit_skipped = payload.get('audit_skipped', False)
                self.state.directory_path_map[path] = node
                if path != '/':
                    parent_node = self.state.directory_path_map.get(parent_path)
                    if parent_node:
                        parent_node.children[name] = node
            
            node.last_updated_at = time.time()

        else:
            # Type change protection
            if path in self.state.directory_path_map:
                await self.delete_node(path)

            if path in self.state.file_path_map:
                node = self.state.file_path_map[path]
                node.size = size
                node.modified_time = mtime
                node.created_time = ctime
            else:
                node = FileNode(name, path, size, mtime, ctime)
                self.state.file_path_map[path] = node
                parent_node = self.state.directory_path_map.get(parent_path)
                if parent_node:
                    parent_node.children[name] = node
            
            node.last_updated_at = time.time()

    def _ensure_parent_chain(self, parent_path: str):
        parts = [p for p in parent_path.split('/') if p]
        current_path = ""
        parent_node = self.state.directory_path_map["/"]
        watermark = self.state.logical_clock.get_watermark()
        
        for part in parts:
            current_path = os.path.normpath(current_path + "/" + part)
            if current_path not in self.state.directory_path_map:
                new_dir = DirectoryNode(part, current_path)
                new_dir.last_updated_at = time.time()
                parent_node.children[part] = new_dir
                self.state.directory_path_map[current_path] = new_dir
            parent_node = self.state.directory_path_map[current_path]

    async def delete_node(self, path: str):
        """Recursively remove a node from the tree maps and parent children."""
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        parent_path = os.path.normpath(os.path.dirname(path))
        name = os.path.basename(path)

        dir_node = self.state.directory_path_map.get(path)
        file_node = self.state.file_path_map.get(path)

        if dir_node:
            # Recursive map cleanup
            stack = [dir_node]
            while stack:
                curr = stack.pop()
                self.state.directory_path_map.pop(curr.path, None)
                for child in curr.children.values():
                    if isinstance(child, DirectoryNode):
                        stack.append(child)
                    else:
                        self.state.file_path_map.pop(child.path, None)
            
            # Remove from parent's children
            parent = self.state.directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)

        elif file_node:
            self.state.file_path_map.pop(path, None)
            parent = self.state.directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)
