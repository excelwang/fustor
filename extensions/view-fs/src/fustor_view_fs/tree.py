import os
import logging
import time
from typing import Dict, Any
from .nodes import DirectoryNode, FileNode
from .state import FSState
from fustor_schema_fs.models import FSSchemaFields

logger = logging.getLogger(__name__)

class TreeManager:
    """
    Manages tree mutations and structure for the FS View.
    """
    def __init__(self, state: FSState):
        self.state = state
        self.logger = logging.getLogger(f"fustor_view.fs.tree.{state.view_id}")

    async def update_node(self, payload: Dict[str, Any], path: str, last_updated_at: float = 0.0):
        """Update or create a node in the tree based on event payload."""
        if not path.startswith('/'):
            path = '/' + path
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        parent_path = os.path.normpath(os.path.dirname(path))
        name = os.path.basename(path)
        
        size = payload.get(FSSchemaFields.SIZE, 0)
        mtime = payload.get(FSSchemaFields.MODIFIED_TIME, 0.0)
        ctime = payload.get(FSSchemaFields.CREATED_TIME, 0.0)
        is_dir = payload.get(FSSchemaFields.IS_DIRECTORY, payload.get('is_dir', False))

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
                node.audit_skipped = payload.get(FSSchemaFields.AUDIT_SKIPPED, False)
            else:
                if not self._check_capacity(): return
                node = DirectoryNode(name, path, size, mtime, ctime)
                node.audit_skipped = payload.get(FSSchemaFields.AUDIT_SKIPPED, False)
                self.state.directory_path_map[path] = node
                
            # Ensure parent-child relationship (Fix for potential orphan nodes)
            if path != '/':
                parent_node = self.state.directory_path_map.get(parent_path)
                if parent_node:
                    parent_node.children[name] = node
                    # self.logger.debug(f"Attached directory {name} to parent {parent_path}")
            
            node.last_updated_at = last_updated_at

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
                if not self._check_capacity(): return
                node = FileNode(name, path, size, mtime, ctime)
                self.state.file_path_map[path] = node
            
            # Ensure parent-child relationship (Fix for potential orphan nodes)
            parent_node = self.state.directory_path_map.get(parent_path)
            if parent_node:
                parent_node.children[name] = node
                logger.debug(f"ATTACHED {path} to parent {parent_path}. Parent children count: {len(parent_node.children)}")
            else:
                logger.warning(f"ORPHAN {path} - Parent {parent_path} not found!")
            
            node.last_updated_at = last_updated_at

    def _check_capacity(self) -> bool:
        """Return True if tree has capacity for new nodes."""
        if self.state.max_nodes <= 0: return True
        current = len(self.state.file_path_map) + len(self.state.directory_path_map)
        if current >= self.state.max_nodes:
            # Throttle logging to once per minute
            if time.time() - self.state.last_oom_log > 60:
                self.logger.error(f"OOM Protection: Blocked node creation. Max nodes ({self.state.max_nodes}) reached.")
                self.state.last_oom_log = time.time()
            return False
        return True

    def _ensure_parent_chain(self, parent_path: str):
        parts = [p for p in parent_path.split('/') if p]
        current_path = ""
        parent_node = self.state.directory_path_map["/"]
        watermark = self.state.logical_clock.get_watermark()
        
        for part in parts:
            current_path = os.path.normpath(current_path + "/" + part)
            if current_path not in self.state.directory_path_map:
                if not self._check_capacity(): return
                new_dir = DirectoryNode(part, current_path)
                # Auto-created parents: last_updated_at remains 0.0 (no Stale Evidence Protection)
                # until confirmed by a Realtime event
                parent_node.children[part] = new_dir
                self.state.directory_path_map[current_path] = new_dir
            parent_node = self.state.directory_path_map.get(current_path)
            if not parent_node: return # Stop chain if capacity blocked creation

    async def delete_node(self, path: str):
        """Recursively remove a node from the tree maps and parent children."""
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        
        # Safety: Never delete the root directory
        if path == '/':
            self.logger.warning("Safety check: attempt to delete root directory blocked.")
            return

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
                self.state.suspect_list.pop(curr.path, None)

                if hasattr(self.state, 'blind_spot_additions'):
                    self.state.blind_spot_additions.discard(curr.path)

                for child in curr.children.values():
                    if isinstance(child, DirectoryNode):
                        stack.append(child)
                    else:
                        self.state.file_path_map.pop(child.path, None)
                        self.state.suspect_list.pop(child.path, None) # Clear suspect list
                        if hasattr(self.state, 'blind_spot_additions'):
                            self.state.blind_spot_additions.discard(child.path)

            # Remove from parent's children
            parent = self.state.directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)

        elif file_node:
            self.state.file_path_map.pop(path, None)
            self.state.suspect_list.pop(path, None) # Clear suspect list
            if hasattr(self.state, 'blind_spot_additions'):
                self.state.blind_spot_additions.discard(path)

            parent = self.state.directory_path_map.get(parent_path)
            if parent:
                parent.children.pop(name, None)
