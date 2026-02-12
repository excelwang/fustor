from typing import Dict, Any, Optional
import os

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
        self.known_by_agent: bool = False # Set to True if seen via Realtime event
        # Lineage info
        self.last_agent_id: Optional[str] = None
        self.source_uri: Optional[str] = None

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
                'integrity_suspect': self.integrity_suspect,
                'last_agent_id': self.last_agent_id,
                'source_uri': self.source_uri
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
            # Non-recursive: return only self, no children
            pass
        
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
        self.known_by_agent: bool = False # Set to True if seen via Realtime event
        # Lineage info
        self.last_agent_id: Optional[str] = None
        self.source_uri: Optional[str] = None

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
                'integrity_suspect': self.integrity_suspect,
                'last_agent_id': self.last_agent_id,
                'source_uri': self.source_uri
            })
        return result
