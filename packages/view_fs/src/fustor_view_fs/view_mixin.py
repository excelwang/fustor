from typing import Dict, List, Optional, Any
import os
from .base import FSViewBase

class ViewMixin(FSViewBase):
    async def get_suspect_list(self) -> Dict[str, float]:
        """Get the current Suspect List for Sentinel Sweep."""
        async with self._global_semaphore:
            return {path: expires_at for path, (expires_at, _) in self._suspect_list.items()}
    
    async def get_directory_tree(self, path: str = "/", recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
        """Get the tree structure starting from path."""
        path = os.path.normpath(path).rstrip('/') if path != '/' else '/'
        async with self._global_semaphore:
            node = self._directory_path_map.get(path)
            if node:
                return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)

            node = self._file_path_map.get(path)
            if node:
                return node.to_dict(recursive=recursive, max_depth=max_depth, only_path=only_path)
            return None
    
    async def get_blind_spot_list(self) -> Dict[str, Any]:
        """Get the current Blind-spot List."""
        async with self._global_semaphore:
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
        """Search files by name pattern."""
        results = []
        async with self._global_semaphore:
            for path, node in self._file_path_map.items():
                if query.lower() in node.name.lower():
                    results.append(node.to_dict())
        return results

    async def get_directory_stats(self) -> Dict[str, Any]:
        """Return basic statistics about the parsed data."""
        async with self._global_semaphore:
            oldest_dir = None
            if self._directory_path_map:
                dirs = [d for d in self._directory_path_map.values() if d.path != "/"]
                if dirs:
                    oldest_node = min(dirs, key=lambda x: x.modified_time)
                    oldest_dir = {"path": oldest_node.path, "timestamp": oldest_node.modified_time}

            blind_spot_files = len(self._blind_spot_additions)
            suspect_files = sum(1 for node in self._file_path_map.values() if node.integrity_suspect)

            return {
                "total_directories": len(self._directory_path_map),
                "total_files": len(self._file_path_map),
                "last_event_latency_ms": self._last_event_latency,
                "oldest_directory": oldest_dir,
                "has_blind_spot": blind_spot_files > 0 or len(self._blind_spot_deletions) > 0,
                "blind_spot_file_count": blind_spot_files,
                "blind_spot_deletion_count": len(self._blind_spot_deletions),
                "suspect_file_count": suspect_files,
                "logical_now": self._logical_clock.get_watermark()
            }
