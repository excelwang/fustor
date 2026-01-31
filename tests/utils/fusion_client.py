"""
Fusion API client for integration tests.
"""
import time
from typing import Any, Optional
import requests


class FusionClient:
    """HTTP client for Fusion API."""

    def __init__(self, base_url: str = "http://localhost:18102", api_key: Optional[str] = None, view_id: str = "fs"):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.view_id = view_id
        self.session = requests.Session()
        if api_key:
            self.session.headers["X-API-Key"] = api_key

    def set_api_key(self, api_key: str) -> None:
        """Set API key for authentication."""
        self.api_key = api_key
        self.session.headers["X-API-Key"] = api_key

    # ============ Views API ============

    def get_tree(
        self,
        path: str = "/",
        max_depth: int = -1,
        only_path: bool = False,
        dry_run: bool = False
    ) -> dict[str, Any]:
        """Get file tree from Fusion."""
        resp = self.session.get(
            f"{self.base_url}/api/v1/views/{self.view_id}/tree",
            params={
                "path": path,
                "max_depth": max_depth,
                "only_path": only_path,
                "dry_run": dry_run
            }
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            if resp.status_code == 503:
                try:
                    detail = resp.json().get('detail', 'No detail provided')
                    print(f"\n[FUSION_CLIENT_ERROR] 503 Service Unavailable for {path}: {detail}")
                except Exception:
                    print(f"\n[FUSION_CLIENT_ERROR] 503 Service Unavailable for {path}: {resp.text}")
            raise e
        return resp.json()

    def search(self, pattern: str, path: str = "/") -> dict[str, Any]:
        """Search files by pattern."""
        resp = self.session.get(
            f"{self.base_url}/api/v1/views/{self.view_id}/search",
            params={"pattern": pattern, "path": path}
        )
        resp.raise_for_status()
        return resp.json()

    def get_stats(self) -> dict[str, Any]:
        """Get file system statistics."""
        resp = self.session.get(f"{self.base_url}/api/v1/views/{self.view_id}/stats")
        resp.raise_for_status()
        return resp.json()

    def reset(self) -> None:
        """Reset Fusion state for current datastore."""
        resp = self.session.delete(f"{self.base_url}/api/v1/views/{self.view_id}/reset")
        resp.raise_for_status()



    # ============ Consistency API ============

    def get_suspect_list(self, source_id: Optional[str] = None) -> list[dict]:
        """Get suspect list (files with integrity concerns)."""
        params = {}
        if source_id:
            params["source_id"] = source_id
        resp = self.session.get(
            f"{self.base_url}/api/v1/views/{self.view_id}/suspect-list",
            params=params
        )
        resp.raise_for_status()
        return resp.json()

    def update_suspect_list(self, updates: list[dict]) -> dict:
        """Update suspect list with new mtime values."""
        resp = self.session.put(
            f"{self.base_url}/api/v1/views/{self.view_id}/suspect-list",
            json={"updates": updates}
        )
        resp.raise_for_status()
        return resp.json()

    # ============ Session API ============

    def get_sessions(self) -> list[dict]:
        """Get all active sessions."""
        resp = self.session.get(f"{self.base_url}/api/v1/ingest/sessions/")
        resp.raise_for_status()
        return resp.json().get("active_sessions", [])

    def get_leader_session(self) -> Optional[dict]:
        """Get the current leader session."""
        sessions = self.get_sessions()
        for s in sessions:
            if s.get("role") == "leader":
                return s
        return None

    def get_blind_spots(self) -> dict:
        """Get the full blind-spot information as a dictionary."""
        resp = self.session.get(f"{self.base_url}/api/v1/views/{self.view_id}/blind-spots")
        resp.raise_for_status()
        return resp.json()

    def get_blind_spot_list(self) -> list[dict]:
        """
        Get blind-spot list as a list of dictionaries (for compatibility with existing tests).
        Returns a unified list of files and deletions.
        """
        data = self.get_blind_spots()
        result = []
        
        # Add files marked as blind spot additions
        for f in data.get("additions", []):
            # To match the previous list format, we can add a type field if needed
            # but usually the tests look for 'path' in the dictionaries.
            f["type"] = "file"
            result.append(f)
            
        # Add deletions
        for path in data.get("deletions", []):
            result.append({
                "path": path,
                "type": "deletion",
                "content_type": "file" # Assumption for FS view
            })
            
        return result

    # ============ Utility Methods ============

    def wait_for_file(
        self,
        path: str,
        timeout: float = 30,
        interval: float = 0.1,
        should_exist: bool = True
    ) -> bool:
        """Wait for file to appear/disappear in Fusion tree."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                tree = self.get_tree(path=path, max_depth=0)
                exists = tree.get("name") is not None or tree.get("path") is not None
                if exists == should_exist:
                    return True
            except requests.HTTPError:
                if not should_exist:
                    return True
            time.sleep(interval)
        return False

    def wait_for_file_in_tree(
        self,
        file_path: str,
        root_path: str = "/",
        timeout: float = 30,
        interval: float = 0.1
    ) -> Optional[dict]:
        """Wait for a specific file to appear in tree."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                tree = self.get_tree(path=root_path, max_depth=-1)
                found = self._find_in_tree(tree, file_path)
                if found:
                    return found
            except requests.HTTPError:
                pass
            time.sleep(interval)
        return None

    def _find_in_tree(self, node: dict, target_path: str) -> Optional[dict]:
        """Recursively find a file in tree."""
        if node.get("path") == target_path:
            return node
        for child in node.get("children", []):
            found = self._find_in_tree(child, target_path)
            if found:
                return found
        return None

    def check_file_flags(self, file_path: str) -> dict:
        """Check flag status of a file."""
        # Check integrity_suspect from tree
        try:
            tree = self.get_tree(path=file_path, max_depth=0)
            suspect = tree.get("integrity_suspect", False)
        except requests.HTTPError:
            suspect = False

        # Check agent_missing from blind-spots API
        # Note: This is less efficient but correct per new API design
        blind_spots = self.get_blind_spots()
        agent_missing = False
        
        # Check in additions list
        for f in blind_spots.get("additions", []):
            if f.get("path") == file_path:
                agent_missing = True
                break
                
        return {
            "agent_missing": agent_missing,
            "integrity_suspect": suspect
        }

    def wait_for_flag(
        self,
        file_path: str,
        flag_name: str,
        expected_value: bool,
        timeout: float = 30,
        interval: float = 0.1
    ) -> bool:
        """Wait for a specific flag to reach expected value."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                flags = self.check_file_flags(file_path)
                if flags.get(flag_name) == expected_value:
                    return True
            except requests.HTTPError:
                pass
            time.sleep(interval)
        return False
