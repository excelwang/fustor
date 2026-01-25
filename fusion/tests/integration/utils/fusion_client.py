"""
Fusion API client for integration tests.
"""
import time
from typing import Any, Optional
import requests


class FusionClient:
    """HTTP client for Fusion API."""

    def __init__(self, base_url: str = "http://localhost:18102", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
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
            f"{self.base_url}/api/v1/views/fs/tree",
            params={
                "path": path,
                "max_depth": max_depth,
                "only_path": only_path,
                "dry_run": dry_run
            }
        )
        resp.raise_for_status()
        return resp.json()

    def search(self, pattern: str, path: str = "/") -> dict[str, Any]:
        """Search files by pattern."""
        resp = self.session.get(
            f"{self.base_url}/api/v1/views/fs/search",
            params={"pattern": pattern, "path": path}
        )
        resp.raise_for_status()
        return resp.json()

    def get_stats(self) -> dict[str, Any]:
        """Get file system statistics."""
        resp = self.session.get(f"{self.base_url}/api/v1/views/fs/stats")
        resp.raise_for_status()
        return resp.json()



    # ============ Consistency API ============

    def get_suspect_list(self, source_id: Optional[str] = None) -> list[dict]:
        """Get suspect list (files with integrity concerns)."""
        params = {}
        if source_id:
            params["source_id"] = source_id
        resp = self.session.get(
            f"{self.base_url}/api/v1/views/fs/suspect-list",
            params=params
        )
        resp.raise_for_status()
        return resp.json()

    def update_suspect_list(self, updates: list[dict]) -> dict:
        """Update suspect list with new mtime values."""
        resp = self.session.put(
            f"{self.base_url}/api/v1/views/fs/suspect-list",
            json={"updates": updates}
        )
        resp.raise_for_status()
        return resp.json()

    def get_blind_spot_list(self) -> list[dict]:
        """Get blind-spot list (files from clients without agents)."""
        resp = self.session.get(f"{self.base_url}/api/v1/views/fs/blind-spot-list")
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

    # ============ Utility Methods ============

    def wait_for_file(
        self,
        path: str,
        timeout: float = 30,
        interval: float = 1,
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
        interval: float = 1
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
        tree = self.get_tree(path=file_path, max_depth=0)
        return {
            "agent_missing": tree.get("agent_missing", False),
            "integrity_suspect": tree.get("integrity_suspect", False)
        }

    def wait_for_flag(
        self,
        file_path: str,
        flag_name: str,
        expected_value: bool,
        timeout: float = 30,
        interval: float = 1
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
