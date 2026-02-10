"""
Fusion API client for integration tests.
"""
import time
from typing import Any, Optional
import requests

# Import it directory to sys.path if needed for imports
import os
import sys
from pathlib import Path

_utils_dir = Path(__file__).parent
_it_dir = _utils_dir.parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

try:
    from fixtures.constants import (
        AGENT_READY_TIMEOUT,
        VIEW_READY_TIMEOUT,
        AUDIT_WAIT_TIMEOUT,
        POLL_INTERVAL,
        FAST_POLL_INTERVAL
    )
except ImportError:
    # Fallback to defaults if constants not available
    AGENT_READY_TIMEOUT = 30
    VIEW_READY_TIMEOUT = 30
    AUDIT_WAIT_TIMEOUT = 45
    POLL_INTERVAL = 0.5
    FAST_POLL_INTERVAL = 0.1


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
        dry_run: bool = False,
        silence_503: bool = False
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
                if not silence_503:
                    try:
                        detail = resp.json().get('detail', 'No detail provided')
                        print(f"\n[FUSION_CLIENT_ERROR] 503 Service Unavailable for {path}: {detail}")
                    except Exception:
                        print(f"\n[FUSION_CLIENT_ERROR] 503 Service Unavailable for {path}: {resp.text}")
            raise e
        data = resp.json()
        return data["data"]

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
        """Reset Fusion state for current view (comprehensive)."""
        resp = self.session.post(f"{self.base_url}/api/v1/pipe/consistency/reset")
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

    def get_sentinel_tasks(self) -> dict:
        """Get sentinel tasks."""
        resp = self.session.get(f"{self.base_url}/api/v1/pipe/consistency/sentinel/tasks")
        resp.raise_for_status()
        return resp.json()

    def submit_sentinel_feedback(self, feedback: dict) -> dict:
        """Submit sentinel feedback."""
        resp = self.session.post(
            f"{self.base_url}/api/v1/pipe/consistency/sentinel/feedback",
            json=feedback
        )
        resp.raise_for_status()
        return resp.json()

    # ============ Session API ============

    def get_sessions(self) -> list[dict]:
        """Get all active sessions."""
        resp = self.session.get(f"{self.base_url}/api/v1/pipe/session/")
        resp.raise_for_status()
        return resp.json().get("active_sessions", [])

    def terminate_session(self, session_id: str) -> dict:
        """Terminate an active session."""
        # Try new V2 path first: DELETE /session/{session_id}
        resp = self.session.delete(f"{self.base_url}/api/v1/pipe/session/{session_id}")
        if resp.status_code == 404:
            # Fallback to legacy path: DELETE /session/ with header
            resp = self.session.delete(
                f"{self.base_url}/api/v1/pipe/session/",
                headers={"Session-ID": session_id}
            )
        resp.raise_for_status()
        return resp.json()

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


    def get_node(self, path: str) -> Optional[dict[str, Any]]:
        """Get metadata for a single node (file/dir) via tree API."""
        try:
            # max_depth=0 returns the node itself if it's a file, or the dir with children if it's a dir
            # But the tree API returns the subtree rooted at 'path'.
            tree = self.get_tree(path=path, max_depth=0, silence_503=True)
            if not tree:
                return None
            # If path doesn't exist, get_tree might throw 404 or return empty?
            # Impl of get_tree usually throws on 404.
            return tree
        except (requests.HTTPError, Exception):
            return None

    # ============ Utility Methods ============

    def wait_for_file(
        self,
        path: str,
        timeout: float = 30,
        interval: float = FAST_POLL_INTERVAL,
        should_exist: bool = True
    ) -> bool:
        """Wait for file to appear/disappear in Fusion tree."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                tree = self.get_tree(path=path, max_depth=0, silence_503=True)
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
        interval: float = FAST_POLL_INTERVAL
    ) -> Optional[dict]:
        """Wait for a specific file to appear in tree."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                tree = self.get_tree(path=root_path, max_depth=-1, silence_503=True)
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
        
        # Check children
        if "children" in node:
            for child in node["children"]:
                found = self._find_in_tree(child, target_path)
                if found:
                    return found
        return None

    def wait_for_file_not_in_tree(
        self,
        file_path: str,
        root_path: str = "/",
        timeout: float = 30,
        interval: float = POLL_INTERVAL
    ) -> bool:
        """Wait for a specific file to disappear from tree."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                tree = self.get_tree(path=root_path, max_depth=-1)
                found = self._find_in_tree(tree, file_path)
                if not found:
                    return True
            except requests.HTTPError:
                # If we can't get tree, consider file gone
                return True
            time.sleep(interval)
        return False

    def check_file_flags(self, file_path: str) -> dict:
        """Check flag status of a file."""
        # Check integrity_suspect from tree
        try:
            tree = self.get_tree(path=file_path, max_depth=0, silence_503=True)
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

    def wait_for_audit(self, timeout: float = AUDIT_WAIT_TIMEOUT, interval: float = POLL_INTERVAL) -> bool:
        """Wait for an audit cycle to complete."""
        try:
            initial_stats = self.get_stats()
            initial_count = initial_stats.get("audit_cycle_count", 0)
        except Exception:
            initial_count = -1
            
        start = time.time()
        while time.time() - start < timeout:
            try:
                stats = self.get_stats()
                current_count = stats.get("audit_cycle_count", 0)
                if current_count > initial_count and stats.get("last_audit_finished_at", 0) > 0:
                    return True
            except Exception:
                pass
            time.sleep(interval)
        return False

    def wait_for_view_ready(self, timeout: float = VIEW_READY_TIMEOUT, interval: float = POLL_INTERVAL) -> bool:
        """Wait for view initial snapshot to complete and queue to drain."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                # Stats endpoint will return 503 if not ready in our readiness checker
                self.get_stats()
                return True
            except requests.HTTPError as e:
                if e.response.status_code == 503:
                    time.sleep(interval)
                    continue
                raise e
            except Exception:
                time.sleep(interval)
        return False

    def wait_for_agent_ready(self, agent_id: str, timeout: float = AGENT_READY_TIMEOUT, interval: float = POLL_INTERVAL) -> bool:
        """Wait for an agent to be registered and reporting can_realtime=True."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                sessions = self.get_sessions()
                agent_session = next((s for s in sessions if agent_id in s.get("agent_id", "")), None)
                if agent_session and agent_session.get("can_realtime"):
                    return True
            except Exception:
                pass
            time.sleep(interval)
        return False
