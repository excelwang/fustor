"""
Registry API client for integration tests.
"""
from typing import Any, Optional
import requests


class RegistryClient:
    """HTTP client for Registry API."""

    def __init__(self, base_url: str = "http://localhost:18101"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self._token: Optional[str] = None

    def login(self, username: str = "admin@admin.com", password: str = "admin") -> str:
        """Login and get access token."""
        # Registry API expects form-urlencoded data
        resp = self.session.post(
            f"{self.base_url}/v1/auth/login",
            data={"username": username, "password": password}
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self.session.headers["Authorization"] = f"Bearer {self._token}"
        return self._token

    def create_datastore(self, name: str, description: str = "", allow_concurrent_push: bool = False) -> dict[str, Any]:
        """Create a new datastore."""
        resp = self.session.post(
            f"{self.base_url}/v1/datastores/",
            json={
                "name": name, 
                "description": description, 
                "allow_concurrent_push": allow_concurrent_push
            }
        )
        if not resp.ok:
            print(f"Create datastore failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def update_datastore(self, datastore_id: int, **updates) -> dict[str, Any]:
        """Update a datastore's configuration."""
        # Note: In registry/api/datastores/router.py, PUT requires a full DatastoreCreate body
        # but the logic allows partial updates via exclude_unset.
        # Actually, let's just get the current state and overlay updates
        datastores = self.get_datastores()
        current = next((ds for ds in datastores if ds["id"] == datastore_id), None)
        if not current:
            raise RuntimeError(f"Datastore {datastore_id} not found for update")
        
        payload = {
            "name": current["name"],
            "description": current.get("description", ""),
            "allow_concurrent_push": current.get("allow_concurrent_push", False),
            **updates
        }
        
        resp = self.session.put(
            f"{self.base_url}/v1/datastores/{datastore_id}",
            json=payload
        )
        resp.raise_for_status()
        return resp.json()

    def get_datastores(self) -> list[dict]:
        """Get all datastores."""
        resp = self.session.get(f"{self.base_url}/v1/datastores/")
        resp.raise_for_status()
        return resp.json()

    def create_api_key(self, datastore_id: str, name: str = "test-key") -> dict[str, Any]:
        """Create an API key for a datastore."""
        resp = self.session.post(
            f"{self.base_url}/v1/keys/",
            json={"datastore_id": datastore_id, "name": name}
        )
        if not resp.ok:
            print(f"Create API key failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def get_api_keys(self) -> list[dict]:
        """Get all API keys."""
        resp = self.session.get(f"{self.base_url}/v1/keys/")
        resp.raise_for_status()
        return resp.json()
