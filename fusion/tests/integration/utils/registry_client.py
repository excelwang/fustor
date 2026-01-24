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

    def login(self, username: str = "admin", password: str = "admin123") -> str:
        """Login and get access token."""
        resp = self.session.post(
            f"{self.base_url}/v1/auth/login",
            json={"username": username, "password": password}
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self.session.headers["Authorization"] = f"Bearer {self._token}"
        return self._token

    def create_datastore(self, name: str, description: str = "") -> dict[str, Any]:
        """Create a new datastore."""
        resp = self.session.post(
            f"{self.base_url}/v1/datastores/",
            json={"name": name, "description": description}
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
        resp.raise_for_status()
        return resp.json()

    def get_api_keys(self) -> list[dict]:
        """Get all API keys."""
        resp = self.session.get(f"{self.base_url}/v1/keys/")
        resp.raise_for_status()
        return resp.json()
