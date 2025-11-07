
import httpx
from typing import Optional

class FusionClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.client = httpx.AsyncClient(base_url=self.base_url, headers={"X-API-Key": self.api_key})

    async def create_session(self, task_id: str) -> Optional[str]:
        """
        Creates a new session and returns the session ID.
        """
        try:
            payload = {"task_id": task_id}
            response = await self.client.post("/ingestor-api/v1/sessions/", json=payload)
            response.raise_for_status()
            return response.json().get("session_id")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
