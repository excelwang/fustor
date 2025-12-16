
import httpx
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

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
            logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None

    async def push_events(self, session_id: str, events: List[Dict[str, Any]], source_type: str) -> bool:
        """
        Pushes a batch of events to the Fusion service.
        """
        try:
            payload = {
                "session_id": session_id,
                "events": events,
                "source_type": source_type
            }
            response = await self.client.post("/ingestor-api/v1/events/", json=payload)
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred during event push: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during event push: {e}")
            return False

    async def send_heartbeat(self, session_id: str) -> bool:
        """
        Sends a heartbeat to the Fusion service to keep the session alive.
        """
        try:
            headers = {"session-id": session_id}
            response = await self.client.post("/ingestor-api/v1/sessions/heartbeat", headers=headers)
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred during heartbeat: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during heartbeat: {e}")
            return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
