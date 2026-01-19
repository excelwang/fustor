
import httpx
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

def contains_surrogate_characters(text: str) -> bool:
    """Check if text contains surrogate characters."""
    try:
        text.encode('utf-8')
        return False
    except UnicodeEncodeError:
        return True

def sanitize_surrogate_characters(obj: Any) -> Any:
    """
    Recursively sanitize an object by replacing surrogate characters with safe alternatives.
    """
    if isinstance(obj, str):
        if contains_surrogate_characters(obj):
            # Encode with replacement and decode back to handle surrogate characters
            return obj.encode('utf-8', errors='replace').decode('utf-8')
        return obj
    elif isinstance(obj, dict):
        return {key: sanitize_surrogate_characters(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_surrogate_characters(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(sanitize_surrogate_characters(item) for item in obj)
    else:
        return obj

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
            # Sanitize task_id to handle any surrogate characters before JSON serialization
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

    async def push_events(self, session_id: str, events: List[Dict[str, Any]], source_type: str, is_snapshot_end: bool = False) -> bool:
        """
        Pushes a batch of events to the Fusion service.
        """
        try:
            # Sanitize events to handle any surrogate characters before JSON serialization
            sanitized_events = [sanitize_surrogate_characters(event) for event in events]
            sanitized_source_type = sanitize_surrogate_characters(source_type)

            payload = {
                "session_id": session_id,
                "events": sanitized_events,
                "source_type": sanitized_source_type,
                "is_snapshot_end": is_snapshot_end
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
