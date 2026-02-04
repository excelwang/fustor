
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
    def __init__(
        self, 
        base_url: str, 
        api_key: str, 
        api_version: str = "legacy",
        timeout: float = 30.0
    ):
        """
        Initialize FusionClient.
        
        Args:
            base_url: The base URL of the Fusion service
            api_key: API key for authentication
            api_version: API version to use ("pipe" uses /api/v1/pipe, "legacy" uses /api/v1/ingest)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.api_key = api_key
        self.api_version = api_version
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            base_url=self.base_url, 
            headers={"X-API-Key": self.api_key},
            timeout=self.timeout
        )
        
        # Set API paths based on version
        if api_version == "legacy":
            # Legacy paths for backward compatibility
            self._session_path = "/api/v1/ingest/sessions"
            self._events_path = "/api/v1/ingest/events"
            self._consistency_path = "/api/v1/ingest/consistency"
        else:
            # New pipe-based paths (recommended)
            self._session_path = "/api/v1/pipe/session"
            self._events_path = "/api/v1/pipe/ingest"
            self._consistency_path = "/api/v1/pipe/consistency"

    async def create_session(
        self, 
        task_id: str, 
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Creates a new session and returns the session details (including ID and role).
        """
        try:
            # Sanitize task_id to handle any surrogate characters before JSON serialization
            payload = {
                "task_id": task_id,
                "source_type": source_type,
                "session_timeout_seconds": session_timeout_seconds
            }
            # Remove None values
            payload = {k: v for k, v in payload.items() if v is not None}
            
            response = await self.client.post(f"{self._session_path}/", json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"An error occurred connecting to {self.base_url}: {e!r}")
            return None

    async def get_sentinel_tasks(self) -> Dict[str, Any]:
        """
        Retrieves generic sentinel tasks (e.g. suspect checks).
        """
        try:
            response = await self.client.get(f"{self._consistency_path}/sentinel/tasks")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get sentinel tasks: {e}")
            return {}

    async def submit_sentinel_feedback(self, feedback: Dict[str, Any]) -> bool:
        """
        Submits feedback for sentinel tasks.
        """
        try:
            response = await self.client.post(f"{self._consistency_path}/sentinel/feedback", json=feedback)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to submit sentinel feedback: {e}")
            return False


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
                "is_end": is_snapshot_end  # Fixed: Receiver expects 'is_end', not 'is_snapshot_end'
            }
            response = await self.client.post(f"{self._events_path}/{session_id}/events", json=payload)
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 419:
                raise
            logger.error(f"HTTP error occurred during event push: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"An error occurred during event push: {e}")
            return False

    async def send_heartbeat(self, session_id: str, can_realtime: bool = False) -> Optional[Dict[str, Any]]:
        """
        Sends a heartbeat to the Fusion service to keep the session alive.
        Returns the response dict if successful, None otherwise.
        """
        try:
            params = {"can_realtime": can_realtime}
            response = await self.client.post(f"{self._session_path}/{session_id}/heartbeat", json=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 419:
                raise
            logger.error(f"HTTP error occurred during heartbeat: {e.response.status_code} - {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"An error occurred during heartbeat: {e}")
            return None

    async def signal_audit_start(self, source_id: int) -> bool:
        """Signals the start of an audit cycle."""
        try:
             # Updated to new consistency API path
             response = await self.client.post(f"{self._consistency_path}/audit/start")
             response.raise_for_status()
             return True
        except Exception as e:
            logger.error(f"Failed to signal audit start: {e}")
            return False

    async def signal_audit_end(self, source_id: int) -> bool:
        """Signals the end of an audit cycle."""
        try:
             # Updated to new consistency API path
             response = await self.client.post(f"{self._consistency_path}/audit/end")
             response.raise_for_status()
             return True
        except Exception as e:
            logger.error(f"Failed to signal audit end: {e}")
            return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
