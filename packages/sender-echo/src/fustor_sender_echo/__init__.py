"""
Fustor Echo Sender Driver (Class-based)
"""
import json
import logging
from typing import Any, Dict, List, Optional

from fustor_core.transport import Sender
from fustor_core.models.config import SenderConfig
from fustor_core.event import EventBase


class EchoDriver(Sender):
    """
    An echo driver that inherits from the Sender ABC.
    It prints batch and cumulative statistics for all received events.
    """
    def __init__(
        self,
        sender_id: str,
        endpoint: str,
        credential: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None
    ):
        """Initializes the driver and its statistics counters."""
        super().__init__(sender_id, endpoint, credential, config)
        self.total_rows = 0
        self.total_size = 0
        self.logger = logging.getLogger(f"fustor_agent.sender.echo.{sender_id}")
        self._snapshot_triggered = False

    async def connect(self) -> None:
        """Echo driver connection is a no-op."""
        self.logger.info(f"Echo Sender {self.id} ready.")

    async def _send_events_impl(
        self, events: List[Any], source_type: str = "realtime", is_end: bool = False
    ) -> Dict:
        """
        Implementation of echo sending.
        """
        batch_rows = 0
        for event in events:
            # Handle both EventBase objects and raw dicts
            if hasattr(event, "rows"):
                batch_rows += len(event.rows)
            elif isinstance(event, dict):
                batch_rows += len(event.get("rows", []))
        
        self.total_rows += batch_rows

        # Prepare a summary for logging
        flags = []
        if is_end:
            flags.append("END")
        
        flags_str = f" | Flags: {', '.join(flags)}" if flags else ""

        self.logger.info(
            f"[EchoSender] [{source_type.upper()}] Task: {self.id} | 本批次: {batch_rows}条; 累计: {self.total_rows}条{flags_str}"
        )

        # For debugging, log the first event's data if available
        if events:
            first_event_rows = None
            if hasattr(events[0], "rows"):
                first_event_rows = events[0].rows
            elif isinstance(events[0], dict):
                first_event_rows = events[0].get("rows", [])
            
            if first_event_rows:
                self.logger.info(f"First event data: {json.dumps(first_event_rows[0], ensure_ascii=False)}")

        # Trigger snapshot only once if the condition is met
        snapshot_needed = False
        if not self._snapshot_triggered:
            snapshot_needed = True
            self._snapshot_triggered = True
            self.logger.info(f"Task '{self.id}' is triggering a one-time snapshot.")

        return {"success": True, "snapshot_needed": snapshot_needed}

    async def heartbeat(self) -> Dict[str, Any]:
        """
        Sends a heartbeat to maintain session state.
        """
        self.logger.info(f"[EchoSender] Heartbeat for session {self.session_id} from task {self.id}")
        return {"status": "ok", "role": self.config.get("mock_role", "leader")}

    async def create_session(
        self,
        task_id: str,
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Creates a new session.
        """
        import uuid
        session_id = str(uuid.uuid4())
        self.session_id = session_id
        role = self.config.get("mock_role", "leader")
        timeout = session_timeout_seconds or self.config.get("session_timeout_seconds", 30)
        self.logger.info(f"[EchoSender] Created session {session_id} for task {task_id} with role {role}")
        metadata = {
            "session_id": session_id,
            "role": role,
            "session_timeout_seconds": timeout
        }
        return session_id, metadata

    async def close_session(self) -> None:
        """Close the current session."""
        if self.session_id:
            self.logger.info(f"[EchoSender] Closing session {self.session_id}")
            self.session_id = None

    async def close(self) -> None:
        """Close the sender."""
        self.logger.info(f"[EchoSender] Sender {self.id} closed")
        self._snapshot_triggered = False

    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        """
        The echo driver does not need any specific fields, so it returns an empty schema,
        signaling that it accepts all fields.
        """
        return {}

