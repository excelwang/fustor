"""
Fuagent Echo Pusher Driver (Class-based)
"""
import json
import logging
from typing import Any, Dict, List

from fustor_core.drivers import PusherDriver
from fustor_core.models.config import PusherConfig
from fustor_core.models.event import EventBase


class EchoDriver(PusherDriver):
    """
    An echo driver that inherits from the PusherDriver ABC.
    It prints batch and cumulative statistics for all received events.
    """
    def __init__(self, id: str, config: PusherConfig):
        """Initializes the driver and its statistics counters."""
        super().__init__(id, config)
        self.total_rows = 0
        self.total_size = 0
        self.logger = logging.getLogger(f"fustor_agent.pusher.echo.{id}")
        self._snapshot_triggered = False


    async def push(self, events: List[EventBase], source_type: str = "realtime", **kwargs) -> Dict:
        """
        Receives events, prints them along with control flags, and updates statistics.
        """
        agent_id = kwargs.get("agent_id", "N/A")
        task_id = kwargs.get("task_id", "N/A")
        is_snapshot_end = kwargs.get("is_snapshot_end", False)
        snapshot_sync_suggested = kwargs.get("snapshot_sync_suggested", False)

        batch_rows = sum(len(event.rows) for event in events)
        self.total_rows += batch_rows

        # Prepare a summary of control flags for logging
        flags = []
        if is_snapshot_end:
            flags.append("SNAPSHOT_END")
        if snapshot_sync_suggested:
            flags.append("SNAPSHOT_SUGGESTED")
        
        flags_str = f" | Flags: {', '.join(flags)}" if flags else ""

        self.logger.info(
            f"[EchoPusher] [{source_type.upper()}] Agent: {agent_id} | Task: {task_id} | 本批次: {batch_rows}条; 累计: {self.total_rows}条{flags_str}"
        )

        # For debugging, log the first event's data if available
        if events and events[0].rows:
            self.logger.info(f"First event data: {json.dumps(events[0].rows[0], ensure_ascii=False)}")

        # Trigger snapshot only once if the condition is met
        snapshot_needed = False
        if not self._snapshot_triggered:
            snapshot_needed = True
            self._snapshot_triggered = True
            self.logger.info(f"Task '{task_id}' is triggering a one-time snapshot.")

        # Conform to the new interface by returning the expected dictionary
        return {"snapshot_needed": snapshot_needed}

    async def heartbeat(self, **kwargs) -> Dict:
        """
        Sends a heartbeat to maintain session state.
        The `kwargs` will contain `agent_id`, `task_id`, and `session_id`.
        """
        session_id = kwargs.get("session_id")
        agent_id = kwargs.get("agent_id")
        task_id = kwargs.get("task_id")
        
        # Echo driver just logs the heartbeat for demonstration
        self.logger.info(f"[EchoPusher] Heartbeat for session {session_id} from task {task_id}, agent {agent_id}")
        return {"status": "ok", "message": f"Echo heartbeat for session {session_id}"}

    async def create_session(self, task_id: str) -> str:
        """
        Creates a new session with the pusher endpoint.
        The `kwargs` will contain `agent_id`, `task_id`, and `task_id`.
        Returns the session ID string.
        """
        # For the echo driver, we just return a random session ID
        import uuid
        session_id = str(uuid.uuid4())
        self.logger.info(f"[EchoPusher] Created session {session_id} for task {task_id}")
        return session_id

    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        """
        The echo driver does not need any specific fields, so it returns an empty schema,
        signaling that it accepts all fields.
        """
        return {}

    @classmethod
    async def get_wizard_steps(cls) -> Dict[str, Any]:
        """Provides a simple wizard definition for the Echo driver."""
        return {
            "steps": [
                {
                    "step_id": "confirmation",
                    "title": "Echo Driver",
                    "schema": {
                        "type": "object",
                        "description": "此驱动没有任何需要配置的参数。它会将接收到的所有事件打印到日志中。"
                    }
                }
            ]
        }
