"""
Fustor Agent Fusion Pusher Driver
"""
import logging
from typing import Any, Dict, List

from fustor_core.drivers import PusherDriver
from fustor_core.models.config import PusherConfig
from fustor_event_model.models import EventBase
from fustor_fusion_sdk.client import FusionClient

class FusionDriver(PusherDriver):
    """
    A driver that pushes events to the Fustor Fusion service using the fusion-sdk.
    """
    def __init__(self, id: str, config: PusherConfig):
        super().__init__(id, config)
        self.logger = logging.getLogger(f"fustor_agent.pusher.fusion.{id}")
        self.fusion_client = FusionClient(base_url=config.endpoint, api_key=config.credential.key)
        self.session_id = None

    async def create_session(self, task_id: str) -> Dict:
        self.logger.info(f"Creating session for task {task_id}...")
        session_id = await self.fusion_client.create_session(task_id)
        if session_id:
            self.session_id = session_id
            self.logger.info(f"Session created successfully: {session_id}")
            return {"session_id": session_id}
        else:
            self.logger.error("Failed to create session.")
            raise RuntimeError("Failed to create session with Fusion service.")

    async def push(self, events: List[EventBase], **kwargs) -> Dict:
        if not self.session_id:
            self.logger.error("Cannot push events: session_id is not set.")
            return {"snapshot_needed": False}

        event_dicts = [event.model_dump(mode='json') for event in events]
        source_type = kwargs.get("source_type", "message")

        success = await self.fusion_client.push_events(
            session_id=self.session_id,
            events=event_dicts,
            source_type=source_type
        )

        if success:
            self.logger.info(f"Successfully pushed {len(events)} events.")
            return {"snapshot_needed": False}
        else:
            self.logger.error(f"Failed to push {len(events)} events.")
            return {"snapshot_needed": False}

    async def heartbeat(self, **kwargs) -> Dict:
        if not self.session_id:
            self.logger.error("Cannot send heartbeat: session_id is not set.")
            return {"status": "error", "message": "Session ID not set"}

        success = await self.fusion_client.send_heartbeat(self.session_id)

        if success:
            self.logger.info("Heartbeat sent successfully.")
            return {"status": "ok"}
        else:
            self.logger.error("Failed to send heartbeat.")
            return {"status": "error", "message": "Failed to send heartbeat"}

    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        # For now, we don't need any specific fields
        return {}
