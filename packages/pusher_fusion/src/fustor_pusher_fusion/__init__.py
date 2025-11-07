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
        # This will be implemented next
        self.logger.info(f"Pushing {len(events)} events...")
        return {"snapshot_needed": False}

    async def heartbeat(self, **kwargs) -> Dict:
        # This will be implemented next
        self.logger.info("Sending heartbeat...")
        return {"status": "ok"}

    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        # For now, we don't need any specific fields
        return {}
