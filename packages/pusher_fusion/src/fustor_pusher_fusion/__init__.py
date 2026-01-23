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
        session_data = await self.fusion_client.create_session(task_id)
        if session_data and session_data.get("session_id"):
            self.session_id = session_data["session_id"]
            self.logger.info(f"Session created successfully: {self.session_id}, Role: {session_data.get('role')}")
            return session_data
        else:
            self.logger.error("Failed to create session.")
            raise RuntimeError("Failed to create session with Fusion service.")



    async def get_consistency_tasks(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Queries Fusion for consistency tasks.
        Initially supports 'suspect_check' for FS.
        """
        # We can map this to different task types if Fusion API expands.
        # For now, we specifically check the FS suspect list.
        # Ideally, Fusion should have a generic endpoint like /api/consistency/tasks
        # But we are adapting to existing views for now.
        try:
             # Check if we should even ask? Maybe based on config?
             # For now, always ask if source_fs is involved.
             # The Fusion endpoint is view-specific (/api/view/fs/suspect-list).
             
             suspects = await self.fusion_client.get_suspect_list(kwargs.get("source_id"))
             if suspects:
                 # Map FS suspects to generic task format
                 paths = [item['path'] for item in suspects if 'path' in item]
                 if paths:
                     return {
                         'type': 'suspect_check', 
                         'paths': paths,
                         'source_id': kwargs.get("source_id")
                     }
        except Exception as e:
            self.logger.debug(f"Failed to get consistency tasks: {e}")
        return None

    async def submit_consistency_results(self, results: Dict[str, Any], **kwargs) -> bool:
        """
        Submits consistency results to Fusion.
        """
        try:
            res_type = results.get('type')
            if res_type == 'suspect_update':
                updates = results.get('updates', [])
                if updates:
                    return await self.fusion_client.update_suspect_list(updates)
            return True
        except Exception as e:
             self.logger.error(f"Failed to submit consistency results: {e}")
             return False

    async def send_command(self, command: str, **kwargs) -> Any:
        if command == "get_suspect_list":
            return await self.fusion_client.get_suspect_list(kwargs.get("source_id"))
        elif command == "update_suspect_list":
            return await self.fusion_client.update_suspect_list(kwargs.get("updates"))
        elif command == "signal_audit_start":
            return await self.fusion_client.signal_audit_start(kwargs.get("source_id"))
        elif command == "signal_audit_end":
            return await self.fusion_client.signal_audit_end(kwargs.get("source_id"))
        else:
            raise NotImplementedError(f"Command '{command}' not supported by FusionDriver.")

    async def push(self, events: List[EventBase], **kwargs) -> Dict:
        if not self.session_id:
            self.logger.error("Cannot push events: session_id is not set.")
            return {"snapshot_needed": False}

        event_dicts = [event.model_dump(mode='json') for event in events]
        source_type = kwargs.get("source_type", "message")
        is_snapshot_end = kwargs.get("is_snapshot_end", False)
        
        # Calculate total rows across all events for accurate logging
        total_rows = sum(len(event.rows) for event in events if event.rows)

        success = await self.fusion_client.push_events(
            session_id=self.session_id,
            events=event_dicts,
            source_type=source_type,
            is_snapshot_end=is_snapshot_end
        )

        if success:
            self.logger.info(f"[{source_type}] Successfully pushed {len(events)} events ({total_rows} rows).")
            return {"snapshot_needed": False}
        else:
            self.logger.error(f"[{source_type}] Failed to push {len(events)} events ({total_rows} rows).")
            return {"snapshot_needed": False}

    async def heartbeat(self, **kwargs) -> Dict:
        if not self.session_id:
            self.logger.error("Cannot send heartbeat: session_id is not set.")
            return {"status": "error", "message": "Session ID not set"}

        result = await self.fusion_client.send_heartbeat(self.session_id)

        if result:
            self.logger.debug("Heartbeat sent successfully.")
            return result
        else:
            self.logger.error("Failed to send heartbeat.")
            return {"status": "error", "message": "Failed to send heartbeat"}

    @classmethod
    async def get_needed_fields(cls, **kwargs) -> Dict[str, Any]:
        # For now, we don't need any specific fields
        return {}
