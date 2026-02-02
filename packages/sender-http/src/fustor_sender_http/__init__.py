"""
Fustor HTTP Sender - Transport layer for Agent to Fusion communication.

This package implements the HTTP transport protocol for sending events
from Fustor Agent to Fustor Fusion.
"""
import logging
from typing import Any, Dict, List, Optional

import httpx
from fustor_core.transport import Sender
from fustor_core.event import EventBase
from fustor_core.exceptions import SessionObsoletedError


class HTTPSender(Sender):
    """
    HTTP-based Sender implementation for Fustor.
    
    Uses the Fusion SDK client to communicate with Fusion's REST API.
    """
    
    def __init__(
        self,
        sender_id: str,
        endpoint: str,
        credential: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(sender_id, endpoint, credential, config)
        self.logger = logging.getLogger(f"fustor.sender.http.{sender_id}")
        
        # Lazy import to avoid circular dependency
        from fustor_fusion_sdk.client import FusionClient
        
        api_key = credential.get("key") or credential.get("api_key")
        
        # Extended configuration
        timeout = self.config.get("timeout", 30.0)
        api_version = self.config.get("api_version", "legacy")
        
        self.client = FusionClient(
            base_url=endpoint, 
            api_key=api_key,
            timeout=timeout,
            api_version=api_version
        )
    
    async def connect(self) -> None:
        """Establish connection (for HTTP, this is a no-op as we use stateless requests)."""
        self.logger.debug(f"HTTP Sender {self.id} ready for endpoint {self.endpoint}")
    
    async def create_session(
        self, 
        task_id: str,
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a new session with Fusion.
        
        Args:
            task_id: Identifier for this sync task
            source_type: Type of source
            session_timeout_seconds: Requested timeout
            
        Returns:
            Session metadata including session_id, timeout, role
        """
        self.logger.info(f"Creating session for task {task_id}...")
        session_data = await self.client.create_session(
            task_id, 
            source_type=source_type,
            session_timeout_seconds=session_timeout_seconds
        )
        
        if session_data and session_data.get("session_id"):
            self.session_id = session_data["session_id"]
            self.logger.info(
                f"Session created: {self.session_id}, "
                f"Role: {session_data.get('role')}, "
                f"Timeout: {session_data.get('session_timeout_seconds')}s"
            )
            return session_data
        else:
            self.logger.error("Failed to create session.")
            raise RuntimeError("Failed to create session with Fusion service.")
    
    async def _send_events_impl(
        self, 
        events: List[EventBase], 
        source_type: str = "message",
        is_end: bool = False
    ) -> Dict[str, Any]:
        """
        Implementation of sending events to Fusion.
        Called by the base class template method.
        """
        if not self.session_id:
            self.logger.error("Cannot send events: session_id is not set.")
            return {"success": False, "error": "No session"}
        
        # Convert events to dictionaries for JSON serialization
        event_dicts = []
        for event in events:
            if hasattr(event, 'model_dump'):
                event_dicts.append(event.model_dump(mode='json'))
            elif isinstance(event, dict):
                event_dicts.append(event)
            else:
                event_dicts.append(dict(event))
        
        total_rows = sum(len(e.get("rows", [])) for e in event_dicts)
        self.logger.debug(f"[{source_type}] Attempting to push {len(events)} events ({total_rows} rows) to {self.endpoint}")
        
        try:
            success = await self.client.push_events(
                session_id=self.session_id,
                events=event_dicts,
                source_type=source_type,
                is_snapshot_end=is_end
            )
            
            if success:
                self.logger.info(f"[{source_type}] Sent {len(events)} events ({total_rows} rows).")
                return {"success": True}
            else:
                self.logger.error(f"[{source_type}] Failed to send {len(events)} events.")
                return {"success": False, "error": "Push failed"}
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 419:
                raise SessionObsoletedError(f"Session {self.session_id} is obsolete (419)")
            self.logger.error(f"[{source_type}] Failed to send {len(events)} events: {e}")
            return {"success": False, "error": f"Push failed: {e}"}
        except Exception as e:
            self.logger.error(f"[{source_type}] Failed to send {len(events)} events: {e}")
            return {"success": False, "error": "Push failed"}
    
    async def heartbeat(self) -> Dict[str, Any]:
        """
        Send a heartbeat to maintain session.
        
        Returns:
            Response including current role status
        """
        if not self.session_id:
            self.logger.error("Cannot send heartbeat: session_id is not set.")
            return {"status": "error", "message": "Session ID not set"}
        
        try:
            result = await self.client.send_heartbeat(self.session_id)
            
            if result:
                self.logger.debug("Heartbeat sent successfully.")
                return result
            else:
                self.logger.error("Failed to send heartbeat.")
                return {"status": "error", "message": "Heartbeat failed"}
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 419:
                raise SessionObsoletedError(f"Session {self.session_id} is obsolete (419)")
            self.logger.error(f"Failed to send heartbeat: {e}")
            return {"status": "error", "message": f"Heartbeat failed: {e}"}
    
    async def close_session(self) -> None:
        """Close the current session gracefully."""
        if self.session_id:
            try:
                await self.client.terminate_session(self.session_id)
                self.logger.info(f"Session {self.session_id} terminated.")
            except Exception as e:
                self.logger.warning(f"Failed to terminate session: {e}")
            finally:
                self.session_id = None
    
    async def close(self) -> None:
        """Close the sender and release resources."""
        await self.close_session()
        if hasattr(self.client, 'close'):
            await self.client.close()
    
    # --- Consistency signals ---
    
    async def signal_audit_start(self) -> bool:
        """Signal the start of an audit cycle."""
        return await self.client.signal_audit_start(self.id)
    
    async def signal_audit_end(self) -> bool:
        """Signal the end of an audit cycle."""
        return await self.client.signal_audit_end(self.id)
    
    async def get_sentinel_tasks(self) -> Optional[Dict[str, Any]]:
        """Query for sentinel verification tasks."""
        try:
            return await self.client.get_sentinel_tasks()
        except Exception as e:
            self.logger.debug(f"Failed to get sentinel tasks: {e}")
            return None
    
    async def submit_sentinel_results(self, results: Dict[str, Any]) -> bool:
        """Submit sentinel verification results."""
        try:
            return await self.client.submit_sentinel_feedback(results)
        except Exception as e:
            self.logger.error(f"Failed to submit sentinel results: {e}")
            return False


# Alias for backward compatibility with entry point name
FusionSender = HTTPSender
