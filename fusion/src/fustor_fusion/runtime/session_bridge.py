# fusion/src/fustor_fusion/runtime/session_bridge.py
"""
Bridge between FusionPipeline and the existing SessionManager.

This module enables gradual migration from the legacy SessionManager
to the Pipeline-based session management.

Architecture:
    
    ┌────────────────────────┐
    │   FusionPipeline       │
    │   (new architecture)   │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │   PipelineSessionBridge│
    │   (integration layer)  │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │   SessionManager       │
    │   (legacy, global)     │
    └────────────────────────┘

Usage:
    from fustor_fusion.runtime import FusionPipeline, PipelineSessionBridge
    
    pipeline = FusionPipeline(...)
    bridge = PipelineSessionBridge(pipeline, session_manager)
    
    # Create session goes through both systems
    session_id = await bridge.create_session(task_id="agent:sync", ...)
"""
import asyncio
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .fusion_pipeline import FusionPipeline
    from fustor_fusion.core.session_manager import SessionManager

logger = logging.getLogger("fustor_fusion.session_bridge")


class PipelineSessionBridge:
    """
    Bridge that synchronizes sessions between FusionPipeline and SessionManager.
    
    This allows:
    1. Gradual migration without breaking existing code
    2. Session state shared between Pipeline and legacy components
    3. Eventual deprecation of SessionManager global singleton
    """
    
    def __init__(
        self,
        pipeline: "FusionPipeline",
        session_manager: "SessionManager"
    ):
        """
        Initialize the bridge.
        
        Args:
            pipeline: FusionPipeline instance
            session_manager: Legacy SessionManager instance
        """
        self._pipeline = pipeline
        self._session_manager = session_manager
        
        # Map session_id -> datastore_id (for legacy compatibility)
        self._session_datastore_map: Dict[str, int] = {}
    
    async def create_session(
        self,
        task_id: str,
        client_ip: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        allow_concurrent_push: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Create a session in both Pipeline and SessionManager.
        
        Args:
            task_id: Agent task ID
            client_ip: Client IP address
            session_timeout_seconds: Session timeout
            allow_concurrent_push: Whether to allow concurrent push
            
        Returns:
            Session info dict with session_id, role, etc.
        """
        import uuid
        import time
        
        session_id = str(uuid.uuid4())
        datastore_id = int(self._pipeline.datastore_id)
        
        # Create in legacy SessionManager
        self._session_manager.create_session_entry(
            datastore_id=datastore_id,
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            allow_concurrent_push=allow_concurrent_push,
            session_timeout_seconds=session_timeout_seconds
        )
        
        # Create in Pipeline
        await self._pipeline.on_session_created(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip
        )
        
        # Track mapping
        self._session_datastore_map[session_id] = datastore_id
        
        # Get role from pipeline
        role = self._pipeline.get_session_role(session_id)
        
        return {
            "session_id": session_id,
            "role": role,
            "timeout_seconds": session_timeout_seconds or 30,
        }
    
    async def keep_alive(
        self,
        session_id: str,
        client_ip: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Keep session alive (heartbeat).
        
        Args:
            session_id: The session to keep alive
            client_ip: Client IP for tracking
            
        Returns:
            Heartbeat response with role, tasks, etc.
        """
        datastore_id = self._session_datastore_map.get(session_id)
        
        if datastore_id is not None:
            # Update legacy SessionManager
            self._session_manager.keep_session_alive(
                datastore_id=datastore_id,
                session_id=session_id,
                client_ip=client_ip
            )
        
        # Get role from pipeline
        role = self._pipeline.get_session_role(session_id)
        
        return {
            "role": role,
            "session_id": session_id,
        }
    
    async def close_session(self, session_id: str) -> bool:
        """
        Close a session in both systems.
        
        Args:
            session_id: The session to close
            
        Returns:
            True if successfully closed
        """
        datastore_id = self._session_datastore_map.get(session_id)
        
        if datastore_id is not None:
            # Remove from legacy SessionManager
            self._session_manager.remove_session(
                datastore_id=datastore_id,
                session_id=session_id
            )
            del self._session_datastore_map[session_id]
        
        # Close in Pipeline
        await self._pipeline.on_session_closed(session_id)
        
        return True
    
    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session info from both systems.
        
        Priority: Pipeline session info, fallback to SessionManager.
        """
        # Try Pipeline first
        if session_id in self._pipeline._active_sessions:
            info = self._pipeline._active_sessions[session_id].copy()
            info["session_id"] = session_id
            return info
        
        # Fallback to legacy
        datastore_id = self._session_datastore_map.get(session_id)
        if datastore_id is not None:
            return self._session_manager.get_session_info(datastore_id, session_id)
        
        return None
    
    def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions."""
        return self._pipeline._active_sessions.copy()
    
    @property
    def leader_session(self) -> Optional[str]:
        """Get the current leader session ID."""
        return self._pipeline._leader_session


def create_session_bridge(
    pipeline: "FusionPipeline",
    session_manager: Optional["SessionManager"] = None
) -> PipelineSessionBridge:
    """
    Create a session bridge for the given pipeline.
    
    Args:
        pipeline: The FusionPipeline instance
        session_manager: Optional SessionManager, uses global if not provided
        
    Returns:
        PipelineSessionBridge instance
    """
    if session_manager is None:
        from fustor_fusion.core.session_manager import session_manager as global_session_manager
        session_manager = global_session_manager
    
    return PipelineSessionBridge(pipeline, session_manager)
