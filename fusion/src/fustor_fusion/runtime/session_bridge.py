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
    session_id = await bridge.create_session(task_id="agent:pipeline", ...)
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
        
        # Map session_id -> view_id (for legacy compatibility)
        self._session_view_map: Dict[str, str] = {}
    
    async def create_session(
        self,
        task_id: str,
        client_ip: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        allow_concurrent_push: Optional[bool] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a session in both Pipeline and SessionManager.
        
        Args:
            task_id: Agent task ID
            client_ip: Client IP address
            session_timeout_seconds: Session timeout
            allow_concurrent_push: Whether to allow concurrent push
            session_id: Optional session ID to use (if already generated)
            
        Returns:
            Session info dict with session_id, role, etc.
        """
        import uuid
        import time
        
        session_id = session_id or str(uuid.uuid4())
        view_id = str(self._pipeline.view_id)
        
        from fustor_fusion.view_state_manager import view_state_manager
        
        # Leader/Follower election (First-Come-First-Serve)
        # Note: view_state_manager should handle string IDs too
        is_leader = await view_state_manager.try_become_leader(view_id, session_id)
        if is_leader:
            await view_state_manager.set_authoritative_session(view_id, session_id)
            if not allow_concurrent_push:
                await view_state_manager.lock_for_session(view_id, session_id)
        
        # Create in legacy SessionManager
        await self._session_manager.create_session_entry(
            view_id=view_id,
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            allow_concurrent_push=allow_concurrent_push,
            session_timeout_seconds=session_timeout_seconds
        )
        
        # Create in Pipeline
        # Pass is_leader hint if the pipelinesupports it
        await self._pipeline.on_session_created(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            is_leader=is_leader
        )
        
        # Track mapping
        self._session_view_map[session_id] = view_id
        
        # Get role from pipeline
        role = await self._pipeline.get_session_role(session_id)
        
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
        view_id = self._session_view_map.get(session_id)
        
        if view_id is not None:
            # Update legacy SessionManager
            await self._session_manager.keep_session_alive(
                view_id=view_id,
                session_id=session_id,
                client_ip=client_ip
            )
        
        # Get role from pipeline
        role = await self._pipeline.get_session_role(session_id)
        
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
        view_id = self._session_view_map.get(session_id)
        
        if view_id is not None:
            # Remove from legacy SessionManager and release locks/leader
            await self._session_manager.terminate_session(
                view_id=view_id,
                session_id=session_id
            )
            
            # Explicitly release leader/lock just in case terminate_session didn't cover everything
            from fustor_fusion.view_state_manager import view_state_manager
            await view_state_manager.unlock_for_session(view_id, session_id)
            await view_state_manager.release_leader(view_id, session_id)
            
            if session_id in self._session_view_map:
                del self._session_view_map[session_id]
        
        # Close in Pipeline
        await self._pipeline.on_session_closed(session_id)
        
        return True
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session info from both systems.
        
        Priority: Pipeline session info, fallback to SessionManager.
        """
        # Try Pipeline first
        info = await self._pipeline.get_session_info(session_id)
        if info:
            return info
        
        # Fallback to legacy
        view_id = self._session_view_map.get(session_id)
        if view_id is not None:
            legacy_info = await self._session_manager.get_session_info(view_id, session_id)
            if legacy_info:
                # Map legacy SessionInfo to dict for consistency
                return {
                    "session_id": legacy_info.session_id,
                    "task_id": legacy_info.task_id,
                    "client_ip": legacy_info.client_ip,
                    "role": "unknown", # Legacy doesn't track role in same way
                }
        
        return None
    
    async def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions."""
        return await self._pipeline.get_all_sessions()
    
    @property
    def leader_session(self) -> Optional[str]:
        """Get the current leader session ID."""
        return self._pipeline.leader_session


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
