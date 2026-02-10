# fusion/src/fustor_fusion/runtime/session_bridge.py
"""
Bridge between FusionPipe and the existing SessionManager.

This module enables gradual migration from the legacy SessionManager
to the Pipe-based session management.

Architecture:
    
    ┌────────────────────────┐
    │   FusionPipe       │
    │   (new architecture)   │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │   PipeSessionBridge│
    │   (integration layer)  │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │   SessionManager       │
    │   (legacy, global)     │
    └────────────────────────┘

Usage:
    from fustor_fusion.runtime import FusionPipe, PipeSessionBridge
    
    pipe = FusionPipe(...)
    bridge = PipeSessionBridge(pipe, session_manager)
    
    # Create session goes through both systems
    session_id = await bridge.create_session(task_id="agent:pipe", ...)
"""
import asyncio
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .fusion_pipe import FusionPipe
    from fustor_fusion.core.session_manager import SessionManager

logger = logging.getLogger("fustor_fusion.session_bridge")


class PipeSessionBridge:
    """
    Bridge that synchronizes sessions between FusionPipe and SessionManager.
    
    This allows:
    1. Gradual migration without breaking existing code
    2. Session state shared between Pipe and legacy components
    3. Eventual deprecation of SessionManager global singleton
    """
    
    def __init__(
        self,
        pipe: "FusionPipe",
        session_manager: "SessionManager"
    ):
        """
        Initialize the bridge.
        
        Args:
            pipe: FusionPipe instance
            session_manager: Legacy SessionManager instance
        """
        self._pipe = pipe
        self._session_manager = session_manager
        
        # Map session_id -> view_id (for legacy compatibility)
        self._session_view_map: Dict[str, str] = {}
        
        # Performance: Cache authoritative leader status per view
        # {view_id: set(session_id)}
        self._leader_cache: Dict[str, set] = {}
    
    async def create_session(
        self,
        task_id: str,
        client_ip: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        allow_concurrent_push: Optional[bool] = None,
        session_id: Optional[str] = None,
        source_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a session in both Pipe and SessionManager.
        
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
        view_id = str(self._pipe.view_id)
        
        # Use pipe config if not explicitly overridden
        if allow_concurrent_push is None:
            allow_concurrent_push = getattr(self._pipe, 'allow_concurrent_push', True)

        from fustor_fusion.view_state_manager import view_state_manager
        
        # We need to handle ALL views associated with this pipe
        all_view_ids = {view_id} # Start with primary
        if hasattr(self._pipe, '_view_handlers'):
            for h in self._pipe._view_handlers.values():
                h_view_id = getattr(h, 'view_id', None)
                if not h_view_id and hasattr(h, 'manager'):
                    h_view_id = getattr(h.manager, 'view_id', None)
                if h_view_id:
                    all_view_ids.add(str(h_view_id))

        is_leader = False
        # Try to become leader for the primary view first
        is_leader = await view_state_manager.try_become_leader(view_id, session_id)
        
        # For all views, we need to ensure we have a role and respect concurrent push
        for v_id in all_view_ids:
            v_is_leader = await view_state_manager.try_become_leader(v_id, session_id)
            
            if v_id == view_id:
                is_leader = v_is_leader
            
            if not v_is_leader and not allow_concurrent_push:
                leader_sid = await view_state_manager.get_leader(v_id)
                if leader_sid:
                    logger.warning(f"Rejecting session {session_id} for view {v_id}: Locked by leader {leader_sid}")
                    raise ValueError(f"View {v_id} is currently locked by session {leader_sid}. Concurrent push is disabled.")

            if v_is_leader:
                await view_state_manager.set_authoritative_session(v_id, session_id)
                self._leader_cache.setdefault(v_id, set()).add(session_id)
                if not allow_concurrent_push:
                    await view_state_manager.lock_for_session(v_id, session_id)
        
        # Create in legacy SessionManager
        await self._session_manager.create_session_entry(
            view_id=view_id,
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            allow_concurrent_push=allow_concurrent_push,
            session_timeout_seconds=session_timeout_seconds,
            source_uri=source_uri
        )
        
        # Create in Pipe
        # Pass is_leader hint if the pipesupports it
        await self._pipe.on_session_created(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            is_leader=is_leader
        )
        
        # Track mapping
        self._session_view_map[session_id] = view_id
        
        # Get role from pipe
        role = await self._pipe.get_session_role(session_id)
        
        timeout = session_timeout_seconds or 30
        
        return {
            "session_id": session_id,
            "role": role,
            "session_timeout_seconds": timeout,
            "suggested_heartbeat_interval_seconds": timeout // 2,
        }
    
    async def keep_alive(
        self,
        session_id: str,
        client_ip: Optional[str] = None,
        can_realtime: bool = False
    ) -> Dict[str, Any]:
        """
        Keep session alive (heartbeat).
        
        Args:
            session_id: The session to keep alive
            client_ip: Client IP for tracking
            can_realtime: Whether the agent is ready for realtime events
            
        Returns:
            Heartbeat response with role, tasks, etc.
        """
        view_id = self._session_view_map.get(session_id)
        
        if view_id is None:
            # Try to get it from pipe as fallback
            role = await self._pipe.get_session_role(session_id)
            if role is None:
                return {
                    "status": "error",
                    "message": f"Session {session_id} not found",
                    "session_id": session_id
                }
            # If pipe knows it, then we are fine (maybe bridge map lost it but pipe has it)
        else:
            # 1. Update legacy SessionManager
            alive = await self._session_manager.keep_session_alive(
                view_id=view_id,
                session_id=session_id,
                client_ip=client_ip,
                can_realtime=can_realtime
            )
            if not alive:
                return {
                    "status": "error",
                    "message": f"Session {session_id} expired in SessionManager",
                    "session_id": session_id
                }
            
            # 2. Try to become leader (Follower promotion) if not known leader
            from fustor_fusion.view_state_manager import view_state_manager
            
            is_known_leader = session_id in self._leader_cache.get(view_id, set())
            if not is_known_leader:
                is_leader = await view_state_manager.try_become_leader(view_id, session_id)
                if is_leader:
                    await view_state_manager.set_authoritative_session(view_id, session_id)
                    self._leader_cache.setdefault(view_id, set()).add(session_id)
        
        # 3. Get final status from pipe
        role = await self._pipe.get_session_role(session_id)
        timeout = self._pipe.config.get("session_timeout_seconds", 30)
        
        return {
            "role": role,
            "session_id": session_id,
            "can_realtime": can_realtime,
            "suggested_heartbeat_interval_seconds": timeout // 2,
            "status": "ok"
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
            
            # Remove from leader cache
            if view_id in self._leader_cache and session_id in self._leader_cache[view_id]:
                self._leader_cache[view_id].discard(session_id)
        
        # Close in Pipe
        await self._pipe.on_session_closed(session_id)
        
        return True
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session info from both systems.
        
        Priority: Pipe session info, fallback to SessionManager.
        """
        # Try Pipe first
        info = await self._pipe.get_session_info(session_id)
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
        return await self._pipe.get_all_sessions()
    
    @property
    def leader_session(self) -> Optional[str]:
        """Get the current leader session ID."""
        return self._pipe.leader_session


def create_session_bridge(
    pipe: "FusionPipe",
    session_manager: Optional["SessionManager"] = None
) -> PipeSessionBridge:
    """
    Create a session bridge for the given pipe.
    
    Args:
        pipe: The FusionPipe instance
        session_manager: Optional SessionManager, uses global if not provided
        
    Returns:
        PipeSessionBridge instance
    """
    if session_manager is None:
        from fustor_fusion.core.session_manager import session_manager as global_session_manager
        session_manager = global_session_manager
    
    return PipeSessionBridge(pipe, session_manager)
