# fusion/src/fustor_fusion/runtime/session_bridge.py
"""
Bridge between FusionPipe and the existing SessionManager.

This module connects the Pipe Runtime with the SessionManager
for session lifecycle management.

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
        
        # Performance: Cache authoritative leader status per view
        # {view_id: set(session_id)}
        self._leader_cache: Dict[str, set] = {}
        
        # Heartbeat counter for periodic leader validation
        # Every N heartbeats, verify cache against actual VSM state
        self._heartbeat_count: Dict[str, int] = {}  # session_id -> count
        self._LEADER_VERIFY_INTERVAL = 5  # Verify every N heartbeats
    
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
        
        # Delegate election and role determination to the View Handler
        # This decouples the bridge from specific consistency models (forest vs standard)
        view_handler = self._pipe.get_view_handler(view_id)
        
        # Fallback for ViewManagerAdapter which might have a prefixed handler_id
        if not view_handler:
            view_handler = self._pipe.find_handler_for_view(view_id)
            
        session_result = {"role": "leader"} # Default fallback
        
        if view_handler:
             try:
                 session_result = await view_handler.resolve_session_role(session_id, fusion_pipe_id=self._pipe.id)
             except Exception as e:
                 logger.error(f"View handler failed to process new session {session_id}: {e}")
                 # Should we fail the session? For now, log and proceed as follower for safety
                 session_result = {"role": "follower"}

        is_leader = (session_result.get("role") == "leader")
        election_key = session_result.get("election_key", view_id)
        
        # If the view decided we are leader, we might need to lock for concurrent push
        # (The view might have already done set_authoritative_session, but bridge manages concurrency lock)
        if is_leader:
             self._leader_cache.setdefault(election_key, set()).add(session_id)
             if not allow_concurrent_push:
                 # We lock on the election key provided by the view (could be scoped or global)
                 await view_state_manager.lock_for_session(election_key, session_id)
        
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
        
        # Get role from pipe
        role = await self._pipe.get_session_role(session_id)
        
        timeout = session_timeout_seconds or 30
        
        return {
            "session_id": session_id,
            "role": role,
            "session_timeout_seconds": timeout,
        }
    
    async def keep_alive(
        self,
        session_id: str,
        client_ip: Optional[str] = None,
        can_realtime: bool = False,
        agent_status: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Keep session alive (heartbeat).
        
        Args:
            session_id: The session to keep alive
            client_ip: Client IP for tracking
            can_realtime: Whether the agent is ready for realtime events
            agent_status: Status report from the agent
            
        Returns:
            Heartbeat response with role, tasks, etc.
        """
        view_id = str(self._pipe.view_id)
        pipe_id = self._pipe.id
        
        # No legacy flag check needed here. 
        # Election status is maintained via _leader_cache and verified/retried below.
        
        commands = []
        
        # 1. Update legacy SessionManager
        alive, commands = await self._session_manager.keep_session_alive(
            view_id=view_id,
            session_id=session_id,
            client_ip=client_ip,
            can_realtime=can_realtime,
            agent_status=agent_status
        )
        if not alive:
            return {
                "status": "error",
                "message": f"Session {session_id} expired in SessionManager",
                "session_id": session_id
            }
        
        # 2. Update Pipe
        await self._pipe.keep_session_alive(
            session_id, 
            can_realtime=can_realtime, 
            agent_status=agent_status
        )
        
        # 3. Try to become leader (Follower promotion) if not known leader
        # We delegate this to resolve_session_role again (idempotent "try become leader" check)
        
        # Periodic validation/retry: every N heartbeats
        count = self._heartbeat_count.get(session_id, 0) + 1
        self._heartbeat_count[session_id] = count
        
        if count % self._LEADER_VERIFY_INTERVAL == 0:
            view_handler = self._pipe.get_view_handler(view_id)
            if not view_handler:
                view_handler = self._pipe.find_handler_for_view(view_id)
            if view_handler:
                 try:
                     # Re-run election logic via handler
                     res = await view_handler.resolve_session_role(session_id, pipe_id=self._pipe.id)
                     is_leader = (res.get("role") == "leader")
                     election_key = res.get("election_key", view_id)
                     
                     if is_leader:
                         self._leader_cache.setdefault(election_key, set()).add(session_id)
                     else:
                         # Demoted or failed to promote - ensure we don't have stale cache
                         if election_key in self._leader_cache:
                             self._leader_cache[election_key].discard(session_id)
                             
                 except Exception as e:
                     logger.debug(f"Leader promotion check failed during keep_alive: {e}")
    
        # 3. Get final status from pipe
        role = await self._pipe.get_session_role(session_id)
        timeout = self._pipe.config.get("session_timeout_seconds", 30)
        
        return {
            "role": role,
            "session_id": session_id,
            "can_realtime": can_realtime,
            "status": "ok",
            "commands": commands
        }
    
    async def close_session(self, session_id: str) -> bool:
        """
        Close a session in both systems.
        
        Args:
            session_id: The session to close
            
        Returns:
            True if successfully closed
        """
        view_id = str(self._pipe.view_id)
        # Explicitly release leader/lock just in case terminate_session didn't cover everything
        from fustor_fusion.view_state_manager import view_state_manager
        
        # We don't know the exact election keys used (scoped vs global) without asking Handler.
        # But we tracked them in self._leader_cache!
        # Clean up ANY key where this session was leader.
        
        keys_to_clean = []
        for key, sessions in self._leader_cache.items():
            if session_id in sessions:
                keys_to_clean.append(key)
                
        # Remove from legacy SessionManager
        await self._session_manager.terminate_session(
            view_id=view_id,
            session_id=session_id
        )

        for key in keys_to_clean:
            await view_state_manager.unlock_for_session(key, session_id)
            await view_state_manager.release_leader(key, session_id)
            self._leader_cache[key].discard(session_id)
        
        # Remove heartbeat counter
        self._heartbeat_count.pop(session_id, None)
        
        # Close in Pipe
        await self._pipe.on_session_closed(session_id)
        
        return True
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session info from Pipe.
        """
        # Try Pipe first
        info = await self._pipe.get_session_info(session_id)
        return info
    
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
