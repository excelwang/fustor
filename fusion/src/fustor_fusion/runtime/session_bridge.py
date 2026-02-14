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
        
        # ForestView support: Per-tree leader election
        # Only apply scoping if the driver strictly requires it (e.g. ForestFSViewDriver)
        # Otherwise standard views (FSViewDriver) must use the raw view_id for consistency.
        pipe_id = getattr(self._pipe, 'pipe_id', None)
        election_view_id = view_id
        
        if pipe_id:
            # Check if driver opts-in for scoped election
            # Path: Pipe -> Handler -> Manager -> Driver
            should_scope = False
            handler = self._pipe.get_view_handler(view_id)
            if handler:
                # Try to find the driver in common adapter structures
                driver = getattr(handler, 'driver', None)
                if not driver and hasattr(handler, 'manager'):
                    driver = getattr(handler.manager, 'driver', None)
                
                # Check flag
                if driver:
                    should_scope = getattr(driver, 'use_scoped_session_leader', False)
            
            if should_scope:
                 election_view_id = f"{view_id}:{pipe_id}"
        
        
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
        is_leader = await view_state_manager.try_become_leader(election_view_id, session_id)
        
        # For all views, we need to ensure we have a role and respect concurrent push
        for v_id in all_view_ids:
            # Apply the same scoping logic for other views if they are forest views?
            # For simplicity, we assume secondary views follow standard logic or
            # user must use primary view for Forest features.
            v_is_leader = await view_state_manager.try_become_leader(v_id, session_id)
            
            if v_id == view_id:
                # If v_id matches primary, we already did it with election_view_id?
                # current implementation of all_view_ids includes primary view_id.
                # But we used election_view_id for primary.
                # So we should skip v_id == view_id in this loop if we want to rely on the scoped one.
                pass 
            
            # Actually, the loop logic is a bit flawed for ForestView.
            # ForestView has 1 view_id but N trees. 
            # We only care about OUR tree (election_view_id).
            # The all_view_ids loop is for "Composite" view handlers (legacy).
            # Let's keep it but skip the primary view_id from loop to avoid double-locking wrong key.
            if v_id == view_id:
                 continue

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

        # Handle the Primary Election ID (Scoped)
        if is_leader:
            await view_state_manager.set_authoritative_session(election_view_id, session_id)
            self._leader_cache.setdefault(election_view_id, set()).add(session_id)
            if not allow_concurrent_push:
                await view_state_manager.lock_for_session(election_view_id, session_id)
        
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
        pipe_id = getattr(self._pipe, 'pipe_id', None)
        
        election_view_id = view_id
        if pipe_id:
            should_scope = False
            handler = self._pipe.get_view_handler(view_id)
            if handler:
                driver = getattr(handler, 'driver', None)
                if not driver and hasattr(handler, 'manager'):
                    driver = getattr(handler.manager, 'driver', None)
                if driver:
                    should_scope = getattr(driver, 'use_scoped_session_leader', False)
            
            if should_scope:
                election_view_id = f"{view_id}:{pipe_id}"
        
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
        from fustor_fusion.view_state_manager import view_state_manager
        
        # Check against the election_view_id
        is_known_leader = session_id in self._leader_cache.get(election_view_id, set())
        
        # Periodic validation: every N heartbeats, verify cache against actual state
        count = self._heartbeat_count.get(session_id, 0) + 1
        self._heartbeat_count[session_id] = count
        if is_known_leader and count % self._LEADER_VERIFY_INTERVAL == 0:
            actual_leader = await view_state_manager.is_leader(election_view_id, session_id)
            if not actual_leader:
                logger.debug(f"Leader cache stale for {session_id} on view {election_view_id}, clearing")
                self._leader_cache.get(election_view_id, set()).discard(session_id)
                is_known_leader = False
        
        if not is_known_leader:
            # Try to become leader on the scoped view ID
            is_leader = await view_state_manager.try_become_leader(election_view_id, session_id)
            if is_leader:
                await view_state_manager.set_authoritative_session(election_view_id, session_id)
                self._leader_cache.setdefault(election_view_id, set()).add(session_id)
    
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
        pipe_id = getattr(self._pipe, 'pipe_id', None)
        
        election_view_id = view_id
        if pipe_id:
            should_scope = False
            handler = self._pipe.get_view_handler(view_id)
            if handler:
                driver = getattr(handler, 'driver', None)
                if not driver and hasattr(handler, 'manager'):
                    driver = getattr(handler.manager, 'driver', None)
                if driver:
                    should_scope = getattr(driver, 'use_scoped_session_leader', False)
            
            if should_scope:
                election_view_id = f"{view_id}:{pipe_id}"
        
        # Remove from legacy SessionManager and release locks/leader
        await self._session_manager.terminate_session(
            view_id=view_id,
            session_id=session_id
        )
        
        # Explicitly release leader/lock just in case terminate_session didn't cover everything
        from fustor_fusion.view_state_manager import view_state_manager
        
        # Unlock SCOPED view
        await view_state_manager.unlock_for_session(election_view_id, session_id)
        await view_state_manager.release_leader(election_view_id, session_id)
        
        # Remove from leader cache
        if election_view_id in self._leader_cache and session_id in self._leader_cache[election_view_id]:
            self._leader_cache[election_view_id].discard(session_id)
        
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
