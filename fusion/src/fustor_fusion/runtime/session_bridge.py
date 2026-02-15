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
from typing import Any, Dict, Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .fusion_pipe import FusionPipe
    from fustor_fusion.core.session_manager import SessionManager

logger = logging.getLogger("fustor_fusion.session_bridge")


class PipeSessionStore:
    """
    Dedicated store for session-related state of a FusionPipe.
    
    This object is owned by the PipeSessionBridge and updated synchronously
    during session creation/closure to ensure the control plane has 
    an immediate, consistent view of active sessions and lineages.
    """
    
    def __init__(self, view_ids: List[str]):
        self.view_ids = view_ids
        # {session_id: {agent_id, source_uri}}
        self.lineage_cache: Dict[str, Dict[str, str]] = {}
        # {election_key: set(session_id)}
        self.leader_cache: Dict[str, set] = {}
        # session_id -> heartbeat_count
        self.heartbeat_count: Dict[str, int] = {}
        
    def add_session(self, session_id: str, lineage: Dict[str, str]):
        """Synchronously add session metadata."""
        self.lineage_cache[session_id] = lineage
        self.heartbeat_count[session_id] = 0
        
    def remove_session(self, session_id: str):
        """Synchronously remove session metadata."""
        self.lineage_cache.pop(session_id, None)
        self.heartbeat_count.pop(session_id, None)
        # Remove from leader cache
        for key in list(self.leader_cache.keys()):
            self.leader_cache[key].discard(session_id)
            if not self.leader_cache[key]:
                del self.leader_cache[key]

    def get_lineage(self, session_id: str) -> Dict[str, str]:
        """Get lineage info for an active session."""
        return self.lineage_cache.get(session_id, {})

    def is_leader(self, session_id: str, election_key: str) -> bool:
        """Check if a session is currently recorded as leader for a key."""
        return session_id in self.leader_cache.get(election_key, set())

    def is_any_leader(self, session_id: str) -> bool:
        """Check if session is leader for ANY election key tracked for this pipe."""
        for sessions in self.leader_cache.values():
            if session_id in sessions:
                return True
        return False

    def record_leader(self, session_id: str, election_key: str):
        """Record leadership status."""
        self.leader_cache.setdefault(election_key, set()).add(session_id)

    def record_heartbeat(self, session_id: str) -> int:
        """Increment and return heartbeat count."""
        count = self.heartbeat_count.get(session_id, 0) + 1
        self.heartbeat_count[session_id] = count
        return count


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
        
        # Link bridge back to pipe for role lookups
        pipe.session_bridge = self
        
        # New State Management (GAP-4)
        self.store = PipeSessionStore(pipe.view_ids)
        
        # Event queue for asynchronous handler notifications in FusionPipe
        self.event_queue = asyncio.Queue()
        
        # Command/Response Correlation (GAP-P0-3)
        # {command_id: Future}
        self._pending_commands: Dict[str, asyncio.Future] = {} 
        
        self._LEADER_VERIFY_INTERVAL = 5  # Verify every N heartbeats
    
    async def create_session(
        self,
        task_id: str,
        client_ip: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None,
        allow_concurrent_push: Optional[bool] = None,
        session_id: Optional[str] = None,
        source_uri: Optional[str] = None, **kwargs
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
        
        # Use pipe config if not explicitly overridden
        if allow_concurrent_push is None:
            allow_concurrent_push = getattr(self._pipe, 'allow_concurrent_push', True)

        # Find a view handler that can resolve leadership.
        # In M:N, we might have multiple handlers. We pick the first valid one or broadcast.
        # For now, we use the first view's handler as the authoritative one for the pipe's role.
        view_id = self._pipe.view_ids[0]
        view_handler = self._pipe.get_view_handler(view_id)
        
        # Fallback for ViewManagerAdapter which might have a prefixed handler_id
        if not view_handler:
            view_handler = self._pipe.find_handler_for_view(view_id)
        
        session_result = {"role": "leader"} # Default fallback
        
        if view_handler:
             try:
                 session_result = await view_handler.resolve_session_role(session_id, pipe_id=self._pipe.id)
             except Exception as e:
                 logger.error(f"View handler failed to process new session {session_id}: {e}")
                 # Should we fail the session? For now, log and proceed as leader (optimistic)
                 session_result = {"role": "leader"}
        else:
             logger.warning(f"No view handler found for view {view_id}, defaulting to follower role")

        is_leader = (session_result.get("role") == "leader")
        election_key = session_result.get("election_key", view_id)
        
        # If the view decided we are leader, we might need to lock for concurrent push
        if is_leader:
             self.store.record_leader(session_id, election_key)
             if not allow_concurrent_push:
                 from fustor_fusion.view_state_manager import view_state_manager
                 # Check for stale lock first (GAP-1 stabilization)
                 locked_sid = await view_state_manager.get_locked_session_id(election_key)
                 if locked_sid:
                      # If the session holding the lock is not in ANY view's active list
                      # (Checking legacy global session manager for now as it holds all state)
                      # We check globally across all views because election_key might be scoped
                      from fustor_fusion.core.session_manager import session_manager
                      all_active = await session_manager.get_all_active_sessions()
                      is_active = any(locked_sid in view_sess for view_sess in all_active.values())
                      
                      if not is_active:
                           logger.warning(f"View {election_key} locked by stale session {locked_sid}. Auto-unlocking.")
                           await view_state_manager.unlock_for_session(election_key, locked_sid)

                 # We lock on the election key provided by the view (could be scoped or global)
                 locked = await view_state_manager.lock_for_session(election_key, session_id)
                 if not locked:
                      raise ValueError(f"View {election_key} is currently locked by another session")
        
        # Create in legacy SessionManager for EACH view served by this pipe
        for vid in self._pipe.view_ids:
            await self._session_manager.create_session_entry(
                view_id=vid,
                session_id=session_id,
                task_id=task_id,
                client_ip=client_ip,
                allow_concurrent_push=allow_concurrent_push,
                session_timeout_seconds=session_timeout_seconds,
                source_uri=source_uri
            )
        
        # Synchronously record in local store for immediate use in process_events
        lineage = {}
        if source_uri:
            lineage["source_uri"] = source_uri
        if task_id:
            lineage["agent_id"] = task_id.split(":")[0] if ":" in task_id else task_id
            
        self.store.add_session(session_id, lineage)

        # Enqueue background notification for FusionPipe handlers
        # This replaces the blocking on_session_created call
        await self.event_queue.put({
            "type": "create",
            "session_id": session_id,
            "task_id": task_id,
            "is_leader": is_leader,
            "kwargs": kwargs
        })
        
        # Pass is_leader hint if the pipesupports it (for local state update if any)
        # Note: FusionPipe will also consume the queue to notify handlers
        await self._pipe.on_session_created(
            session_id=session_id,
            task_id=task_id,
            client_ip=client_ip,
            is_leader=is_leader
        )
        
        # Get role from pipe (now fast since handlers are backgrounded)
        role = "leader" if is_leader else "follower"
        
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
        pipe_id = self._pipe.id
       
        # No legacy flag check needed here. 
        # Election status is maintained via _leader_cache and verified/retried below.
        
        commands = []
        
        # 1. Update legacy SessionManager for ALL served views
        for vid in self._pipe.view_ids:
            alive, commands_batch = await self._session_manager.keep_session_alive(
                view_id=vid,
                session_id=session_id,
                client_ip=client_ip,
                can_realtime=can_realtime,
                agent_status=agent_status
            )
            if alive:
                # Merge commands from all views (one pipe, multiple views)
                commands.extend(commands_batch)
            else:
                # If it died for one view, we might want to know, but we continue loop
                logger.debug(f"Session {session_id} expired in SessionManager for view {vid}")
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
        count = self.store.record_heartbeat(session_id)
        
        if count % self._LEADER_VERIFY_INTERVAL == 0:
            # Re-verify leadership for all views
            for vid in self._pipe.view_ids:
                view_handler = self._pipe.get_view_handler(vid)
                if not view_handler:
                    view_handler = self._pipe.find_handler_for_view(vid)
                if view_handler:
                     try:
                         # Re-run election logic via handler
                         res = await view_handler.resolve_session_role(session_id, pipe_id=self._pipe.id)
                         is_l = (res.get("role") == "leader")
                         e_key = res.get("election_key", vid)
                         
                         if is_l:
                             self.store.record_leader(session_id, e_key)
                         else:
                             if e_key in self.store.leader_cache:
                                 self.store.leader_cache[e_key].discard(session_id)
                     except Exception as e:
                         logger.debug(f"Leader promotion check failed for view {vid}: {e}")
    
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
        # Explicitly release leader/lock just in case terminate_session didn't cover everything
        from fustor_fusion.view_state_manager import view_state_manager
        
        # We don't know the exact election keys used (scoped vs global) without asking Handler.
        # But we tracked them in self._leader_cache!
        # Clean up ANY key where this session was leader.
        
        # Remove from legacy SessionManager for ALL served views
        for vid in self._pipe.view_ids:
            await self._session_manager.terminate_session(
                view_id=vid,
                session_id=session_id
            )

        # Synchronously clean up local store
        keys_to_clean = []
        for key, sessions in self.store.leader_cache.items():
            if session_id in sessions:
                keys_to_clean.append(key)

        for key in keys_to_clean:
            await view_state_manager.unlock_for_session(key, session_id)
            await view_state_manager.release_leader(key, session_id)
        
        self.store.remove_session(session_id)
        
        # Enqueue background notification for FusionPipe handlers
        await self.event_queue.put({
            "type": "close",
            "session_id": session_id
        })
        
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

    async def send_command_and_wait(
        self, 
        session_id: str, 
        command: str, 
        params: Dict[str, Any], 
        timeout: float = 5.0
    ) -> Dict[str, Any]:
        """
        Send a command to the agent and wait for a response.
        
        This enables synchronous-like API behavior over the async pipe.
        
        Args:
            session_id: Target session ID
            command: Command name (e.g., 'scan_path')
            params: Command parameters
            timeout: Max wait time in seconds
            
        Returns:
            The command result dictionary
            
        Raises:
            asyncio.TimeoutError: If no response received in time
            Exception: If command fails or session invalid
        """
        import uuid
        
        # 1. Generate unique command ID for correlation
        cmd_id = str(uuid.uuid4())
        
        # 2. Register future
        future = asyncio.get_running_loop().create_future()
        self._pending_commands[cmd_id] = future
        
        try:
            # 3. Queue command for next heartbeat (legacy mechanism)
            # OR send directly if we had a push channel (future optimization)
            # Currently we piggy-back on heartbeat response via session_manager
            # Flatten params into the command dict for Agent compatibility
            command_payload = {
                "id": cmd_id,
                "type": command,
                **params
            }
            
            # Queue to all views (redundant but ensures visibility)
            for vid in self._pipe.view_ids:
                logger.debug(f"Queuing command {cmd_id} for session {session_id} on view {vid}")
                await self._session_manager.queue_command(
                    vid, 
                    session_id, 
                    command_payload
                )
           
            # 4. Wait for result
            logger.debug(f"Queued command {command} ({cmd_id}) for session {session_id}, waiting {timeout}s...")
            return await asyncio.wait_for(future, timeout=timeout)
            
        except Exception:
            # Cleanup on error
            if cmd_id in self._pending_commands:
                del self._pending_commands[cmd_id]
            raise
            
    def resolve_command(self, command_id: str, result: Dict[str, Any]):
        """
        Resolve a pending command with a result.
        Called by FusionPipe when it receives a command_result event.
        """
        if command_id in self._pending_commands:
            future = self._pending_commands.pop(command_id)
            if not future.done():
                if result.get("success", True):
                    future.set_result(result)
                else:
                    future.set_exception(Exception(result.get("error", "Command failed")))
        else:
            logger.warning(f"Received result for unknown/expired command {command_id}")



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
