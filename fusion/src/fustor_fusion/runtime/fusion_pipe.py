# fusion/src/fustor_fusion/runtime/fusion_pipe.py
"""
FusionPipe - The V2 Pipe implementation for Fusion.

This Pipe receives events from Agents and dispatches them to ViewHandlers.
It implements the receiver side of the Agent -> Fusion data flow.

Architecture:
=============

    Agent                               Fusion
    ┌─────────────┐                    ┌─────────────────────────────┐
    │AgentPipe│ ─── HTTP/gRPC ───▶ │     FusionPipe          │
    └─────────────┘                    │  ┌─────────────────────┐    │
                                       │  │ ReceiverHandler     │    │
                                       │  │ (session, events)   │    │
                                       │  └──────────┬──────────┘    │
                                       │             │               │
                                       │  ┌──────────▼──────────┐    │
                                       │  │ ViewHandler[]       │    │
                                       │  │ (fs-view, etc.)     │    │
                                       │  └─────────────────────┘    │
                                       └─────────────────────────────┘
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from fustor_core.pipe import Pipe, PipeState
from fustor_core.pipe.handler import ViewHandler
from fustor_core.event import EventBase
from fustor_core.common.metrics import get_metrics
from pydantic import ValidationError

if TYPE_CHECKING:
    from fustor_core.pipe.context import PipeContext

from ..core.session_manager import session_manager

logger = logging.getLogger("fustor_fusion.pipe")


class FusionPipe(Pipe):
    """
    Fusion-side Pipe for receiving and processing events.
    
    This pipe:
    1. Receives events from Agents (via receivers)
    2. Dispatches events to registered ViewHandlers
    3. Manages session lifecycle on the Fusion side
    4. Provides aggregated statistics and data views
    
    Usage:
        from fustor_fusion.runtime import FusionPipe
        
        pipe = FusionPipe(
            pipe_id="view-1",
            config={"view_id": "view-1"},
            view_handlers=[fs_view_handler, ...]
        )
        
        await pipe.start()
        
        # Process incoming events
        await pipe.process_events(events, session_id="...")
        
        # Query views
        data = pipe.get_view("fs")
    """
    
    def __init__(
        self,
        pipe_id: str,
        config: Dict[str, Any],
        view_handlers: Optional[List[ViewHandler]] = None,
        context: Optional["PipeContext"] = None
    ):
        """
        Initialize the FusionPipe.
        
        Args:
            pipe_id: Unique identifier (typically view_id)
            config: Configuration dict containing:
                - view_id: str
            view_handlers: List of ViewHandler instances to dispatch events to
            context: Optional PipeContext for dependency injection
        """
        super().__init__(pipe_id, config, context)
        
        # Terminology: view_id is the primary identifier
        self.view_id = str(config.get("view_id", pipe_id))

        
        self.allow_concurrent_push = config.get("allow_concurrent_push", True)
        self.queue_batch_size = config.get("queue_batch_size", 100)
        
        # View handlers registry
        self._view_handlers: Dict[str, ViewHandler] = {}
        for handler in (view_handlers or []):
            self.register_view_handler(handler)
        # Session tracking - handled centrally via PipeManager/SessionManager
        self._lock = asyncio.Lock()  # Generic lock for pipestate
        
        # Processing task
        self._processing_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._queue_drained = asyncio.Event()  # Signaled when queue becomes empty
        self._queue_drained.set()  # Initially empty
        self._active_pushes = 0 # Track concurrent push requests
        
        # Statistics
        self.statistics = {
            "events_received": 0,
            "events_processed": 0,
            "sessions_created": 0,
            "sessions_closed": 0,
            "errors": 0,
        }
        self._cached_leader_session: Optional[str] = None
        
        # Handler fault isolation
        self._handler_errors: Dict[str, int] = {}  # Per-handler error counts
        self._disabled_handlers: set = set()  # Handlers disabled due to repeated failures
        self._disabled_handlers_timestamps: Dict[str, float] = {} # Timestamp when handler was disabled
        self.HANDLER_TIMEOUT = config.get("handler_timeout", 30.0)  # Seconds
        self.MAX_HANDLER_ERRORS = config.get("max_handler_errors", 50)  # Before disabling
        self.HANDLER_RECOVERY_INTERVAL = config.get("handler_recovery_interval", 60.0) # Seconds cooldown
        
        # Lineage cache: session_id -> {agent_id, source_uri}
        self._session_lineage_cache: Dict[str, Dict[str, str]] = {}
    
    def register_view_handler(self, handler: ViewHandler) -> None:
        """
        Register a view handler for processing events.
        
        Args:
            handler: ViewHandler instance
        """
        handler_id = handler.id
        self._view_handlers[handler_id] = handler
        logger.debug(f"Registered view handler: {handler_id}")
    
    def get_view_handler(self, handler_id: str) -> Optional[ViewHandler]:
        """Get a registered view handler by ID."""
        return self._view_handlers.get(handler_id)
    
    def get_available_views(self) -> List[str]:
        """Get list of available view handler IDs."""
        return list(self._view_handlers.keys())
    
    async def start(self) -> None:
        """Start the pipe and begin processing events."""
        if self.is_running():
            logger.warning(f"Pipe {self.id} is already running")
            return
        
        logger.info(f"Starting FusionPipe {self.id}")
        self._set_state(PipeState.RUNNING, "Pipe started")
        
        # Initialize all handlers
        for handler in self._view_handlers.values():
            if hasattr(handler, 'initialize'):
                await handler.initialize()
        
        # Start processing task
        self._processing_task = asyncio.create_task(
            self._processing_loop(),
            name=f"fusion-pipe-{self.id}"
        )
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(
            self._session_cleanup_loop(),
            name=f"fusion-pipe-cleanup-{self.id}"
        )
        
        logger.info(f"FusionPipe {self.id} started with {len(self._view_handlers)} view handlers")
    
    async def stop(self) -> None:
        """Stop the pipe."""
        if not self.is_running():
            return
        
        logger.info(f"Stopping FusionPipe {self.id}")
        self._set_state(PipeState.PAUSED, "Pipe stopping")
        
        # Cancel processing task
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await asyncio.wait_for(self._processing_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._processing_task = None
        
        # Close all handlers
        for handler in self._view_handlers.values():
            if hasattr(handler, 'close'):
                await handler.close()
        
        from ..core.session_manager import session_manager
        
        # Clear sessions for this view
        await session_manager.clear_all_sessions(self.view_id)
        
        self._set_state(PipeState.STOPPED, "Pipe stopped")
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        logger.info(f"FusionPipe {self.id} stopped")
    
    async def _processing_loop(self) -> None:
        """Background loop for processing queued events."""
        logger.debug(f"Processing loop started for pipe {self.id}")
        
        while True:
            try:
                # Wait for events in queue
                event_batch = await self._event_queue.get()
                
                if event_batch is None:  # Shutdown signal
                    break
                
                # Process each event
                t0 = time.time()
                for event in event_batch:
                    await self._dispatch_to_handlers(event)
                    self.statistics["events_processed"] += 1
                
                duration = time.time() - t0
                get_metrics().counter("fustor.fusion.events_processed", len(event_batch), {"pipe": self.id})
                get_metrics().histogram("fustor.fusion.processing_latency", duration, {"pipe": self.id})
                
                self._event_queue.task_done()
                
                # Signal queue drain if empty and no pushes active
                if self._event_queue.empty():
                    async with self._lock:
                        if self._active_pushes == 0:
                            self._queue_drained.set()
                
            except asyncio.CancelledError:
                logger.debug(f"Processing loop cancelled for pipe {self.id}")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                self.statistics["errors"] += 1
                await asyncio.sleep(0.1)
    
    async def _dispatch_to_handlers(self, event: EventBase) -> None:
        """
        Dispatch an event to matching view handlers with fault isolation.
        """
        for handler_id, handler in self._view_handlers.items():
            # 1. Schema Routing Check
            # If handler has a specific schema_name, it must match the event's schema.
            # Empty schema_name means it's a generic handler (like ViewManagerAdapter).
            if handler.schema_name and handler.schema_name != event.event_schema:
                if handler.schema_name != "view-manager": # Special case for aggregator
                    continue

            # Skip disabled handlers unless they have recovered
            if handler_id in self._disabled_handlers:
                if not self._attempt_handler_recovery(handler_id):
                    continue
                
            try:
                if hasattr(handler, 'process_event'):
                    # Apply timeout protection
                    try:
                        success = await asyncio.wait_for(
                            handler.process_event(event),
                            timeout=self.HANDLER_TIMEOUT
                        )
                        if not success:
                            self._record_handler_error(handler_id, "Returned False")
                            logger.warning(f"Handler {handler_id} returned False for event processing")
                    except asyncio.TimeoutError:
                        self._record_handler_error(handler_id, f"Timeout after {self.HANDLER_TIMEOUT}s")
                        logger.error(f"Handler {handler_id} timed out processing event")
            except Exception as e:
                self._record_handler_error(handler_id, str(e))
                logger.error(f"Error in handler {handler_id}: {e}", exc_info=True)

    def _attempt_handler_recovery(self, handler_id: str) -> bool:
        """Check if a disabled handler can be re-enabled."""
        last_disabled = self._disabled_handlers_timestamps.get(handler_id, 0)
        if time.time() - last_disabled > self.HANDLER_RECOVERY_INTERVAL:
            self._disabled_handlers.remove(handler_id)
            # Reset error count to give it a fresh start
            self._handler_errors[handler_id] = 0
            if handler_id in self._disabled_handlers_timestamps:
                del self._disabled_handlers_timestamps[handler_id]
            logger.debug(f"Handler {handler_id} re-enabled after cooldown period.")
            return True
        return False
    
    def _record_handler_error(self, handler_id: str, reason: str) -> None:
        """Record a handler error and disable if threshold exceeded."""
        self.statistics["errors"] += 1
        self._handler_errors[handler_id] = self._handler_errors.get(handler_id, 0) + 1
        
        if self._handler_errors[handler_id] >= self.MAX_HANDLER_ERRORS:
            if handler_id not in self._disabled_handlers:
                self._disabled_handlers.add(handler_id)
                self._disabled_handlers_timestamps[handler_id] = time.time()
                logger.warning(
                    f"Handler {handler_id} disabled after {self._handler_errors[handler_id]} errors. "
                    f"Last error: {reason}"
                )
    
    
    # --- Session Management ---
    
    async def on_session_created(
        self, 
        session_id: str, 
        task_id: Optional[str] = None, 
        is_leader: bool = False,
        **kwargs
    ) -> None:
        """
        Handle session creation notification.
        
        Args:
            session_id: The ID of the created session
            task_id: Originating task ID
            is_leader: Whether this session has been elected as leader (by bridge)
            **kwargs: Additional metadata
        """
        # Note: Session creation in backing store is handled by PipeSessionBridge
        
        self.statistics["sessions_created"] += 1
        
        # Build lineage cache for this session
        session_info = await session_manager.get_session_info(self.view_id, session_id)
        if session_info:
            lineage = {}
            if session_info.source_uri:
                lineage["source_uri"] = session_info.source_uri
            if session_info.task_id and ":" in session_info.task_id:
                lineage["agent_id"] = session_info.task_id.split(":")[0]
            elif session_info.task_id:
                lineage["agent_id"] = session_info.task_id
            if lineage:
                self._session_lineage_cache[session_id] = lineage
        
        # Notify view handlers of session start
        for handler in self._view_handlers.values():
            if hasattr(handler, 'on_session_start'):
                await handler.on_session_start()
        
        if is_leader:
            self._cached_leader_session = session_id
            
        logger.info(f"Session {session_id} created for view {self.view_id} (role={'leader' if is_leader else 'follower'})")
    
    async def on_session_closed(self, session_id: str) -> None:
        """
        Handle session closure notification.
        """
        # Note: Session removal from backing store is handled by PipeSessionBridge
        
        self.statistics["sessions_closed"] += 1
        
        # Clean lineage cache
        self._session_lineage_cache.pop(session_id, None)
        
        from ..view_state_manager import view_state_manager

        if self._cached_leader_session == session_id:
            self._cached_leader_session = None

        # New Leader Election (Architecture V2)
        # If a session closes, we need to ensure there is a leader if sessions remain.
        # We query the session_manager (source of truth) for remaining sessions.
        remaining_sessions = await session_manager.get_view_sessions(self.view_id)
        if remaining_sessions:
            current_leader = await view_state_manager.get_leader(self.view_id)
            if not current_leader:
                # No leader, try to elect from remaining
                for sid in remaining_sessions.keys():
                    if await view_state_manager.try_become_leader(self.view_id, sid):
                        logger.info(f"New leader elected for view {self.view_id} after session close: {sid}")
                        self._cached_leader_session = sid
                        break
            else:
                self._cached_leader_session = current_leader
        
        # Notify view handlers of session close
        for handler in self._view_handlers.values():
            if hasattr(handler, 'on_session_close'):
                await handler.on_session_close()
        
        logger.info(f"Session {session_id} closed for view {self.view_id}")
    
    async def keep_session_alive(self, session_id: str, can_realtime: bool = False, agent_status: Optional[Dict[str, Any]] = None) -> bool:
        """Update last activity for a session."""
        from ..core.session_manager import session_manager
        si = await session_manager.keep_session_alive(
            self.view_id, 
            session_id, 
            can_realtime=can_realtime, 
            agent_status=agent_status
        )
        return si is not None

    async def get_session_role(self, session_id: str) -> str:
        """Get the role of a session (leader/follower)."""
        from ..view_state_manager import view_state_manager
        is_leader = await view_state_manager.is_leader(self.view_id, session_id)
        return "leader" if is_leader else "follower"
    
    async def _session_cleanup_loop(self) -> None:
        """
        Periodic task to clean up expired sessions.
        Note: The global SessionManager handles its own cleanup, 
        but we keep this for view-specific cleanup or monitoring if needed.
        Currently just waits until cancelled.
        """
        try:
            while self.is_running():
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass
    
    # --- Event Processing ---
    
    async def process_events(
        self,
        events: List[Any],
        session_id: str,
        source_type: str = "message",
        is_end: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Process a batch of events from an Agent.
        
        Args:
            events: List of events to process
            session_id: The session sending the events
            source_type: Type of events (message, snapshot, audit)
            **kwargs: Additional context (is_end, etc.)
        
        Returns:
            Processing result dict
        """
        if not self.is_running():
            return {"success": False, "error": "Pipe not running"}
        
        async with self._lock:
            self._active_pushes += 1
            self._queue_drained.clear()

        try:
            self.statistics["events_received"] += len(events)
            logger.debug(f"Pipe {self.id}: Received {len(events)} events from {session_id} (source={source_type})")
            
            # Convert dict events to EventBase if needed
            processed_events = []
            skipped_count = 0
            
            for event in events:
                if isinstance(event, dict):
                    try:
                        ev = EventBase.model_validate(event)
                        processed_events.append(ev)
                    except Exception as e:
                        logger.warning(f"Pipe {self.id}: Skipping malformed event in batch: {e}")
                        self.statistics["errors"] += 1
                        skipped_count += 1
                else:
                    processed_events.append(event)
            
            if skipped_count > 0:
                logger.warning(f"Pipe {self.id}: Skipped {skipped_count}/{len(events)} malformed events in batch")
            
            # Inject Lineage Info from cache (built at session creation)
            lineage_meta = self._session_lineage_cache.get(session_id, {})
            if lineage_meta:
                for ev in processed_events:
                    if ev.metadata is None:
                        # Efficiently inject cached lineage info
                        ev.metadata = lineage_meta.copy()
                    else:
                        ev.metadata.update(lineage_meta)

            # Handle special phases (config_report, etc.)
            if source_type == "config_report":
                metadata = kwargs.get("metadata", {})
                config_yaml = metadata.get("config_yaml")
                if config_yaml:
                    session_info = await session_manager.get_session_info(self.view_id, session_id)
                    if session_info:
                        session_info.reported_config = config_yaml
                        logger.info(f"Pipe {self.id}: Cached reported config for session {session_id}")
                return {"success": True, "message": "Config cached"}

            # Queue for processing
            if processed_events:
                get_metrics().counter("fustor.fusion.events_received", len(processed_events), {"pipe": self.id, "source": source_type})
                await self._event_queue.put(processed_events)
            
            # Handle snapshot completion signal
            is_snapshot_end = is_end or kwargs.get("is_snapshot_end", False)
            if source_type == "snapshot" and is_snapshot_end:
                from ..view_state_manager import view_state_manager
                # Only leader can signal snapshot end
                if await view_state_manager.is_leader(self.view_id, session_id):
                    logger.info(f"Pipe {self.id}: Received SNAPSHOT end signal from leader session {session_id}. Draining before marking complete.")
                    # Ensure all snapshot events are processed before marking as complete
                    await self.wait_for_drain(timeout=30.0, target_active_pushes=1)
                    
                    # Mark primary view complete
                    await view_state_manager.set_snapshot_complete(self.view_id, session_id)
                    logger.info(f"Pipe {self.id}: Marked view {self.view_id} as snapshot complete.")
                    
                    # Also mark all other handlers' views as complete if they are different
                    for h_id, handler in self._view_handlers.items():
                        # Check common adapter patterns
                        h_view_id = getattr(handler, 'view_id', None)
                        if not h_view_id and hasattr(handler, 'manager'):
                            h_view_id = getattr(handler.manager, 'view_id', None)
                        if not h_view_id and hasattr(handler, '_vm'):
                            h_view_id = getattr(handler._vm, 'view_id', None)
                        
                        if h_view_id and str(h_view_id) != str(self.view_id):
                            await view_state_manager.set_snapshot_complete(str(h_view_id), session_id)
                            logger.debug(f"Pipe {self.id}: Also marking view {h_view_id} as complete.")
                else:
                    logger.warning(f"Pipe {self.id}: Received snapshot end signal from non-leader session {session_id}. Ignored.")

            # Handle audit completion signal
            if source_type == "audit" and is_end:
                logger.info(f"Pipe {self.id}: Received AUDIT end signal from session {session_id}. Triggering audit finalization.")
                # Ensure all previous audit events are processed before finalizing
                # This prevents race conditions where handle_audit_end reads stale dir metadata
                # We target 1 because this current push is still active
                await self.wait_for_drain(timeout=30.0, target_active_pushes=1)
                
                for handler in self._view_handlers.values():
                    if hasattr(handler, 'handle_audit_end'):
                        try:
                            await handler.handle_audit_end()
                        except Exception as e:
                            logger.error(f"Pipe {self.id}: Error in handle_audit_end for {handler}: {e}")

            return {
                "success": True,
                "count": len(processed_events),
                "skipped": skipped_count,
                "source_type": source_type,
            }
        finally:
            async with self._lock:
                self._active_pushes -= 1
                if self._active_pushes == 0 and self._event_queue.empty():
                    self._queue_drained.set()

    
    # --- View Access ---
    
    def get_view(self, handler_id: str, **kwargs) -> Any:
        """
        Get data from a specific view handler.
        
        Args:
            handler_id: The view handler ID
            **kwargs: View-specific parameters
        
        Returns:
            View data (handler-specific format)
        """
        handler = self._view_handlers.get(handler_id)
        if handler and hasattr(handler, 'get_data_view'):
            return handler.get_data_view(**kwargs)
        return None
    
    # --- DTO & Stats ---
    
    async def get_dto(self) -> Dict[str, Any]:
        """Get pipestatus as a dictionary."""
        from ..view_state_manager import view_state_manager
        
        sessions = await session_manager.get_view_sessions(self.view_id)
        leader = await view_state_manager.get_leader(self.view_id)
        
        # Lock-free: all reads are safe in asyncio single-thread model
        return {
            "id": self.id,
            "view_id": self.view_id,
            "state": self.state.name if hasattr(self.state, 'name') else str(self.state),
            "info": self.info,
            "view_handlers": self.get_available_views(),
            "active_sessions": len(sessions),
            "leader_session": leader,
            "statistics": self.statistics.copy(),
            "queue_size": self._event_queue.qsize(),
        }
    
    async def get_aggregated_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from all view handlers."""
        pipe_stats = self.statistics.copy()
        pipe_stats["queue_size"] = self._event_queue.qsize()
        
        stats = {
            "pipe": pipe_stats,
            "views": {},
        }
        
        for handler_id, handler in self._view_handlers.items():
            if hasattr(handler, 'get_stats'):
                # Handle both sync and async get_stats
                res = handler.get_stats()
                if asyncio.iscoroutine(res):
                    stats["views"][handler_id] = await res
                else:
                    stats["views"][handler_id] = res
        
        return stats
        
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific session."""
        si = await session_manager.get_session_info(self.view_id, session_id)
        if si:
            # Convert to dict for DTO
            return {
                "session_id": si.session_id,
                "task_id": si.task_id,
                "client_ip": si.client_ip,
                "source_uri": si.source_uri,
                "created_at": si.created_at,
                "last_activity": si.last_activity,
            }
        return None
    
    async def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions."""
        si_map = await session_manager.get_view_sessions(self.view_id)
        return {k: {"task_id": v.task_id} for k, v in si_map.items()}
        
    @property
    def leader_session(self) -> Optional[str]:
        """
        Get the current leader session ID.
        Note: This is a cached value. For accurate result, use async get_dto().
        """
        return self._cached_leader_session
    
    async def wait_for_drain(self, timeout: float = None, target_active_pushes: int = 0) -> bool:
        """
        Wait until the event queue is empty and active pushes reach target.
        
        Args:
            timeout: Max time to wait
            target_active_pushes: The number of active pushes to wait for (default 0).
                                  Set to 1 if calling from within a push handler.
        """
        start_time = time.time()
        
        # 1. Wait for queue to be fully processed (including batches currently being handled)
        try:
            if timeout:
                await asyncio.wait_for(self._event_queue.join(), timeout=timeout)
            else:
                await self._event_queue.join()
        except asyncio.TimeoutError:
            return False
            
        # 2. Wait for active pushes to reach target
        if target_active_pushes == 0:
            # For 0 case, we can also use the event for better responsiveness
            # though join() already ensured current queue is empty.
            async with self._lock:
                if self._active_pushes == 0:
                    return True
            try:
                if timeout:
                    remaining = timeout - (time.time() - start_time)
                    if remaining <= 0: return False
                    await asyncio.wait_for(self._queue_drained.wait(), timeout=remaining)
                else:
                    await self._queue_drained.wait()
                return True
            except asyncio.TimeoutError:
                return False
        else:
            # Polling for non-zero target
            while True:
                async with self._lock:
                    if self._active_pushes <= target_active_pushes:
                        return True
                
                if timeout and (time.time() - start_time > timeout):
                    return False
                
                await asyncio.sleep(0.1)

    def __str__(self) -> str:
        return f"FusionPipe({self.id}, state={self.state.name})"



