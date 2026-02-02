# fusion/src/fustor_fusion/runtime/fusion_pipeline.py
"""
FusionPipeline - The V2 Pipeline implementation for Fusion.

This Pipeline receives events from Agents and dispatches them to ViewHandlers.
It implements the receiver side of the Agent -> Fusion data flow.

Architecture:
=============

    Agent                               Fusion
    ┌─────────────┐                    ┌─────────────────────────────┐
    │AgentPipeline│ ─── HTTP/gRPC ───▶ │     FusionPipeline          │
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

from fustor_core.pipeline import Pipeline, PipelineState
from fustor_core.pipeline.handler import ViewHandler
from fustor_core.event import EventBase

if TYPE_CHECKING:
    from fustor_core.pipeline.context import PipelineContext

logger = logging.getLogger("fustor_fusion.pipeline")


class FusionPipeline(Pipeline):
    """
    Fusion-side Pipeline for receiving and processing events.
    
    This pipeline:
    1. Receives events from Agents (via receivers)
    2. Dispatches events to registered ViewHandlers
    3. Manages session lifecycle on the Fusion side
    4. Provides aggregated statistics and data views
    
    Usage:
        from fustor_fusion.runtime import FusionPipeline
        
        pipeline = FusionPipeline(
            pipeline_id="view-1",
            config={"view_id": "view-1"},
            view_handlers=[fs_view_handler, ...]
        )
        
        await pipeline.start()
        
        # Process incoming events
        await pipeline.process_events(events, session_id="...")
        
        # Query views
        data = pipeline.get_view("fs")
    """
    
    def __init__(
        self,
        pipeline_id: str,
        config: Dict[str, Any],
        view_handlers: Optional[List[ViewHandler]] = None,
        context: Optional["PipelineContext"] = None
    ):
        """
        Initialize the FusionPipeline.
        
        Args:
            pipeline_id: Unique identifier (typically view_id)
            config: Configuration dict containing:
                - view_id: str
                - allow_concurrent_push: bool
                - queue_batch_size: int
            view_handlers: List of ViewHandler instances to dispatch events to
            context: Optional PipelineContext for dependency injection
        """
        super().__init__(pipeline_id, config, context)
        
        # Consistent terminology: view_id is the primary identifier
        self.view_id = str(config.get("view_id", config.get("datastore_id", pipeline_id)))
        # self.datastore_id is now a property

        
        self.allow_concurrent_push = config.get("allow_concurrent_push", True)
        self.queue_batch_size = config.get("queue_batch_size", 100)
        
        # View handlers registry
        self._view_handlers: Dict[str, ViewHandler] = {}
        for handler in (view_handlers or []):
            self.register_view_handler(handler)
        # Session tracking - handled centrally via PipelineManager/SessionManager
        self._lock = asyncio.Lock()  # Generic lock for pipeline state
        
        # Processing task
        self._processing_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        
        # Statistics
        self.statistics = {
            "events_received": 0,
            "events_processed": 0,
            "sessions_created": 0,
            "sessions_closed": 0,
            "errors": 0,
        }
        self._cached_leader_session: Optional[str] = None
    
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
        """Start the pipeline and begin processing events."""
        if self.is_running():
            logger.warning(f"Pipeline {self.id} is already running")
            return
        
        logger.info(f"Starting FusionPipeline {self.id}")
        self._set_state(PipelineState.RUNNING, "Pipeline started")
        
        # Initialize all handlers
        for handler in self._view_handlers.values():
            if hasattr(handler, 'initialize'):
                await handler.initialize()
        
        # Start processing task
        self._processing_task = asyncio.create_task(
            self._processing_loop(),
            name=f"fusion-pipeline-{self.id}"
        )
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(
            self._session_cleanup_loop(),
            name=f"fusion-pipeline-cleanup-{self.id}"
        )
        
        logger.info(f"FusionPipeline {self.id} started with {len(self._view_handlers)} view handlers")
    
    async def stop(self) -> None:
        """Stop the pipeline."""
        if not self.is_running():
            return
        
        logger.info(f"Stopping FusionPipeline {self.id}")
        self._set_state(PipelineState.PAUSED, "Pipeline stopping")
        
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
        
        self._set_state(PipelineState.STOPPED, "Pipeline stopped")
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        logger.info(f"FusionPipeline {self.id} stopped")
    
    async def _processing_loop(self) -> None:
        """Background loop for processing queued events."""
        logger.debug(f"Processing loop started for pipeline {self.id}")
        
        while True:
            try:
                # Wait for events in queue
                event_batch = await self._event_queue.get()
                
                if event_batch is None:  # Shutdown signal
                    break
                
                # Process each event
                for event in event_batch:
                    await self._dispatch_to_handlers(event)
                    self.statistics["events_processed"] += 1
                
                self._event_queue.task_done()
                
            except asyncio.CancelledError:
                logger.debug(f"Processing loop cancelled for pipeline {self.id}")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                self.statistics["errors"] += 1
                await asyncio.sleep(0.1)
    
    async def _dispatch_to_handlers(self, event: EventBase) -> None:
        """Dispatch an event to all registered view handlers."""
        for handler_id, handler in self._view_handlers.items():
            try:
                if hasattr(handler, 'process_event'):
                    success = await handler.process_event(event)
                    if not success:
                        self.statistics["errors"] += 1
                        logger.warning(f"Handler {handler_id} returned False for event processing")
            except Exception as e:
                import traceback
                print(f"DEBUG_HANDLER_ERROR in {handler_id} for event {event.event_type} {event.table}")
                traceback.print_exc()
                logger.error(f"Error in handler {handler_id}: {e}", exc_info=True)
                self.statistics["errors"] += 1
                
                # If we encounter too many errors, we might want to flag the pipeline
                if self.statistics["errors"] > 1000: # Threshold for illustration
                    self._set_state(self.state | PipelineState.ERROR, "Excessive handler errors")
    
    
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
        # Note: Session creation in backing store is handled by PipelineSessionBridge
        
        self.statistics["sessions_created"] += 1
        
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
        # Note: Session removal from backing store is handled by PipelineSessionBridge
        
        self.statistics["sessions_closed"] += 1
        
        from ..core.session_manager import session_manager
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
    
    async def keep_session_alive(self, session_id: str) -> bool:
        """Update last activity for a session."""
        from ..core.session_manager import session_manager
        si = await session_manager.keep_session_alive(self.view_id, session_id)
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
            return {"success": False, "error": "Pipeline not running"}
        
        async with self._lock:
            self.statistics["events_received"] += len(events)
            print(f"DEBUG_FUSION_PIPE: Received {len(events)} events from {session_id} (source={source_type})")
        
        # Convert dict events to EventBase if needed
        processed_events = []
        for event in events:
            if isinstance(event, dict):
                ev = EventBase.model_validate(event)
                processed_events.append(ev)
                if ev.rows:
                    print(f"DEBUG_FUSION_EVENT: {ev.event_type} table={ev.table} first_row={ev.rows[0]}")
                else:
                    print(f"DEBUG_FUSION_EVENT: {ev.event_type} table={ev.table} (empty rows)")
            else:
                processed_events.append(event)
                if hasattr(event, "rows") and event.rows:
                    print(f"DEBUG_FUSION_EVENT: {event.event_type} table={event.table} first_row={event.rows[0]}")
        
        # Queue for processing
        await self._event_queue.put(processed_events)
        
        # Handle snapshot completion signal
        is_end = kwargs.get("is_end", False) or kwargs.get("is_snapshot_end", False)
        if source_type == "snapshot" and is_end:
            from ..view_state_manager import view_state_manager
            # Only leader can signal snapshot end
            if await view_state_manager.is_leader(self.view_id, session_id):
                await view_state_manager.set_snapshot_complete(self.view_id, session_id)
                logger.info(f"Pipeline {self.id}: Received snapshot end signal from leader session {session_id}. Marking snapshot as complete.")
            else:
                logger.warning(f"Pipeline {self.id}: Received snapshot end signal from non-leader session {session_id}. Ignored.")

        return {
            "success": True,
            "count": len(events),
            "source_type": source_type,
        }

    
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
        """Get pipeline status as a dictionary."""
        from ..core.session_manager import session_manager
        from ..view_state_manager import view_state_manager
        
        sessions = await session_manager.get_view_sessions(self.view_id)
        leader = await view_state_manager.get_leader(self.view_id)
        
        async with self._lock:
            return {
                "id": self.id,
                "view_id": self.view_id,
                "datastore_id": self.view_id, # Backward compatibility
                "state": self.state.name if hasattr(self.state, 'name') else str(self.state),
                "info": self.info,
                "view_handlers": self.get_available_views(),
                "active_sessions": len(sessions),
                "leader_session": leader,
                "statistics": self.statistics.copy(),
                "queue_size": self._event_queue.qsize(),
            }
    
    def get_aggregated_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from all view handlers."""
        stats = {
            "pipeline": self.statistics.copy(),
            "views": {},
        }
        
        for handler_id, handler in self._view_handlers.items():
            if hasattr(handler, 'get_stats'):
                stats["views"][handler_id] = handler.get_stats()
        
        return stats
        
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific session."""
        from ..core.session_manager import session_manager
        si = await session_manager.get_session_info(self.view_id, session_id)
        if si:
            # Convert to dict for DTO
            return {
                "session_id": si.session_id,
                "task_id": si.task_id,
                "client_ip": si.client_ip,
                "created_at": si.created_at,
                "last_activity": si.last_activity,
            }
        return None
    
    async def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all active sessions."""
        from ..core.session_manager import session_manager
        si_map = await session_manager.get_view_sessions(self.view_id)
        return {k: {"task_id": v.task_id} for k, v in si_map.items()}
        
    @property
    def leader_session(self) -> Optional[str]:
        """
        Get the current leader session ID.
        Note: This is a cached value. For accurate result, use async get_dto().
        """
        return self._cached_leader_session
    
    def __str__(self) -> str:
        return f"FusionPipeline({self.id}, state={self.state.name})"

    @property
    def datastore_id(self) -> str:
        """Deprecated alias for view_id."""
        import warnings
        warnings.warn("datastore_id is deprecated, use view_id instead", DeprecationWarning, stacklevel=2)
        return self.view_id

