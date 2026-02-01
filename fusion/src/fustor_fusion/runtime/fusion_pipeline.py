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
            pipeline_id="datastore-1",
            config={"datastore_id": 1},
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
            pipeline_id: Unique identifier (typically datastore_id)
            config: Configuration dict containing:
                - datastore_id: int
                - allow_concurrent_push: bool
                - queue_batch_size: int
            view_handlers: List of ViewHandler instances to dispatch events to
            context: Optional PipelineContext for dependency injection
        """
        super().__init__(pipeline_id, config, context)
        
        self.datastore_id = str(config.get("datastore_id", pipeline_id))
        self.allow_concurrent_push = config.get("allow_concurrent_push", True)
        self.queue_batch_size = config.get("queue_batch_size", 100)
        
        # View handlers registry
        self._view_handlers: Dict[str, ViewHandler] = {}
        for handler in (view_handlers or []):
            self.register_view_handler(handler)
        
        # Session tracking
        self._active_sessions: Dict[str, Dict[str, Any]] = {}
        self._leader_session: Optional[str] = None
        
        # Processing task
        self._processing_task: Optional[asyncio.Task] = None
        self._event_queue: asyncio.Queue = asyncio.Queue()
        
        # Statistics
        self.statistics = {
            "events_received": 0,
            "events_processed": 0,
            "sessions_created": 0,
            "sessions_closed": 0,
            "errors": 0,
        }
    
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
        
        # Clear sessions
        self._active_sessions.clear()
        self._leader_session = None
        
        self._set_state(PipelineState.STOPPED, "Pipeline stopped")
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
                    await handler.process_event(event)
            except Exception as e:
                logger.error(f"Error in handler {handler_id}: {e}", exc_info=True)
                self.statistics["errors"] += 1
    
    # --- Session Management ---
    
    async def on_session_created(self, session_id: str, **kwargs) -> None:
        """
        Handle session creation.
        
        Called when an Agent creates a new session.
        """
        task_id = kwargs.get("task_id")
        client_ip = kwargs.get("client_ip")
        
        # Determine role (leader/follower)
        role = "follower"
        if not self._leader_session:
            self._leader_session = session_id
            role = "leader"
        
        self._active_sessions[session_id] = {
            "task_id": task_id,
            "client_ip": client_ip,
            "role": role,
            "created_at": asyncio.get_event_loop().time(),
        }
        
        self.statistics["sessions_created"] += 1
        
        # Notify view handlers of session start
        for handler in self._view_handlers.values():
            if hasattr(handler, 'on_session_start'):
                await handler.on_session_start()
        
        logger.info(f"Session {session_id} created with role={role}")
    
    async def on_session_closed(self, session_id: str) -> None:
        """
        Handle session closure.
        
        Called when an Agent closes its session.
        """
        if session_id in self._active_sessions:
            del self._active_sessions[session_id]
            
            # If leader left, elect new leader
            if session_id == self._leader_session:
                self._leader_session = None
                if self._active_sessions:
                    new_leader = next(iter(self._active_sessions))
                    self._leader_session = new_leader
                    self._active_sessions[new_leader]["role"] = "leader"
                    logger.info(f"New leader elected: {new_leader}")
        
        self.statistics["sessions_closed"] += 1
        
        # Notify view handlers of session close
        for handler in self._view_handlers.values():
            if hasattr(handler, 'on_session_close'):
                await handler.on_session_close()
        
        logger.info(f"Session {session_id} closed")
    
    def get_session_role(self, session_id: str) -> str:
        """Get the role of a session (leader/follower)."""
        session = self._active_sessions.get(session_id)
        return session.get("role", "unknown") if session else "unknown"
    
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
        
        self.statistics["events_received"] += len(events)
        
        # Convert dict events to EventBase if needed
        processed_events = []
        for event in events:
            if isinstance(event, dict):
                processed_events.append(EventBase.model_validate(event))
            else:
                processed_events.append(event)
        
        # Queue for processing
        await self._event_queue.put(processed_events)
        
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
    
    def get_dto(self) -> Dict[str, Any]:
        """Get pipeline status as a dictionary."""
        return {
            "id": self.id,
            "datastore_id": self.datastore_id,
            "state": self.state.name if hasattr(self.state, 'name') else str(self.state),
            "info": self.info,
            "view_handlers": self.get_available_views(),
            "active_sessions": len(self._active_sessions),
            "leader_session": self._leader_session,
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
    
    def __str__(self) -> str:
        return f"FusionPipeline({self.id}, state={self.state.name})"
