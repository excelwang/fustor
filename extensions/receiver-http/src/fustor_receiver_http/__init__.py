"""
Fustor HTTP Receiver - Transport layer for Fusion to receive events from Agents.

This package implements the HTTP transport protocol for receiving events
on the Fusion side. It provides FastAPI routers that can be mounted into
the Fusion application.
"""
import logging
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass
import uuid

from fastapi import APIRouter, Depends, HTTPException, status, Request
from pydantic import BaseModel

from fustor_core.transport import Receiver
from fustor_core.event import EventBase, EventType, MessageSource

logger = logging.getLogger(__name__)


# --- Pydantic Models for API ---

class CreateSessionRequest(BaseModel):
    """Request payload for creating a new session."""
    task_id: str
    client_info: Optional[Dict[str, Any]] = None
    session_timeout_seconds: Optional[int] = None


class CreateSessionResponse(BaseModel):
    """Response for session creation."""
    session_id: str
    role: str  # 'leader' or 'follower'
    session_timeout_seconds: int
    message: str


class EventBatch(BaseModel):
    """Batch of events to ingest."""
    events: List[EventBase]
    source_type: str = "message"  # 'message', 'snapshot', 'audit', 'scan_complete'
    is_end: bool = False
    metadata: Optional[Dict[str, Any]] = None  # Extra info e.g., scan_path


class HeartbeatResponse(BaseModel):
    status: str
    role: Optional[str] = None
    message: Optional[str] = None
    can_realtime: Optional[bool] = None


# --- Session Handler Protocol ---

@dataclass
class SessionInfo:
    """Information about an active session."""
    session_id: str
    task_id: str
    view_id: str
    role: str  # 'leader' or 'follower'
    created_at: float
    last_heartbeat: float
    can_realtime: bool = False

    @property
    def pipe_id(self) -> str:
        """Deprecated alias for view_id."""
        import warnings
        warnings.warn("pipe_id is deprecated, use view_id instead", DeprecationWarning, stacklevel=2)
        return self.view_id


# Type aliases for callbacks
SessionCreatedCallback = Callable[[str, str, str, Dict[str, Any], int], Awaitable[SessionInfo]]
EventReceivedCallback = Callable[[str, List[EventBase], str, bool], Awaitable[bool]]
HeartbeatCallback = Callable[[str, bool], Awaitable[Dict[str, Any]]]
SessionClosedCallback = Callable[[str], Awaitable[None]]


class HTTPReceiver(Receiver):
    """
    HTTP-based Receiver implementation for Fustor Fusion.
    
    This receiver creates FastAPI routers that handle:
    - Session creation and management
    - Event batch ingestion
    - Heartbeat processing
    
    The receiver delegates actual processing to registered callbacks.
    """
    
    def __init__(
        self,
        receiver_id: str,
        bind_host: str = "0.0.0.0",
        port: int = 8101,
        credentials: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(receiver_id, bind_host, port, credentials or {}, config)
        
        # Callbacks for event processing
        self._on_session_created: Optional[SessionCreatedCallback] = None
        self._on_event_received: Optional[EventReceivedCallback] = None
        self._on_heartbeat: Optional[HeartbeatCallback] = None
        self._on_session_closed: Optional[SessionClosedCallback] = None
        self._on_scan_complete: Optional[Callable[[str, str], Awaitable[None]]] = None  # session_id, path
        
        # API key to pipe mapping
        self._api_key_to_pipe: Dict[str, str] = {}
        self._api_key_cache: Dict[str, str] = {}
        
        
        # Session timeout configuration
        self.session_timeout_seconds = config.get("session_timeout_seconds", 30) if config else 30

        
        # Create routers
        self._session_router = self._create_session_router()
        self._ingestion_router = self._create_ingestion_router()
    
    def register_callbacks(
        self,
        on_session_created: Optional[SessionCreatedCallback] = None,
        on_event_received: Optional[EventReceivedCallback] = None,
        on_heartbeat: Optional[HeartbeatCallback] = None,
        on_session_closed: Optional[SessionClosedCallback] = None,
        on_scan_complete: Optional[Callable[[str, str], Awaitable[None]]] = None,
    ):
        """Register callbacks for event processing."""
        if on_session_created:
            self._on_session_created = on_session_created
        if on_event_received:
            self._on_event_received = on_event_received
        if on_heartbeat:
            self._on_heartbeat = on_heartbeat
        if on_session_closed:
            self._on_session_closed = on_session_closed
        if on_scan_complete:
            self._on_scan_complete = on_scan_complete
    
    def register_api_key(self, api_key: str, pipe_id: str):
        """Register an API key for a pipe."""
        self._api_key_to_pipe[api_key] = pipe_id
        self._api_key_cache.clear()  # Invalidate cache
        self.logger.debug(f"Registered API key for pipe {pipe_id}")
    
    async def validate_credential(self, credential: Dict[str, Any]) -> Optional[str]:
        """
        Validate incoming credential.
        
        Args:
            credential: The credential to validate (expects {"api_key": "..."})
            
        Returns:
            Associated pipe_id if valid, None if invalid
        """
        api_key = credential.get("api_key") or credential.get("key")
        if not api_key:
            return None
            
        # Check cache first
        if api_key in self._api_key_cache:
            return self._api_key_cache[api_key]
            
        # Check mapping
        if api_key in self._api_key_to_pipe:
            pipe_id = self._api_key_to_pipe[api_key]
            self._api_key_cache[api_key] = pipe_id
            return pipe_id
            
        return None
    
    async def start(self) -> None:
        """Start the receiver (routers are mounted externally)."""
        self.logger.info(f"HTTP Receiver {self.id} ready on {self.get_address()}")
    
    async def stop(self) -> None:
        """Stop the receiver gracefully."""
        self.logger.info(f"HTTP Receiver {self.id} stopping")
    
    def get_session_router(self) -> APIRouter:
        """Get the session management router."""
        return self._session_router
    
    def get_ingestion_router(self) -> APIRouter:
        """Get the event ingestion router."""
        return self._ingestion_router
    
    def _create_session_router(self) -> APIRouter:
        """Create the session management router."""
        router = APIRouter(tags=["Session"])
        receiver = self  # Capture self for closures
        
        @router.post("/", response_model=CreateSessionResponse)
        async def create_session(
            payload: CreateSessionRequest,
            request: Request,
        ):
            """Create a new session for event ingestion."""
            # Extract API key from header
            api_key = request.headers.get("X-API-Key")
            if not api_key:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API key required"
                )
            
            pipe_id = await receiver.validate_credential({"api_key": api_key})
            if not pipe_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key"
                )
            
            session_id = str(uuid.uuid4())
            
            # Use client-requested timeout if provided, otherwise fallback to receiver config
            session_timeout_seconds = payload.session_timeout_seconds or receiver.session_timeout_seconds

            
            if receiver._on_session_created:
                try:
                    session_info = await receiver._on_session_created(
                        session_id, 
                        payload.task_id, 
                        pipe_id, 
                        payload.client_info or {},
                        session_timeout_seconds
                    )
                    return CreateSessionResponse(
                        session_id=session_info.session_id,
                        role=session_info.role,
                        session_timeout_seconds=session_timeout_seconds,
                        message="Session created successfully"
                    )
                except Exception as e:
                    receiver.logger.error(f"Failed to create session: {e}")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=str(e)
                    )
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Session handler not configured"
                )
        
        @router.post("/{session_id}/heartbeat", response_model=HeartbeatResponse)
        async def heartbeat(session_id: str, request: Request):
            """Send a heartbeat to maintain session."""
            # Extract can_realtime from payload (if any)
            try:
                payload = await request.json()
                can_realtime = payload.get("can_realtime", False)
            except Exception:
                can_realtime = False

            logger.info(f"Received heartbeat for session {session_id}, can_realtime={can_realtime}")

            if receiver._on_heartbeat:
                try:
                    result = await receiver._on_heartbeat(session_id, can_realtime)
                    if result and result.get("status") == "error":
                        raise HTTPException(
                            status_code=419,
                            detail=result.get("message", "Session obsoleted")
                        )
                    return HeartbeatResponse(
                        status=result.get("status", "ok"),
                        role=result.get("role"),
                        message=result.get("message")
                    )
                except HTTPException:
                    raise
                except Exception as e:
                    receiver.logger.warning(f"Heartbeat failed for {session_id}: {e}")
                    return HeartbeatResponse(status="error", message=str(e))
            
            return HeartbeatResponse(status="ok")
        
        @router.delete("/{session_id}")
        async def terminate_session(session_id: str, request: Request):
            """Terminate a session."""
            if receiver._on_session_closed:
                await receiver._on_session_closed(session_id)
            return {"status": "terminated", "session_id": session_id}
        
        return router
    
    def _create_ingestion_router(self) -> APIRouter:
        """Create the event ingestion router."""
        router = APIRouter(tags=["Ingestion"])
        receiver = self
        
        @router.post("/{session_id}/events")
        async def ingest_events(
            session_id: str,
            batch: EventBatch,
            request: Request,
        ):
            """Ingest a batch of events."""
            
            # Handle scan_complete notification
            if batch.source_type == "scan_complete" and batch.metadata:
                scan_path = batch.metadata.get("scan_path")
                if scan_path and receiver._on_scan_complete:
                    try:
                        await receiver._on_scan_complete(session_id, scan_path)
                        return {"status": "ok", "phase": "scan_complete"}
                    except Exception as e:
                        receiver.logger.error(f"Scan complete handling failed: {e}")
                return {"status": "ok", "phase": "scan_complete"}

            if receiver._on_event_received:
                try:
                    success = await receiver._on_event_received(
                        session_id, batch.events, batch.source_type, batch.is_end
                    )
                    if success:
                        return {"status": "ok", "count": len(batch.events)}
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to process events"
                        )
                except HTTPException:
                    raise
                except Exception as e:
                    receiver.logger.error(f"Event ingestion failed: {e}")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=str(e)
                    )
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Event handler not configured"
                )
        
        return router


# Factory function for creating receiver with standard configuration
def create_http_receiver(
    receiver_id: str = "default",
    config: Optional[Dict[str, Any]] = None
) -> HTTPReceiver:
    """
    Create an HTTP receiver with standard configuration.
    
    Args:
        receiver_id: Unique identifier for this receiver
        config: Optional configuration dict
        
    Returns:
        Configured HTTPReceiver instance
    """
    return HTTPReceiver(
        receiver_id=receiver_id,
        config=config or {}
    )


__all__ = [
    "HTTPReceiver",
    "SessionInfo",
    "CreateSessionRequest",
    "CreateSessionResponse",
    "EventBatch",
    "HeartbeatResponse",
    "create_http_receiver",
]
