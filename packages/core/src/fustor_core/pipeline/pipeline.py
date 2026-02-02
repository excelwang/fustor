"""
Pipeline abstraction for Fustor.

A Pipeline represents the runtime binding between data sources/receivers
and data sinks/views. It manages the lifecycle of data transfer tasks.
"""
from abc import ABC, abstractmethod
from enum import IntFlag, auto
from typing import Optional, Dict, Any, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from .context import PipelineContext
    from .handler import Handler

logger = logging.getLogger(__name__)


class PipelineState(IntFlag):
    """
    Pipeline state flags using bitmask for composite states.
    
    States can be combined, e.g., RUNNING | CONF_OUTDATED
    """
    STOPPED = 0
    INITIALIZING = auto()
    RUNNING = auto()
    PAUSED = auto()
    ERROR = auto()
    CONF_OUTDATED = auto()  # Configuration has changed, needs restart
    SNAPSHOT_PHASE = auto()  # Currently in snapshot phase phase
    MESSAGE_PHASE = auto()   # Currently in message/realtime sync phase
    AUDIT_PHASE = auto()     # Currently in audit phase phase
    RECONNECTING = auto()    # Currently attempting to reconnect
    DRAINING = auto()        # Draining queues before stopping
    STOPPING = auto()        # Gracefully stopping


class Pipeline(ABC):
    """
    Abstract base class for all Pipelines.
    
    A Pipeline orchestrates:
    - Session lifecycle management
    - Handler invocation (Source/Sender or Receiver/View)
    - Heartbeat and timeout handling
    - Error recovery
    
    Agent Pipeline: Source -> Sender
    Fusion Pipeline: Receiver -> View(s)
    """
    
    def __init__(
        self,
        pipeline_id: str,
        config: Dict[str, Any],
        context: Optional["PipelineContext"] = None
    ):
        """
        Initialize the pipeline.
        
        Args:
            pipeline_id: Unique identifier for this pipeline
            config: Pipeline configuration dictionary
            context: Optional shared context for cross-pipeline coordination
        """
        self.id = pipeline_id
        self.config = config
        self.context = context
        self.state = PipelineState.STOPPED
        self.info: Optional[str] = None  # Human-readable status info
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")
        
        # Session tracking
        self.session_id: Optional[str] = None
        self.session_timeout_seconds: int = config.get("session_timeout_seconds", 30)
        
    def __str__(self) -> str:
        return f"Pipeline({self.id}, state={self.state.name})"
    
    def _set_state(self, new_state: PipelineState, info: Optional[str] = None):
        """Update pipeline state with optional info message."""
        old_state = self.state
        self.state = new_state
        if info:
            self.info = info
        self.logger.info(f"State changed: {old_state.name} -> {new_state.name}" + 
                        (f" ({info})" if info else ""))
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the pipeline.
        
        This should:
        1. Initialize handlers
        2. Establish session (if applicable)
        3. Begin data transfer loop
        """
        raise NotImplementedError
    
    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the pipeline gracefully.
        
        This should:
        1. Stop data transfer loop
        2. Close session (if applicable)
        3. Release resources
        """
        raise NotImplementedError
    
    async def restart(self) -> None:
        """Restart the pipeline (stop then start)."""
        await self.stop()
        await self.start()
    
    @abstractmethod
    async def on_session_created(self, session_id: str, **kwargs) -> None:
        """
        Called when a session is successfully created.
        
        Args:
            session_id: The session identifier
            **kwargs: Additional session metadata (timeout, role, etc.)
        """
        raise NotImplementedError
    
    @abstractmethod
    async def on_session_closed(self, session_id: str) -> None:
        """
        Called when a session is closed (normally or due to timeout).
        
        Args:
            session_id: The session identifier
        """
        raise NotImplementedError
    
    def is_running(self) -> bool:
        """Check if pipeline is in a running state."""
        return bool(self.state & (PipelineState.RUNNING | 
                                  PipelineState.SNAPSHOT_PHASE | 
                                  PipelineState.MESSAGE_PHASE | 
                                  PipelineState.AUDIT_PHASE))
    
    def has_active_session(self) -> bool:
        """Check if pipeline has an active session with the remote peer."""
        return self.session_id is not None

    def is_outdated(self) -> bool:
        """Check if pipeline configuration is outdated."""
        return bool(self.state & PipelineState.CONF_OUTDATED)
    
    def get_dto(self) -> Dict[str, Any]:
        """Get a data transfer object representation of the pipeline."""
        return {
            "id": self.id,
            "state": self.state.name,
            "info": self.info,
            "session_id": self.session_id,
            "session_timeout_seconds": self.session_timeout_seconds,
        }
