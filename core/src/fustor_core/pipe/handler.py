"""
Handler abstraction for Fustor.

Handlers process data within a pipe - they are the actual
business logic implementations (Sources, Views, etc.)
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Iterator, List
import logging

from ..event import EventBase

logger = logging.getLogger(__name__)


class Handler(ABC):
    """
    Abstract base class for all Handlers.
    
    A Handler encapsulates data processing logic. Handlers are invoked
    by Pipes to perform actual data operations.
    
    Examples:
    - Source handlers: Read data and produce events
    - View handlers: Consume events and maintain state
    """
    
    # Schema identifier for routing (e.g., "fs", "mysql")
    schema_name: str = ""
    schema_version: str = "1.0"
    
    def __init__(self, handler_id: str, config: Dict[str, Any]):
        """
        Initialize the handler.
        
        Args:
            handler_id: Unique identifier for this handler instance
            config: Handler-specific configuration
        """
        self.id = handler_id
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{handler_id}")
    
    async def initialize(self) -> None:
        """
        Perform async initialization.
        
        Called once when the handler is first loaded.
        Override for setup that requires async operations.
        """
        pass
    
    async def close(self) -> None:
        """
        Release resources.
        
        Called when the handler is being shut down.
        Override to clean up file handles, connections, etc.
        """
        pass
    
    def get_schema_info(self) -> Dict[str, str]:
        """Get schema information for protocol negotiation."""
        return {
            "name": self.schema_name,
            "version": self.schema_version,
        }


class SourceHandler(Handler):
    """
    Base class for source handlers (data producers).
    
    Source handlers read data from external systems and produce events.
    They are used on the Agent side.
    """
    
    @abstractmethod
    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Perform a full snapshot of the source data.
        
        Returns:
            Iterator yielding events for all current data
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[EventBase]:
        """
        Perform incremental data capture (CDC).
        
        Args:
            start_position: Position to resume from (-1 for latest)
            
        Returns:
            Iterator yielding new events as they occur
        """
        raise NotImplementedError
    
    def get_audit_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Perform a consistency audit of the source data.
        
        Optional - default returns empty iterator.
        
        Returns:
            Iterator yielding audit events for consistency check
        """
        return iter([])
    
    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Verify specific items for consistency.
        
        Optional - default returns empty result.
        
        Args:
            task_batch: Batch of items to verify
            
        Returns:
            Verification results
        """
        return {}

    def scan_path(self, path: str, recursive: bool = True) -> Iterator[EventBase]:
        """
        On-demand scan of a specific path.
        
        Optional - default returns empty iterator.
        
        Returns:
            Iterator yielding events for the specified path
        """
        return iter([])


class ViewHandler(Handler):
    """
    Base class for view handlers (data consumers).
    
    View handlers consume events and maintain queryable views.
    They are used on the Fusion side.
    """
    
    @abstractmethod
    async def process_event(self, event: EventBase) -> bool:
        """
        Process a single event and update internal state.
        
        Args:
            event: The event to process
            
        Returns:
            True if processing succeeded
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_data_view(self, **kwargs) -> Any:
        """
        Return the current consistent data view.
        
        The format is handler-specific.
        """
        raise NotImplementedError
    
    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Determine the role (leader/follower) for a newly created session.
        
        Called by SessionBridge to delegate election logic.
        Not to be confused with on_session_start() which resets state.
        
        Returns:
            Dict with 'role', 'election_key', etc.
        """
        return {"role": "leader"}
    
    async def on_session_start(self, **kwargs) -> None:
        """Called when a new Agent session starts."""
        pass
    
    async def on_session_close(self, **kwargs) -> None:
        """Called when an Agent session terminates."""
        pass
    
    async def handle_audit_start(self) -> None:
        """Called at the start of an audit cycle."""
        pass
    
    async def handle_audit_end(self) -> None:
        """Called at the end of an audit cycle."""
        pass
    
    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None:
        """
        Called when a snapshot phase is complete.
        
        Handlers can use this hook to perform finalization or mark scoped keys 
        (e.g., ForestView marking a specific subtree as complete).
        """
        pass
    
    async def reset(self) -> None:
        """Clear all in-memory state."""
        pass
