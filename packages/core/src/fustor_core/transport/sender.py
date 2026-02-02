"""
Sender abstraction for Fustor.

A Sender is responsible for transmitting events over a transport protocol
(HTTP, gRPC, etc.) from Agent to Fusion.
"""
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging

from ..event import EventBase
from ..common.metrics import get_metrics

logger = logging.getLogger(__name__)


class Sender(ABC):
    """
    Abstract base class for all Senders.
    
    A Sender handles:
    - Transport protocol implementation (HTTP, gRPC)
    - Session creation with Fusion
    - Event batch transmission
    - Heartbeat maintenance
    
    Senders are configured with:
    - Endpoint URL
    - Credentials (API key)
    - Protocol-specific parameters
    """
    
    def __init__(
        self,
        sender_id: str,
        endpoint: str,
        credential: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the sender.
        
        Args:
            sender_id: Unique identifier for this sender
            endpoint: Target endpoint URL
            credential: Authentication credentials (e.g., API key)
            config: Additional sender configuration
        """
        self.id = sender_id
        self.endpoint = endpoint
        self.credential = credential
        self.config = config or {}
        self.session_id: Optional[str] = None
        self.logger = logging.getLogger(f"{__name__}.{sender_id}")
        self.metrics = get_metrics()
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the receiver.
        
        This may involve TLS handshake, connection pooling, etc.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def create_session(
        self, 
        task_id: str, 
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a new session with the Fusion receiver.
        
        Args:
            task_id: Identifier for this sync task
            source_type: Type of source (e.g. 'fs', 'mysql')
            session_timeout_seconds: Requested session timeout
            
        Returns:
            Session metadata including:
            - session_id: The session identifier
            - timeout_seconds: Session timeout from server
            - role: 'leader' or 'follower' (for FS consistency)
        """
        raise NotImplementedError
    
    async def send_events(
        self, 
        events: List[EventBase], 
        source_type: str = "message",
        is_end: bool = False
    ) -> Dict[str, Any]:
        """
        Send a batch of events to Fusion.
        This is a template method that wraps the actual implementation with metrics.
        
        Args:
            events: List of events to send
            source_type: Type of events ('message', 'snapshot', 'audit')
            is_end: Whether this is the last batch for this source_type
            
        Returns:
            Response from receiver
        """
        start_time = time.perf_counter()
        tags = {"sender_id": self.id, "source_type": source_type}
        
        try:
            result = await self._send_events_impl(events, source_type, is_end)
            
            # Record success metrics
            duration_ms = (time.perf_counter() - start_time) * 1000
            self.metrics.histogram("fustor.core.sender.latency_ms", duration_ms, tags)
            self.metrics.counter("fustor.core.sender.events_sent", len(events), {**tags, "success": "true"})
            
            return result
        except Exception as e:
            # Record error metrics
            self.metrics.counter("fustor.core.sender.errors", 1, {**tags, "error_type": type(e).__name__})
            self.metrics.counter("fustor.core.sender.events_sent", len(events), {**tags, "success": "false"})
            raise
    
    @abstractmethod
    async def _send_events_impl(
        self, 
        events: List[EventBase], 
        source_type: str,
        is_end: bool
    ) -> Dict[str, Any]:
        """
        Actual implementation of sending events.
        Must be implemented by concrete classes.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def heartbeat(self) -> Dict[str, Any]:
        """
        Send a heartbeat to maintain session.
        
        Returns:
            Response including current role status
        """
        raise NotImplementedError
    
    async def close_session(self) -> None:
        """Close the current session gracefully."""
        pass
    
    async def close(self) -> None:
        """Close the sender and release resources."""
        pass
    
    # --- Optional consistency signals ---
    
    async def signal_audit_start(self) -> bool:
        """Signal the start of an audit cycle. Optional."""
        return False
    
    async def signal_audit_end(self) -> bool:
        """Signal the end of an audit cycle. Optional."""
        return False
    
    async def get_sentinel_tasks(self) -> Optional[Dict[str, Any]]:
        """Query for sentinel verification tasks. Optional."""
        return None
    
    async def submit_sentinel_results(self, results: Dict[str, Any]) -> bool:
        """Submit sentinel verification results. Optional."""
        return True
