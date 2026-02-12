# packages/core/src/fustor_core/pipe/sender.py
"""
Sender Handler abstraction for Agent Pipes.

A Sender handler transmits events from Agent to Fusion.
This is the counterpart to SourceHandler.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Tuple
import logging

from ..event import EventBase
from .handler import Handler

logger = logging.getLogger(__name__)


class SenderHandler(Handler):
    """
    Base class for sender handlers (data transmitters).
    
    Sender handlers transmit events to remote Fusion instances.
    They are used on the Agent side.
    
    This is the V2 architecture replacement for the driver-based

    """
    
    @abstractmethod
    async def create_session(
        self, 
        task_id: str,
        source_type: str,
        session_timeout_seconds: int = 30,
        **kwargs
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Create a new session with the Fusion endpoint.
        
        Args:
            task_id: Unique identifier for the pipe 
            source_type: Type of source (e.g., "fs", "mysql")
            session_timeout_seconds: Session timeout value
            **kwargs: Additional session parameters
            
        Returns:
            Tuple of (session_id, session_metadata)
        """
        raise NotImplementedError
    
    @abstractmethod
    async def send_heartbeat(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Send a heartbeat to keep the session alive.
        
        Args:
            session_id: The session identifier
            
        Returns:
            Heartbeat response containing role and other metadata
        """
        raise NotImplementedError
    
    @abstractmethod
    async def send_batch(
        self, 
        session_id: str,
        events: List[EventBase],
        batch_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Send a batch of events to Fusion.
        
        Args:
            session_id: The session identifier
            events: List of events to send
            batch_context: Optional context (e.g., audit markers)
            
        Returns:
            Tuple of (success, response_metadata)
        """
        raise NotImplementedError
    
    @abstractmethod
    async def close_session(self, session_id: str) -> bool:
        """
        Close the session.
        
        Args:
            session_id: The session identifier
            
        Returns:
            True if closed successfully
        """
        raise NotImplementedError

    @abstractmethod
    async def get_latest_committed_index(self, session_id: str) -> int:
        """
        Get the latest committed event index from Fusion.
        
        Args:
            session_id: The session identifier
            
        Returns:
            The latest confirmed event index (or 0 if none)
        """
        raise NotImplementedError
    
    async def test_connection(self, **kwargs) -> Tuple[bool, str]:
        """
        Test connectivity to the Fusion endpoint.
        
        Returns:
            Tuple of (success, message)
        """
        return (True, "Connection test not implemented")
    
    async def check_privileges(self, **kwargs) -> Tuple[bool, str]:
        """
        Check if we have necessary privileges.
        
        Returns:
            Tuple of (success, message)
        """
        return (True, "Privilege check not implemented")
