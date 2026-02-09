"""
Receiver abstraction for Fustor.

A Receiver is responsible for accepting events over a transport protocol
(HTTP, gRPC, etc.) on the Fusion side.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable, Awaitable
import logging

logger = logging.getLogger(__name__)


# Type alias for event handler callback
EventHandler = Callable[[str, Any], Awaitable[bool]]


class Receiver(ABC):
    """
    Abstract base class for all Receivers.
    
    A Receiver handles:
    - Transport protocol implementation (HTTP server, gRPC server)
    - Credential validation (API key verification)
    - Session management
    - Event batch reception
    
    Receivers are configured with:
    - Bind address and port
    - Credentials (API keys to accept)
    - Protocol-specific parameters
    """
    
    def __init__(
        self,
        receiver_id: str,
        bind_host: str,
        port: int,
        credentials: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the receiver.
        
        Args:
            receiver_id: Unique identifier for this receiver
            bind_host: Host address to bind to
            port: Port number to listen on
            credentials: Valid credentials for authentication
            config: Additional receiver configuration
        """
        self.id = receiver_id
        self.bind_host = bind_host
        self.port = port
        self.credentials = credentials
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{receiver_id}")
        
        # Callback for processing received events
        self._event_handler: Optional[EventHandler] = None
    
    def set_event_handler(self, handler: EventHandler) -> None:
        """
        Set the callback for processing received events.
        
        Args:
            handler: Async function taking (pipe_id, event) and returning success
        """
        self._event_handler = handler
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the receiver and begin accepting connections.
        
        This should start the HTTP/gRPC server.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the receiver gracefully.
        
        This should:
        1. Stop accepting new connections
        2. Complete in-flight requests
        3. Release resources
        """
        raise NotImplementedError
    
    @abstractmethod
    async def validate_credential(self, credential: Dict[str, Any]) -> Optional[str]:
        """
        Validate incoming credential.
        
        Args:
            credential: The credential to validate
            
        Returns:
            Associated pipe_id if valid, None if invalid
        """
        raise NotImplementedError
    
    def get_address(self) -> str:
        """Get the receiver's address as a string."""
        return f"{self.bind_host}:{self.port}"
    
    # --- Session lifecycle hooks ---
    
    async def on_session_created(
        self, 
        session_id: str, 
        pipe_id: str,
        client_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Called when a new session is created.
        
        Override to perform additional setup.
        """
        pass
    
    async def on_session_closed(self, session_id: str, pipe_id: str) -> None:
        """
        Called when a session is closed.
        
        Override to perform cleanup.
        """
        pass
