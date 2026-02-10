"""
Receiver abstraction for Fustor.

A Receiver is responsible for accepting events over a transport protocol
(HTTP, gRPC, etc.) on the Fusion side.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Awaitable, Type
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
    
    # --- Extension points for PipeManager ---
    # Subclasses MUST implement these to enable driver-agnostic orchestration.
    
    @abstractmethod
    def register_callbacks(self, **callbacks) -> None:
        """
        Register event processing callbacks.
        
        Concrete receivers define which callbacks they accept.
        Common callbacks: on_session_created, on_event_received,
        on_heartbeat, on_session_closed, on_scan_complete.
        """
        raise NotImplementedError
    
    @abstractmethod
    def register_api_key(self, api_key: str, pipe_id: str) -> None:
        """
        Register an API key for authenticating requests to a specific pipe.
        """
        raise NotImplementedError
    
    def mount_router(self, router: Any) -> None:
        """
        Mount an additional router onto this receiver's app.
        
        Default is no-op. HTTP-based receivers override this to
        add extra API endpoints (e.g. consistency routes).
        """
        pass
    
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


class ReceiverRegistry:
    """
    Registry for Receiver driver implementations.
    
    Usage:
        # In receiver-http extension:
        ReceiverRegistry.register("http", HTTPReceiver)
        
        # In PipeManager:
        receiver = ReceiverRegistry.create("http", receiver_id=..., ...)
    """
    
    _drivers: Dict[str, Type[Receiver]] = {}
    
    @classmethod
    def register(cls, driver_name: str, receiver_class: Type[Receiver]) -> None:
        """Register a receiver class for a driver name."""
        cls._drivers[driver_name] = receiver_class
        logger.info(f"Registered receiver driver: {driver_name} -> {receiver_class.__name__}")
    
    @classmethod
    def create(cls, driver_name: str, **kwargs) -> Receiver:
        """
        Create a receiver instance by driver name.
        
        Raises KeyError if the driver is not registered.
        """
        if driver_name not in cls._drivers:
            available = list(cls._drivers.keys())
            raise KeyError(
                f"Unknown receiver driver '{driver_name}'. "
                f"Available: {available}. "
                f"Ensure the driver extension is installed."
            )
        return cls._drivers[driver_name](**kwargs)
    
    @classmethod
    def available_drivers(cls) -> List[str]:
        """List registered driver names."""
        return list(cls._drivers.keys())
