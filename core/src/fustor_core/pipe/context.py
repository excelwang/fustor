"""
Pipe context for shared state across pipes.
"""
from typing import Dict, Any, Optional
import asyncio
import logging

logger = logging.getLogger(__name__)


class PipeContext:
    """
    Shared context for pipe coordination.
    
    This class serves as a dependency injection container and
    shared state manager for pipes running in the same process.
    
    Use cases:
    - Agent: Coordinate multiple Source->Sender pipes
    - Fusion: Coordinate multiple Receiver->View pipes
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipe context.
        
        Args:
            config: Optional global configuration
        """
        self.config = config or {}
        self._pipes: Dict[str, Any] = {}  # pipe_id -> Pipe
        self._lock = asyncio.Lock()
        self._event_bus: Optional[Any] = None  # For inter-pipe communication
        
    async def register_pipe(self, pipe_id: str, pipe: Any) -> None:
        """Register a pipe with the context."""
        async with self._lock:
            if pipe_id in self._pipes:
                logger.warning(f"Pipe {pipe_id} already registered, replacing")
            self._pipes[pipe_id] = pipe
            logger.debug(f"Registered pipe {pipe_id}")
    
    async def unregister_pipe(self, pipe_id: str) -> Optional[Any]:
        """Unregister a pipe. Returns the pipe if found."""
        async with self._lock:
            pipe = self._pipes.pop(pipe_id, None)
            if pipe:
                logger.debug(f"Unregistered pipe {pipe_id}")
            return pipe
    
    def get_pipe(self, pipe_id: str) -> Optional[Any]:
        """Get a pipe by ID."""
        return self._pipes.get(pipe_id)
    
    def list_pipes(self) -> Dict[str, Any]:
        """List all registered pipes."""
        return self._pipes.copy()
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)
