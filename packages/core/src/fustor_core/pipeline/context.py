"""
Pipeline context for shared state across pipelines.
"""
from typing import Dict, Any, Optional
import asyncio
import logging

logger = logging.getLogger(__name__)


class PipelineContext:
    """
    Shared context for pipeline coordination.
    
    This class serves as a dependency injection container and
    shared state manager for pipelines running in the same process.
    
    Use cases:
    - Agent: Coordinate multiple Source->Sender pipelines
    - Fusion: Coordinate multiple Receiver->View pipelines
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline context.
        
        Args:
            config: Optional global configuration
        """
        self.config = config or {}
        self._pipelines: Dict[str, Any] = {}  # pipeline_id -> Pipeline
        self._lock = asyncio.Lock()
        self._event_bus: Optional[Any] = None  # For inter-pipeline communication
        
    async def register_pipeline(self, pipeline_id: str, pipeline: Any) -> None:
        """Register a pipeline with the context."""
        async with self._lock:
            if pipeline_id in self._pipelines:
                logger.warning(f"Pipeline {pipeline_id} already registered, replacing")
            self._pipelines[pipeline_id] = pipeline
            logger.debug(f"Registered pipeline {pipeline_id}")
    
    async def unregister_pipeline(self, pipeline_id: str) -> Optional[Any]:
        """Unregister a pipeline. Returns the pipeline if found."""
        async with self._lock:
            pipeline = self._pipelines.pop(pipeline_id, None)
            if pipeline:
                logger.debug(f"Unregistered pipeline {pipeline_id}")
            return pipeline
    
    def get_pipeline(self, pipeline_id: str) -> Optional[Any]:
        """Get a pipeline by ID."""
        return self._pipelines.get(pipeline_id)
    
    def list_pipelines(self) -> Dict[str, Any]:
        """List all registered pipelines."""
        return self._pipelines.copy()
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)
