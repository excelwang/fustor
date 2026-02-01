# agent/src/fustor_agent/runtime/source_handler_adapter.py
"""
Adapter to wrap a Source driver as a SourceHandler for use in AgentPipeline.

This allows the existing source-fs and other driver implementations
to be used with the new Pipeline-based architecture.
"""
import logging
from typing import Any, Dict, Iterator, List, Optional, TYPE_CHECKING

from fustor_core.pipeline.handler import SourceHandler
from fustor_core.models.config import SourceConfig

if TYPE_CHECKING:
    from fustor_agent.services.drivers.source_driver import SourceDriverService

logger = logging.getLogger("fustor_agent")


from fustor_core.drivers import SourceDriver

class SourceHandlerAdapter(SourceHandler):
    """
    Adapts a Source driver to the SourceHandler interface.
    
    This adapter bridges the gap between:
    - Source drivers (like FSDriver) which are class-based with entry points
    - SourceHandler (pipeline/handler layer)
    
    Example usage:
        from fustor_source_fs import FSDriver
        from fustor_core.models.config import SourceConfig
        
        config = SourceConfig(driver="fs", uri="/data", ...)
        driver = FSDriver(id="my-source", config=config)
        handler = SourceHandlerAdapter(driver)
        
        # Now usable with AgentPipeline
        pipeline = AgentPipeline(
            source_handler=handler,
            sender_handler=sender,
            ...
        )
    """
    
    def __init__(
        self,
        driver: SourceDriver,  # The actual driver instance
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the adapter.
        
        Args:
            driver: The underlying source driver instance
            config: Optional configuration overrides
        """
        super().__init__(
            handler_id=driver.id if hasattr(driver, 'id') else "source",
            config=config or {}
        )
        self._driver = driver
        
        # Determine schema from driver if available
        if hasattr(driver, 'schema_name'):
            self.schema_name = driver.schema_name
        elif hasattr(driver.__class__, '__name__'):
            # Infer from class name: FSDriver -> "fs"
            name = driver.__class__.__name__
            if name.endswith("Driver"):
                self.schema_name = name[:-6].lower()
            else:
                self.schema_name = name.lower()
        else:
            self.schema_name = "unknown"
        
        self._initialized = False
    
    @property
    def driver(self) -> Any:
        """Access the underlying driver."""
        return self._driver
    
    async def initialize(self) -> None:
        """Initialize the handler."""
        if not self._initialized:
            # Some drivers have async initialization
            if hasattr(self._driver, 'initialize'):
                await self._driver.initialize()
            self._initialized = True
            logger.debug(f"SourceHandlerAdapter {self.id} initialized")
    
    async def close(self) -> None:
        """Close the handler and release resources."""
        if self._initialized:
            if hasattr(self._driver, 'close'):
                result = self._driver.close()
                # Handle async close methods
                if hasattr(result, '__await__'):
                    await result
            self._initialized = False
            logger.debug(f"SourceHandlerAdapter {self.id} closed")
    
    def get_snapshot_iterator(self, **kwargs) -> Iterator[Any]:
        """
        Get snapshot iterator from the driver.
        
        Delegates to the underlying driver's get_snapshot_iterator method.
        """
        return self._driver.get_snapshot_iterator(**kwargs)
    
    def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[Any]:
        """
        Get message iterator from the driver.
        
        Delegates to the underlying driver's get_message_iterator method.
        """
        return self._driver.get_message_iterator(start_position=start_position, **kwargs)
    
    def get_audit_iterator(self, **kwargs) -> Iterator[Any]:
        """
        Get audit iterator from the driver.
        
        Delegates to the underlying driver's get_audit_iterator method.
        Falls back to empty iterator if not supported.
        """
        if hasattr(self._driver, 'get_audit_iterator'):
            return self._driver.get_audit_iterator(**kwargs)
        return iter([])
    
    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform sentinel check using the driver.
        
        Delegates to the underlying driver's perform_sentinel_check method.
        Falls back to empty result if not supported.
        """
        if hasattr(self._driver, 'perform_sentinel_check'):
            return self._driver.perform_sentinel_check(task_batch)
        return {}
    
    def is_transient(self) -> bool:
        """
        Check if the source is transient (events lost if not processed).
        
        Delegates to the underlying driver's is_transient method.
        """
        if hasattr(self._driver, 'is_transient'):
            return self._driver.is_transient()
        return False


class SourceHandlerFactory:
    """
    Factory for creating SourceHandler instances from configuration.
    
    This factory uses the existing SourceDriverService to create
    driver instances and wraps them with SourceHandlerAdapter.
    """
    
    def __init__(self, source_driver_service: "SourceDriverService"):
        """
        Initialize the factory.
        
        Args:
            source_driver_service: Service for creating source driver instances
        """
        self._driver_service = source_driver_service
    
    def create_handler(
        self,
        source_config: SourceConfig,
        handler_id: Optional[str] = None
    ) -> SourceHandlerAdapter:
        """
        Create a SourceHandler from configuration.
        
        Args:
            source_config: Source configuration
            handler_id: Optional override for handler ID
        
        Returns:
            A SourceHandlerAdapter wrapping the appropriate driver
        """
        # Get the driver class
        driver_type = source_config.driver
        driver_class = self._driver_service._get_driver_by_type(driver_type)
        
        # Create driver instance
        instance_id = handler_id or f"source-{driver_type}"
        driver = driver_class(id=instance_id, config=source_config)
        
        # Wrap in adapter
        return SourceHandlerAdapter(driver)


def create_source_handler_from_config(
    source_config: SourceConfig,
    source_driver_service: "SourceDriverService",
    handler_id: Optional[str] = None
) -> SourceHandlerAdapter:
    """
    Create a SourceHandler from configuration.
    
    This is a convenience function that creates a factory and uses it.
    
    Args:
        source_config: Source configuration
        source_driver_service: Driver service for loading source driver classes
        handler_id: Optional handler ID
        
    Returns:
        A SourceHandlerAdapter instance
    """
    factory = SourceHandlerFactory(source_driver_service)
    return factory.create_handler(source_config, handler_id)
