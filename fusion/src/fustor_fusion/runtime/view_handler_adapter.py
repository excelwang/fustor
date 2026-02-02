# fusion/src/fustor_fusion/runtime/view_handler_adapter.py
"""
Adapter to wrap a ViewDriver or ViewManager as a ViewHandler for use in FusionPipeline.

This allows the existing view-fs and other view driver implementations
to be used with the new Pipeline-based architecture.
"""
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from fustor_core.pipeline.handler import ViewHandler
from fustor_core.drivers import ViewDriver
from fustor_core.event import EventBase

if TYPE_CHECKING:
    from fustor_fusion.view_manager.manager import ViewManager

logger = logging.getLogger("fustor_fusion")


class ViewDriverAdapter(ViewHandler):
    """
    Adapts a single ViewDriver to the ViewHandler interface.
    
    This adapter bridges the gap between:
    - fustor_core.drivers.ViewDriver (driver layer)
    - fustor_core.pipeline.handler.ViewHandler (pipeline/handler layer)
    
    Example usage:
        from fustor_view_fs import FSViewDriver
        
        driver = FSViewDriver(
            view_id="fs-view",
            datastore_id="1",
            config={"mode": "live"}
        )
        handler = ViewDriverAdapter(driver)
        
        # Now usable with FusionPipeline
        pipeline = FusionPipeline(
            pipeline_id="ds-1",
            config={...},
            view_handlers=[handler]
        )
    """
    
    def __init__(
        self,
        driver: ViewDriver,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the adapter.
        
        Args:
            driver: The underlying ViewDriver instance
            config: Optional configuration overrides
        """
        super().__init__(
            handler_id=driver.view_id,
            config=config or driver.config
        )
        self._driver = driver
        self.schema_name = driver.target_schema or "view"
        self._initialized = False
    
    @property
    def driver(self) -> ViewDriver:
        """Access the underlying driver."""
        return self._driver
    
    async def initialize(self) -> None:
        """Initialize the handler."""
        if not self._initialized:
            if hasattr(self._driver, 'initialize'):
                await self._driver.initialize()
            self._initialized = True
            logger.debug(f"ViewDriverAdapter {self.id} initialized")
    
    async def close(self) -> None:
        """Close the handler and release resources."""
        if self._initialized:
            if hasattr(self._driver, 'close'):
                result = self._driver.close()
                if hasattr(result, '__await__'):
                    await result
            self._initialized = False
            logger.debug(f"ViewDriverAdapter {self.id} closed")
    
    async def process_event(self, event: Any) -> bool:
        """
        Process a single event.
        
        Delegates to the underlying driver's process_event method.
        """
        # Convert dict to EventBase if needed
        if isinstance(event, dict):
            event = EventBase.model_validate(event)
        
        result = await self._driver.process_event(event)
        return result if isinstance(result, bool) else True
    
    async def get_data_view(self, **kwargs) -> Any:
        """
        Get the data view from the driver.
        
        Delegates to the underlying driver's get_data_view method.
        """
        return await self._driver.get_data_view(**kwargs)
    
    async def on_session_start(self) -> None:
        """Handle session start."""
        if hasattr(self._driver, 'on_session_start'):
            await self._driver.on_session_start()
    
    async def on_session_close(self) -> None:
        """Handle session close."""
        if hasattr(self._driver, 'on_session_close'):
            await self._driver.on_session_close()
    
    async def handle_audit_start(self) -> None:
        """Handle audit start."""
        if hasattr(self._driver, 'handle_audit_start'):
            await self._driver.handle_audit_start()
    
    async def handle_audit_end(self) -> None:
        """Handle audit end."""
        if hasattr(self._driver, 'handle_audit_end'):
            await self._driver.handle_audit_end()
    
    async def reset(self) -> None:
        """Reset the driver state."""
        if hasattr(self._driver, 'reset'):
            await self._driver.reset()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from the driver."""
        if hasattr(self._driver, 'get_stats'):
            return self._driver.get_stats()
        return {}
    
    @property
    def requires_full_reset_on_session_close(self) -> bool:
        """Check if driver requires full reset."""
        return getattr(self._driver, 'requires_full_reset_on_session_close', False)


class ViewManagerAdapter(ViewHandler):
    """
    Adapts the entire ViewManager to a single ViewHandler interface.
    
    This is useful when you want to treat the ViewManager's multi-driver
    routing as a single handler in the FusionPipeline.
    
    Example usage:
        from fustor_fusion.view_manager.manager import ViewManager
        
        manager = ViewManager(view_id="1")
        await manager.initialize_providers()
        
        handler = ViewManagerAdapter(manager)
        
        # Use as a single handler
        pipeline = FusionPipeline(
            view_handlers=[handler]
        )
    """
    
    def __init__(
        self,
        view_manager: "ViewManager",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the adapter.
        
        Args:
            view_manager: The ViewManager instance
            config: Optional configuration overrides
        """
        super().__init__(
            handler_id=f"view-manager-{view_manager.view_id}",
            config=config or {}
        )
        self._manager = view_manager
        self.schema_name = "view-manager"
    
    @property
    def manager(self) -> "ViewManager":
        """Access the underlying ViewManager."""
        return self._manager
    
    async def initialize(self) -> None:
        """Initialize the view manager's providers."""
        await self._manager.initialize_providers()
        logger.debug(f"ViewManagerAdapter {self.id} initialized")
    
    async def close(self) -> None:
        """Close all providers."""
        # ViewManager doesn't have explicit close, but providers might
        for provider_id, provider in self._manager.providers.items():
            if hasattr(provider, 'close'):
                await provider.close()
        logger.debug(f"ViewManagerAdapter {self.id} closed")
    
    async def process_event(self, event: Any) -> bool:
        """
        Process an event through all view providers.
        
        Delegates to the ViewManager's process_event method.
        """
        # Convert dict to EventBase if needed
        if isinstance(event, dict):
                event = EventBase.model_validate(event)
        
        results = await self._manager.process_event(event)
        # Return True if any provider processed successfully
        return any(
            (r if isinstance(r, bool) else r.get("success", False))
            for r in results.values()
        )
    
    async def get_data_view(self, **kwargs) -> Any:
        """
        Get data view from a specific provider.
        
        Args:
            provider: The provider name to query
            **kwargs: Provider-specific parameters
        
        Returns:
            The data view from the specified provider
        """
        provider_name = kwargs.pop("provider", None)
        if provider_name:
            return self._manager.get_data_view(provider_name, **kwargs)
        
        # Return all available providers if none specified
        return {
            "providers": self._manager.get_available_providers()
        }
    
    async def on_session_start(self) -> None:
        """Handle session start for all providers."""
        await self._manager.on_session_start()
    
    async def on_session_close(self) -> None:
        """Handle session close for all providers."""
        await self._manager.on_session_close()
    
    async def handle_audit_start(self) -> None:
        """Handle audit start for all providers."""
        for provider in self._manager.providers.values():
            if hasattr(provider, 'handle_audit_start'):
                await provider.handle_audit_start()
    
    async def handle_audit_end(self) -> None:
        """Handle audit end for all providers."""
        for provider in self._manager.providers.values():
            if hasattr(provider, 'handle_audit_end'):
                await provider.handle_audit_end()
    
    async def reset(self) -> None:
        """Reset all providers."""
        for provider in self._manager.providers.values():
            if hasattr(provider, 'reset'):
                await provider.reset()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from ViewManager."""
        return self._manager.get_aggregated_stats()
    
    def get_available_providers(self) -> List[str]:
        """Get list of available provider names."""
        return self._manager.get_available_providers()
    
    def get_provider(self, name: str) -> Optional[ViewDriver]:
        """Get a specific provider by name."""
        return self._manager.get_provider(name)


def create_view_handler_from_driver(
    driver: ViewDriver,
    config: Optional[Dict[str, Any]] = None
) -> ViewDriverAdapter:
    """
    Create a ViewHandler from a ViewDriver.
    
    Args:
        driver: The ViewDriver instance
        config: Optional configuration override
        
    Returns:
        A ViewDriverAdapter instance
    """
    return ViewDriverAdapter(driver, config)


def create_view_handler_from_manager(
    view_manager: "ViewManager",
    config: Optional[Dict[str, Any]] = None
) -> ViewManagerAdapter:
    """
    Create a ViewHandler from a ViewManager.
    
    Args:
        view_manager: The ViewManager instance
        config: Optional configuration override
        
    Returns:
        A ViewManagerAdapter instance
    """
    return ViewManagerAdapter(view_manager, config)
