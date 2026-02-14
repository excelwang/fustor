# fusion/src/fustor_fusion/runtime/view_handler_adapter.py
"""
Adapter to wrap a ViewDriver or ViewManager as a ViewHandler for use in FusionPipe.

This allows the existing view-fs and other view driver implementations
to be used with the new Pipe-based architecture.
"""
import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from fustor_core.pipe.handler import ViewHandler
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
    - fustor_core.pipe.handler.ViewHandler (pipe/handler layer)
    
    Example usage:
        from fustor_view_fs import FSViewDriver
        
        driver = FSViewDriver(
            view_id="fs-view",
            view_id="1",
            config={"mode": "live"}
        )
        handler = ViewDriverAdapter(driver)
        
        # Now usable with FusionPipe
        pipe = FusionPipe(
            pipe_id="ds-1",
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

    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """Delegate session role resolution to driver."""
        if hasattr(self._driver, 'resolve_session_role'):
            return await self._driver.resolve_session_role(session_id, **kwargs)
        return {"role": "leader"}
    
    async def on_session_start(self, **kwargs) -> None:
        """Handle session start - delegates state reset to driver."""
        if hasattr(self._driver, 'on_session_start'):
             await self._driver.on_session_start(**kwargs)
    
    async def on_session_close(self, **kwargs) -> None:
        """Handle session close."""
        if hasattr(self._driver, 'on_session_close'):
             await self._driver.on_session_close(**kwargs)
    
    async def handle_audit_start(self) -> None:
        """Handle audit start."""
        if hasattr(self._driver, 'handle_audit_start'):
            await self._driver.handle_audit_start()
    
    async def handle_audit_end(self) -> None:
        """Handle audit end."""
        if hasattr(self._driver, 'handle_audit_end'):
            await self._driver.handle_audit_end()

    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None:
        """Handle snapshot complete."""
        if hasattr(self._driver, 'on_snapshot_complete'):
            await self._driver.on_snapshot_complete(session_id=session_id, **kwargs)
    
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
    """
    
    def __init__(
        self,
        view_manager: "ViewManager",
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            handler_id=f"view-manager-{view_manager.view_id}",
            config=config or {}
        )
        self._manager = view_manager
        # "view-manager" acts as a wildcard schema in FusionPipe routing
        self.schema_name = "view-manager"
    
    @property
    def manager(self) -> "ViewManager":
        """Access the underlying ViewManager."""
        return self._manager
    
    async def initialize(self) -> None:
        """Initialize the view manager's driver instances."""
        await self._manager.initialize_driver_instances()
        logger.debug(f"ViewManagerAdapter {self.id} initialized")
    
    async def close(self) -> None:
        """Close all driver instances."""
        # ViewManager doesn't have explicit close, but driver instances might
        for driver_id, driver_instance in self._manager.driver_instances.items():
            if hasattr(driver_instance, 'close'):
                await driver_instance.close()
        logger.debug(f"ViewManagerAdapter {self.id} closed")
    
    async def process_event(self, event: Any) -> bool:
        """
        Process an event through all view driver instances.
        
        Delegates to the ViewManager's process_event method.
        """
        # Convert dict to EventBase if needed
        if isinstance(event, dict):
                event = EventBase.model_validate(event)
        
        results = await self._manager.process_event(event)

        # Return True if any driver processed successfully
        return any(
            (r if isinstance(r, bool) else r.get("success", False))
            for r in results.values()
        )
    
    async def get_data_view(self, **kwargs) -> Any:
        """
        Get data view from a specific driver instance.
        
        Args:
            driver_id: The driver instance ID to query
            **kwargs: Driver-specific parameters
        
        Returns:
            The data view from the specified driver
        """
        driver_id = kwargs.pop("driver_id", None)
        if driver_id:
            return self._manager.get_data_view(driver_id, **kwargs)
        
        # Return all available driver IDs if none specified
        return {
            "driver_instances": self._manager.get_available_driver_ids()
        }
    
    async def on_session_start(self, **kwargs) -> None:
        """Handle session start for all driver instances."""
        await self._manager.wait_until_ready()
        await self._manager.on_session_start(**kwargs)
    
    async def on_session_close(self, **kwargs) -> None:
        """Handle session close for all driver instances."""
        await self._manager.on_session_close(**kwargs)

    async def on_snapshot_complete(self, session_id: str, **kwargs) -> None:
        """Handle snapshot complete for all driver instances."""
        await self._manager.on_snapshot_complete(session_id=session_id, **kwargs)
    
    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Determine session role.
        
        Wait for view manager to be ready before resolving role.
        """
        await self._manager.wait_until_ready()
        
        # 1. Try delegated election first
        for driver_instance in self._manager.driver_instances.values():
            if hasattr(driver_instance, 'resolve_session_role'):
                res = await driver_instance.resolve_session_role(session_id, **kwargs)
                if res.get("role") == "leader":
                    return res

        # 2. Fallback: Perform global election
        from ..view_state_manager import view_state_manager
        # Try to acquire global leadership for this view
        is_leader = await view_state_manager.try_become_leader(self._manager.view_id, session_id)
        if is_leader:
            await view_state_manager.set_authoritative_session(self._manager.view_id, session_id)
            
        return {"role": "leader" if is_leader else "follower"}
    
    async def handle_audit_start(self) -> None:
        """Handle audit start for all driver instances."""
        for driver_instance in self._manager.driver_instances.values():
            if hasattr(driver_instance, 'handle_audit_start'):
                await driver_instance.handle_audit_start()
    
    async def handle_audit_end(self) -> None:
        """Handle audit end for all driver instances."""
        for driver_instance in self._manager.driver_instances.values():
            if hasattr(driver_instance, 'handle_audit_end'):
                await driver_instance.handle_audit_end()
    
    async def reset(self) -> None:
        """Reset all driver instances."""
        for driver_instance in self._manager.driver_instances.values():
            if hasattr(driver_instance, 'reset'):
                await driver_instance.reset()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from ViewManager."""
        return self._manager.get_aggregated_stats()
    
    def get_available_driver_ids(self) -> List[str]:
        """Get list of available driver instance IDs."""
        return self._manager.get_available_driver_ids()
    
    def get_driver_instance(self, name: str) -> Optional[ViewDriver]:
        """Get a specific driver instance by name."""
        return self._manager.get_driver_instance(name)


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
