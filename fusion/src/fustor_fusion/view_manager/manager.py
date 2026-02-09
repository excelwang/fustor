"""
Main View Manager module that coordinates view driver instances.
This module provides a unified interface for processing various event types
and building corresponding consistent data views.
All data is stored in memory only.

View Drivers are discovered dynamically via the 'fustor.view_drivers' entry point.
"""
from typing import Dict, Any, Optional, Type
from importlib.metadata import entry_points
from fustor_core.drivers import ViewDriver
from ..config import views_config
import logging
import asyncio
from fustor_core.event import EventBase
# from ..in_memory_queue import memory_event_queue

logger = logging.getLogger(__name__)


# --- Global Cache for View Managers ---
# Use the unified registry in runtime_objects to ensure consistency between 
# pipelines, API endpoints, and management commands.
from .. import runtime_objects
_cache_lock = asyncio.Lock()

# --- Cached loaded drivers ---
_loaded_view_drivers: Optional[Dict[str, Type[ViewDriver]]] = None


def _load_view_drivers() -> Dict[str, Type[ViewDriver]]:
    """
    Discover and load all registered ViewDriver implementations via entry points.
    Results are cached for efficiency.
    """
    global _loaded_view_drivers
    if _loaded_view_drivers is not None:
        return _loaded_view_drivers
    
    drivers: Dict[str, Type[ViewDriver]] = {}
    try:
        eps = entry_points(group="fustor.view_drivers")
        for ep in eps:
            try:
                driver_cls = ep.load()
                # Use target_schema if defined, otherwise fall back to entry point name
                key = getattr(driver_cls, 'target_schema', None) or ep.name
                drivers[key] = driver_cls
                logger.info(f"Loaded ViewDriver '{ep.name}' for schema '{key}'")
            except Exception as e:
                logger.error(f"Failed to load ViewDriver entry point '{ep.name}': {e}")
    except Exception as e:
        logger.error(f"Failed to discover ViewDriver entry points: {e}")
    
    _loaded_view_drivers = drivers
    return drivers


class ViewManager:
    """
    Manages multiple view drivers and routes events to appropriate drivers
    based on event type or content.
    """
    
    def __init__(self, view_id: str):
        self.driver_instances: Dict[str, ViewDriver] = {}
        self.logger = logging.getLogger(__name__)
        self.view_id = view_id
    
    async def initialize_driver_instances(self):
        """Initialize view driver instances by loading them from view configs."""
        if not self.view_id:
            return
            
        self.logger.info(f"Initializing view driver instances for view {self.view_id}")
        
        available_drivers = _load_view_drivers()
        
        # Try loading from views_config loader
        view_configs = views_config.get_by_view(self.view_id)
        
        if view_configs:
            self.logger.info(f"Using view configuration for view {self.view_id}: {[v.id for v in view_configs]}")
            for config in view_configs:
                view_name = config.id
                driver_type = config.driver
                if not driver_type:
                    self.logger.warning(f"View '{view_name}' config missing 'driver' field, skipping")
                    continue
                    
                driver_cls = available_drivers.get(driver_type)
                if not driver_cls:
                    self.logger.error(f"Driver type '{driver_type}' not found for view '{view_name}'. Available: {list(available_drivers.keys())}")
                    continue
                
                # Use driver_params directly
                driver_params = config.driver_params
                
                try:
                    driver_instance = driver_cls(
                        id=view_name,
                        view_id=self.view_id,
                        config=driver_params
                    )
                    await driver_instance.initialize()
                    self.driver_instances[view_name] = driver_instance
                    self.logger.info(f"Initialized ViewDriver '{view_name}' (type={driver_type})")
                except Exception as e:
                    self.logger.error(f"Failed to initialize ViewDriver '{view_name}': {e}", exc_info=True)
                    
        else:
            # Fallback: Auto-load all found drivers IF config is empty
            if not views_config.get_all():
                 self.logger.info(f"No fusion config file found, auto-loading all installed view drivers")
                 
                 for schema, driver_cls in available_drivers.items():
                     try:
                         # For auto-discovery, we use the schema name as the view instance name
                         # No default config provided
                         driver_instance = driver_cls(
                             id=schema,
                             view_id=self.view_id,
                             config={}
                         )
                         await driver_instance.initialize()
                         self.driver_instances[schema] = driver_instance
                         self.logger.info(f"Initialized ViewDriver '{schema}' for view {self.view_id}")
                     except Exception as e:
                         self.logger.error(f"Failed to initialize ViewDriver '{schema}': {e}", exc_info=True)
            else:
                 self.logger.info(f"Fusion config active but no views configured for view {self.view_id}")


    
    async def process_event(self, event: EventBase) -> Dict[str, bool]:
        """Process an event with all driver instances and return results"""
        results = {}
        
        for driver_id, driver_instance in self.driver_instances.items():
            try:
                result = await driver_instance.process_event(event)
                results[driver_id] = result
            except Exception as e:
                self.logger.error(f"Error processing event with driver {driver_id}: {e}", exc_info=True)
                results[driver_id] = False
        
        return results
    
    def get_driver_instance(self, name: str) -> Optional[ViewDriver]:
        """Get a driver instance by name."""
        return self.driver_instances.get(name)

    async def get_data_view(self, driver_id: str, **kwargs) -> Optional[Any]:
        """Get the data view from a specific driver instance"""
        driver_instance = self.driver_instances.get(driver_id)
        if driver_instance:
            return await driver_instance.get_data_view(**kwargs)
        return None
    
    def get_available_driver_ids(self) -> list:
        """Get list of available driver instance IDs"""
        return list(self.driver_instances.keys())

    async def cleanup_expired_suspects(self):
        """Cleanup expired suspects in all driver instances that support it."""
        for driver_id, driver_instance in self.driver_instances.items():
            if hasattr(driver_instance, 'cleanup_expired_suspects'):
                try:
                    await driver_instance.cleanup_expired_suspects()
                except Exception as e:
                    self.logger.error(f"Error cleaning up suspects for driver {driver_id}: {e}")


    async def on_session_start(self):
        """Dispatch session start event to all driver instances."""
        for driver_id, driver_instance in self.driver_instances.items():
            await driver_instance.on_session_start()

    async def on_session_close(self):
        """Dispatch session close event to all driver instances for cleanup."""
        for driver_id, driver_instance in self.driver_instances.items():
            await driver_instance.on_session_close()

    async def get_aggregated_stats(self) -> Dict[str, Any]:
        """
        Collect and aggregate stats from all driver instances using standardized interface.
        """
        aggregated = {
            "total_volume": 0,
            "max_latency_ms": 0,
            "max_staleness_seconds": 0,
            "oldest_item_info": None,
            "logical_now": 0,
            "driver_instances": {}
        }
        
        for driver_id, driver_instance in self.driver_instances.items():
            try:
                # Enforce generic stats interface
                if not hasattr(driver_instance, 'get_stats'):
                    self.logger.warning(f"Driver {driver_id} does not implement get_stats(), skipping metrics.")
                    continue

                driver_stats = await driver_instance.get_stats()
                aggregated["driver_instances"][driver_id] = driver_stats
                
                # Aggregate standardized metrics
                # 1. Volume
                aggregated["total_volume"] += driver_stats.get("item_count", 0)
                
                # 2. Latency
                lat = driver_stats.get("latency_ms", 0)
                if lat > aggregated["max_latency_ms"]:
                     aggregated["max_latency_ms"] = lat
                     
                # 3. Logical Clock
                aggregated["logical_now"] = max(aggregated["logical_now"], driver_stats.get("logical_now", 0))

                # 4. Oldest Item / Staleness
                stale = driver_stats.get("staleness_seconds", 0)
                if stale > aggregated["max_staleness_seconds"]:
                    aggregated["max_staleness_seconds"] = stale
                    path = driver_stats.get('oldest_item_path', 'unknown')
                    aggregated["oldest_item_info"] = {
                        "path": f"[{driver_id}] {path}",
                        "age_seconds": stale
                    }
            except Exception as e:
                self.logger.error(f"Failed to get stats from driver {driver_id}: {e}", exc_info=True)
                         
        return aggregated

    async def reset(self):
        """Reset all driver instances."""
        for driver_instance in self.driver_instances.values():
            if hasattr(driver_instance, 'reset'):
                await driver_instance.reset()

async def reset_views(view_id: str) -> bool:
    """
    Reset all views by calling reset() on the cached manager.
    """
    v_id_str = str(view_id)
    logger.info(f"Resetting views for view {v_id_str}")
    try:
        manager = runtime_objects.view_managers.get(v_id_str)
        if manager:
            await manager.reset()
        
        return True
    except Exception as e:
        logger.error(f"Failed to reset views for view {v_id_str}: {e}", exc_info=True)
        return False




async def get_cached_view_manager(view_id: str) -> 'ViewManager':
    """
    Gets a cached ViewManager for a given view_id.
    If not in cache, it creates, initializes, and caches one.
    """
    v_id_str = str(view_id)
    if v_id_str in runtime_objects.view_managers:
        return runtime_objects.view_managers[v_id_str]

    async with _cache_lock:
        if v_id_str in runtime_objects.view_managers:
            return runtime_objects.view_managers[v_id_str]
        
        logger.info(f"Creating new view manager for view {v_id_str}")
        new_manager = ViewManager(view_id=v_id_str)
        await new_manager.initialize_driver_instances()
        runtime_objects.view_managers[v_id_str] = new_manager
        return new_manager


async def cleanup_all_expired_suspects():
    """Iterate through all cached managers and cleanup suspects."""
    for manager in list(runtime_objects.view_managers.values()):
        try:
            await manager.cleanup_expired_suspects()
        except Exception as e:
            logger.error(f"Error during suspect cleanup for view {manager.view_id}: {e}")


# --- Public Interface for Processing events ---

async def process_event(event: EventBase, view_id: str) -> Dict[str, bool]:
    """Process a single event with all available view driver instances"""
    manager = await get_cached_view_manager(view_id)
    return await manager.process_event(event)


async def on_session_start(view_id: str):
    """Notify view driver instances that a new session has started."""
    manager = await get_cached_view_manager(view_id)
    await manager.on_session_start()


async def on_session_close(view_id: str):
    """Notify view driver instances that a session has closed for cleanup."""
    v_id_str = str(view_id)
    if v_id_str in runtime_objects.view_managers:
        manager = runtime_objects.view_managers[v_id_str]
        await manager.on_session_close()
