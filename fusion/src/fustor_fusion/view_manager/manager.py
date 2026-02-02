"""
Main View Manager module that coordinates different view providers.
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
_view_manager_cache: Dict[str, 'ViewManager'] = {}
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
    
    def __init__(self, datastore_id: str = None, view_id: str = None):
        self.providers: Dict[str, ViewDriver] = {}
        self.logger = logging.getLogger(__name__)
        self.view_id = view_id or datastore_id
        self.datastore_id = self.view_id
    
    async def initialize_providers(self):
        """Initialize view providers by loading them from entry points."""
        if not self.datastore_id:
            return
            
        self.logger.info(f"Initializing view providers for datastore {self.datastore_id}")
        
        available_drivers = _load_view_drivers()
        
        # Try loading from views_config loader
        view_configs = views_config.get_by_datastore(self.datastore_id)
        
        if view_configs:
            self.logger.info(f"Using view configuration for datastore {self.datastore_id}: {[v.id for v in view_configs]}")
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
                    provider = driver_cls(
                        id=view_name,
                        view_id=self.view_id,
                        config=driver_params
                    )
                    await provider.initialize()
                    self.providers[view_name] = provider
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
                         provider = driver_cls(
                             id=schema,
                             view_id=self.view_id,
                             config={}
                         )
                         await provider.initialize()
                         self.providers[schema] = provider
                         self.logger.info(f"Initialized ViewDriver '{schema}' for datastore {self.datastore_id}")
                     except Exception as e:
                         self.logger.error(f"Failed to initialize ViewDriver '{schema}': {e}", exc_info=True)
            else:
                 self.logger.info(f"Fusion config active but no views configured for datastore {self.datastore_id}")


    
    async def process_event(self, event: EventBase) -> Dict[str, bool]:
        """Process an event with all applicable providers and return results"""
        results = {}
        
        for provider_name, provider in self.providers.items():
            try:
                result = await provider.process_event(event)
                results[provider_name] = result
            except Exception as e:
                self.logger.error(f"Error processing event with provider {provider_name}: {e}", exc_info=True)
                results[provider_name] = False
        
        return results
    
    def get_provider(self, name: str) -> Optional[ViewDriver]:
        """Get a provider by name."""
        return self.providers.get(name)

    async def get_data_view(self, provider_name: str, **kwargs) -> Optional[Any]:
        """Get the data view from a specific provider"""
        provider = self.providers.get(provider_name)
        if provider:
            return await provider.get_data_view(**kwargs)
        return None
    
    def get_available_providers(self) -> list:
        """Get list of available provider names"""
        return list(self.providers.keys())

    async def cleanup_expired_suspects(self):
        """Cleanup expired suspects in all providers that support it."""
        for name, provider in self.providers.items():
            if hasattr(provider, 'cleanup_expired_suspects'):
                try:
                    await provider.cleanup_expired_suspects()
                except Exception as e:
                    self.logger.error(f"Error cleaning up suspects for provider {name}: {e}")


    async def on_session_start(self):
        """Dispatch session start event to all providers."""
        for name, provider in self.providers.items():
            await provider.on_session_start()

    async def on_session_close(self):
        """Dispatch session close event to all providers for cleanup."""
        for name, provider in self.providers.items():
            await provider.on_session_close()

    async def get_aggregated_stats(self) -> Dict[str, Any]:
        """
        Collect and aggregate stats from all providers using standardized interface.
        """
        aggregated = {
            "total_volume": 0,
            "max_latency_ms": 0,
            "max_staleness_seconds": 0,
            "oldest_item_info": None,
            "logical_now": 0,
            "providers": {}
        }
        
        for name, provider in self.providers.items():
            try:
                # Enforce generic stats interface
                if not hasattr(provider, 'get_stats'):
                    self.logger.warning(f"Provider {name} does not implement get_stats(), skipping metrics.")
                    continue

                provider_stats = await provider.get_stats()
                aggregated["providers"][name] = provider_stats
                
                # Aggregate standardized metrics
                # 1. Volume
                aggregated["total_volume"] += provider_stats.get("item_count", 0)
                
                # 2. Latency
                lat = provider_stats.get("latency_ms", 0)
                if lat > aggregated["max_latency_ms"]:
                     aggregated["max_latency_ms"] = lat
                     
                # 3. Logical Clock
                aggregated["logical_now"] = max(aggregated["logical_now"], provider_stats.get("logical_now", 0))

                # 4. Oldest Item / Staleness
                stale = provider_stats.get("staleness_seconds", 0)
                if stale > aggregated["max_staleness_seconds"]:
                    aggregated["max_staleness_seconds"] = stale
                    # We might need provider-specific formatting for path, but generally we just preface with provider name
                    path = provider_stats.get('oldest_item_path', 'unknown')
                    aggregated["oldest_item_info"] = {
                        "path": f"[{name}] {path}",
                        "age_seconds": stale
                    }
            except Exception as e:
                self.logger.error(f"Failed to get stats from provider {name}: {e}", exc_info=True)
                         
        return aggregated

async def reset_views(datastore_id: str) -> bool:
    """
    Reset all views by clearing cached manager and data for a specific datastore.
    """
    ds_id_str = str(datastore_id)
    logger.info(f"Resetting views for datastore {ds_id_str}")
    try:
        async with _cache_lock:
            if ds_id_str in _view_manager_cache:
                del _view_manager_cache[ds_id_str]
        
        # await memory_event_queue.clear_datastore_data(ds_id_str)
        return True
    except Exception as e:
        logger.error(f"Failed to reset views for datastore {ds_id_str}: {e}", exc_info=True)
        return False




async def get_cached_view_manager(datastore_id: str) -> 'ViewManager':
    """
    Gets a cached ViewManager for a given datastore_id.
    If not in cache, it creates, initializes, and caches one.
    """
    ds_id_str = str(datastore_id)
    if ds_id_str in _view_manager_cache:
        return _view_manager_cache[ds_id_str]

    async with _cache_lock:
        if ds_id_str in _view_manager_cache:
            return _view_manager_cache[ds_id_str]
        
        logger.info(f"Creating new view manager for datastore {ds_id_str}")
        new_manager = ViewManager(datastore_id=ds_id_str)
        await new_manager.initialize_providers()
        _view_manager_cache[ds_id_str] = new_manager
        return new_manager


async def cleanup_all_expired_suspects():
    """Iterate through all cached managers and cleanup suspects."""
    for manager in list(_view_manager_cache.values()):
        try:
            await manager.cleanup_expired_suspects()
        except Exception as e:
            logger.error(f"Error during suspect cleanup for datastore {manager.datastore_id}: {e}")


# --- Public Interface for Processing events ---

async def process_event(event: EventBase, datastore_id: str) -> Dict[str, bool]:
    """Process a single event with all available view providers"""
    manager = await get_cached_view_manager(datastore_id)
    return await manager.process_event(event)


async def on_session_start(datastore_id: str):
    """Notify view providers that a new session has started."""
    manager = await get_cached_view_manager(datastore_id)
    await manager.on_session_start()


async def on_session_close(datastore_id: str):
    """Notify view providers that a session has closed for cleanup."""
    ds_id_str = str(datastore_id)
    if ds_id_str in _view_manager_cache:
        manager = _view_manager_cache[ds_id_str]
        await manager.on_session_close()
