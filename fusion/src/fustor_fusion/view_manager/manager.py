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
from ..config import fusion_config
import logging
import asyncio
from fustor_event_model.models import EventBase
from ..in_memory_queue import memory_event_queue

logger = logging.getLogger(__name__)


# --- Global Cache for View Managers ---
_view_manager_cache: Dict[int, 'ViewManager'] = {}
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
    
    def __init__(self, datastore_id: int = None):
        self.providers: Dict[str, ViewDriver] = {}
        self.logger = logging.getLogger(__name__)
        self.datastore_id = datastore_id
    
    async def initialize_providers(self):
        """Initialize view providers by loading them from entry points."""
        if not self.datastore_id:
            return
            
        self.logger.info(f"Initializing view providers for datastore {self.datastore_id}")
        
        drivers = _load_view_drivers()
        config = {"hot_file_threshold": fusion_config.FUSTOR_FUSION_SUSPECT_TTL_SECONDS}
        
        for schema, driver_cls in drivers.items():
            try:
                self.providers[schema] = driver_cls(
                    datastore_id=self.datastore_id,
                    config=config
                )
                self.logger.info(f"Initialized ViewDriver '{schema}' for datastore {self.datastore_id}")
            except Exception as e:
                self.logger.error(f"Failed to initialize ViewDriver '{schema}': {e}", exc_info=True)
    
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
    
    async def get_file_directory_provider(self) -> ViewDriver:
        """Get the file directory structure provider"""
        return self.providers["file_directory"]
    
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
        """Cleanup expired suspects in all supporting providers."""
        provider = self.providers.get("file_directory")
        if provider:
            await provider.cleanup_expired_suspects()

    async def on_session_start(self, session_id: str):
        """Dispatch session start event to all providers."""
        for name, provider in self.providers.items():
            await provider.on_session_start(session_id)

    async def on_session_close(self, session_id: str):
        """Dispatch session close event to all providers for cleanup."""
        for name, provider in self.providers.items():
            await provider.on_session_close(session_id)


async def get_cached_view_manager(datastore_id: int) -> 'ViewManager':
    """
    Gets a cached ViewManager for a given datastore_id.
    If not in cache, it creates, initializes, and caches one.
    """
    if datastore_id in _view_manager_cache:
        return _view_manager_cache[datastore_id]

    async with _cache_lock:
        if datastore_id in _view_manager_cache:
            return _view_manager_cache[datastore_id]
        
        logger.info(f"Creating new view manager for datastore {datastore_id}")
        new_manager = ViewManager(datastore_id=datastore_id)
        await new_manager.initialize_providers()
        _view_manager_cache[datastore_id] = new_manager
        return new_manager


async def cleanup_all_expired_suspects():
    """Iterate through all cached managers and cleanup suspects."""
    for manager in list(_view_manager_cache.values()):
        try:
            await manager.cleanup_expired_suspects()
        except Exception as e:
            logger.error(f"Error during suspect cleanup for datastore {manager.datastore_id}: {e}")


# --- Public Interface for Processing events ---

async def process_event(event: EventBase, datastore_id: int) -> Dict[str, bool]:
    """Process a single event with all available view providers"""
    manager = await get_cached_view_manager(datastore_id)
    return await manager.process_event(event)


# --- Public Interface for Data Access ---

async def get_directory_tree(path: str = "/", datastore_id: int = None, recursive: bool = True, max_depth: Optional[int] = None, only_path: bool = False) -> Optional[Dict[str, Any]]:
    """Get the directory tree from the file directory provider"""
    if datastore_id:
        manager = await get_cached_view_manager(datastore_id)
        provider = await manager.get_file_directory_provider()
        if provider:
            return await provider.get_directory_tree(path, recursive=recursive, max_depth=max_depth, only_path=only_path)
    return None

async def search_files(pattern: str, datastore_id: int = None) -> list:
    """Search for files using the file directory provider"""
    if datastore_id:
        manager = await get_cached_view_manager(datastore_id)
        provider = await manager.get_file_directory_provider()
        if provider:
            return await provider.search_files(pattern)
    return []

async def get_directory_stats(datastore_id: int = None) -> Dict[str, Any]:
    """Get directory statistics using the file directory provider"""
    if datastore_id:
        manager = await get_cached_view_manager(datastore_id)
        provider = await manager.get_file_directory_provider()
        if provider:
            return await provider.get_directory_stats()
    return {}

async def reset_directory_tree(datastore_id: int) -> bool:
    """
    Reset the directory tree by clearing all entries for a specific datastore.
    """
    logger.info(f"Resetting directory tree for datastore {datastore_id}")
    try:
        async with _cache_lock:
            if datastore_id in _view_manager_cache:
                del _view_manager_cache[datastore_id]
        
        await memory_event_queue.clear_datastore_data(datastore_id)
        return True
    except Exception as e:
        logger.error(f"Failed to reset directory tree for datastore {datastore_id}: {e}", exc_info=True)
        return False

async def on_session_start(datastore_id: int, session_id: str):
    """Notify view providers that a new session has started."""
    manager = await get_cached_view_manager(datastore_id)
    await manager.on_session_start(session_id)
async def on_session_close(datastore_id: int, session_id: str):
    """Notify view providers that a session has closed for cleanup."""
    if datastore_id in _view_manager_cache:
        manager = _view_manager_cache[datastore_id]
        await manager.on_session_close(session_id)

