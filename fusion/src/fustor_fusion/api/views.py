"""
API endpoints for data views.

View-specific endpoints are provided by view driver packages
and dynamically registered via entry points.
Each view driver can export a 'create_router' function to provide its API routes.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from importlib.metadata import entry_points
import logging

from ..view_manager.manager import get_cached_view_manager
from ..auth.dependencies import get_datastore_id_from_api_key
from ..datastore_state_manager import datastore_state_manager
from ..in_memory_queue import memory_event_queue
from ..processing_manager import processing_manager

logger = logging.getLogger(__name__)

view_router = APIRouter(tags=["Data Views"])


async def _check_core_readiness(datastore_id: int):
    """Internal helper to check core system readiness (snapshot signal, queue, inflight)."""
    is_signal_complete = await datastore_state_manager.is_snapshot_complete(datastore_id)
    queue_size = memory_event_queue.get_queue_size(datastore_id)
    inflight_count = processing_manager.get_inflight_count(datastore_id)
    
    if not is_signal_complete or queue_size > 0 or inflight_count > 0:
        detail = "Initial snapshot sync in progress. Service temporarily unavailable for this datastore."
        if is_signal_complete and (queue_size > 0 or inflight_count > 0):
            detail = f"Sync signal received, but still processing ingested data: queue={queue_size}, inflight={inflight_count}."
            logger.info(f"Datastore {datastore_id} {detail}")
            
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail
        )

def make_readiness_checker(driver_name: str):
    """
    Factory to create a complete readiness checker for a specific driver.
    Checks:
    1. Core System Readiness (Snapshot, Queue, Inflight)
    2. Live Session Requirement (if Provider is configured as Live)
    """
    async def _check(datastore_id: int):
        # 1. Check core readiness
        await _check_core_readiness(datastore_id)
        
        # 2. Check "live" mode
        from ..core.session_manager import session_manager
        
        manager = await get_cached_view_manager(datastore_id)
        provider = manager.providers.get(driver_name)
        
        # Determine if we need to enforce live session check
        is_live = False
        if provider:
            # Prefer property check if available (standard interface)
            if getattr(provider, "requires_full_reset_on_session_close", False):
                is_live = True
            elif provider.config:
                # Fallback to config inspection
                is_live = provider.config.get("mode") == "live" or provider.config.get("is_live") is True
        
        if is_live:
            sessions = await session_manager.get_datastore_sessions(datastore_id)
            if not sessions:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No active sessions for this live datastore. Service temporarily unavailable."
                )
    return _check


async def check_snapshot_status(datastore_id: int):
    """
    Generic core readiness check (Snapshot + Queue + Inflight).
    Usage: For generic endpoints that don't target a specific view driver.
    """
    await _check_core_readiness(datastore_id)


async def get_view_provider(datastore_id: int, driver_name: str):
    """
    Helper to get a view provider for a specific driver.
    
    Args:
        datastore_id: The datastore ID
        driver_name: The driver name (e.g., 'fs')
    """
    manager = await get_cached_view_manager(datastore_id)
    return manager.providers.get(driver_name)


def _discover_view_api_routers():
    """
    Discover and load API routers from view driver packages.
    
    View driver packages can register API routers via the 'fustor.view_api' entry point group.
    Each entry point should be a function that accepts:
        - get_provider_func: async function(datastore_id, driver_name) -> provider
        - check_snapshot_func: async function(datastore_id) -> None (raises HTTPException)
        - get_datastore_id_dep: FastAPI dependency for authentication
    
    Returns:
        List of (name, router) tuples
    """
    routers = []
    try:
        eps = entry_points(group="fustor.view_api")
        for ep in eps:
            try:
                create_router_func = ep.load()
                driver_name = ep.name
                
                # Create a provider getter bound to this driver name
                async def get_provider_for_driver(datastore_id: int, _driver=driver_name):
                    return await get_view_provider(datastore_id, _driver)
                
                # Create specialized checker that covers all readiness logic
                checker = make_readiness_checker(driver_name)
                
                router = create_router_func(
                    get_provider_func=get_provider_for_driver,
                    check_snapshot_func=checker,
                    get_datastore_id_dep=get_datastore_id_from_api_key
                )
                routers.append((ep.name, router))
                logger.info(f"Discovered view API router: {ep.name}")
            except Exception as e:
                logger.error(f"Failed to load view API entry point '{ep.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error discovering view API entry points: {e}", exc_info=True)
    
    return routers


def register_view_driver_routes():
    """
    Dynamically register API routes from view driver packages.
    Routes are discovered via the 'fustor.view_api' entry point group.
    """
    routers = _discover_view_api_routers()
    for name, router in routers:
        try:
            # Register with prefix matching the view_id (driver name)
            # This allows multiple drivers or custom driver IDs to coexist
            view_router.include_router(router, prefix=f"/{name}")
            logger.info(f"Registered view API routes: {name} at prefix /{name}")
        except Exception as e:
            logger.error(f"Error registering view API routes '{name}': {e}", exc_info=True)
    
    if not routers:
        logger.warning("No view API routers discovered. Check if view driver packages are installed.")


# Register routes when module is loaded
register_view_driver_routes()
