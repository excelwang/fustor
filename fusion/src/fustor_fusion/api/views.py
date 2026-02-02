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
from ..auth.dependencies import get_view_id_from_api_key
from ..view_state_manager import view_state_manager
from .. import runtime_objects
from ..config.views import views_config

logger = logging.getLogger(__name__)

view_router = APIRouter(tags=["Data Views"])


async def _check_core_readiness(view_id: str):
    """Internal helper to check core system readiness (snapshot signal, queue, inflight)."""
    is_signal_complete = await view_state_manager.is_snapshot_complete(view_id)
    
    queue_size = 0
    pm = runtime_objects.pipeline_manager
    if pm:
        pipelines = pm.get_pipelines()
        for p in pipelines.values():
             if hasattr(p, 'view_id') and p.view_id == view_id:
                 # Check queue size of feeding pipelines
                 dto = await p.get_dto()
                 queue_size += dto.get('queue_size', 0)

    # In V2, queue_size covers pending work. Inflight is considered 0 if queue is empty as processing is serial.
    inflight_count = 0 
    
    if not is_signal_complete or queue_size > 0:
        detail = f"View {view_id} not ready: snapshot_complete={is_signal_complete}, queue={queue_size}. "
        if not is_signal_complete:
            detail += "Initial snapshot sync in progress. Waiting for end signal from authoritative agent."
        else:
            detail += "Events still processing in queue."
            
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail
        )

def make_readiness_checker(lookup_key: str):
    """
    Factory to create a complete readiness checker for a specific provider instance.
    Checks:
    1. Core System Readiness (Snapshot, Queue, Inflight)
    2. Live Session Requirement (if Provider is configured as Live)
    """
    async def _check(view_id: str):
        # 1. Check core readiness
        await _check_core_readiness(view_id)
        
        # 2. Check "live" mode
        from ..core.session_manager import session_manager
        
        manager = await get_cached_view_manager(view_id)
        provider = manager.providers.get(lookup_key)
        
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
            sessions = await session_manager.get_view_sessions(view_id)
            if not sessions:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No active sessions for this live datastore. Service temporarily unavailable."
                )
    return _check


async def check_snapshot_status(view_id: str):
    """
    Generic core readiness check (Snapshot + Queue + Inflight).
    Usage: For generic endpoints that don't target a specific view driver.
    """
    await _check_core_readiness(view_id)


async def get_view_provider(view_id: str, lookup_key: str):
    """
    Helper to get a view provider for a specific key (instance name or driver name).
    
    Args:
        view_id: The view ID
        lookup_key: The key used to look up the provider in ViewManager
    """
    manager = await get_cached_view_manager(view_id)
    return manager.providers.get(lookup_key)


def _discover_view_api_factories():
    """
    Discover view API router factories from view driver packages.
    
    Returns:
        List of (driver_name, create_router_func) tuples
    """
    factories = []
    try:
        eps = entry_points(group="fustor.view_api")
        for ep in eps:
            try:
                create_router_func = ep.load()
                factories.append((ep.name, create_router_func))
                logger.info(f"Discovered view API factory: {ep.name}")
            except Exception as e:
                logger.error(f"Failed to load view API entry point '{ep.name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error discovering view API entry points: {e}", exc_info=True)
    
    return factories


def register_view_driver_routes():
    """
    Dynamically register API routes from view driver packages.
    Routes are discovered via the 'fustor.view_api' entry point group.
    
    Registration Priority:
    1. If views are defined in local_config, register each view_name as a prefix.
    2. Fallback: Register the driver names themselves as prefixes.
    """
    available_factories = {name: func for name, func in _discover_view_api_factories()}
    
    if not available_factories:
        logger.warning("No view API factories discovered. Check if view driver packages are installed.")
        return

    # 1. Try to register based on specific view instances in config
    view_configs = views_config.get_all()
    registered_count = 0
    
    if view_configs:
        for view_name, cfg in view_configs.items():
            # Skip disabled views
            if cfg.disabled:
                continue
                
            driver_name = cfg.driver
            create_func = available_factories.get(driver_name)
            
            if create_func:
                try:
                    # Create context-bound provider getter and readiness checker
                    async def get_provider_for_instance(view_id: str, _key=view_name):
                        return await get_view_provider(view_id, _key)
                    
                    checker = make_readiness_checker(view_name)
                    
                    router = create_func(
                        get_provider_func=get_provider_for_instance,
                        check_snapshot_func=checker,
                        get_view_id_dep=get_view_id_from_api_key
                    )
                    
                    # Register with prefix matching the view_name (e.g., test-fs)
                    view_router.include_router(router, prefix=f"/{view_name}")
                    logger.info(f"Registered view API routes: {view_name} (driver: {driver_name}) at prefix /{view_name}")
                    registered_count += 1
                except Exception as e:
                    logger.error(f"Error registering view API routes '{view_name}': {e}", exc_info=True)
            else:
                logger.warning(f"View '{view_name}' configures driver '{driver_name}', but no API factory for that driver was found.")

    # 2. If no config or no views registered via config, fall back to driver names
    if registered_count == 0:
        for name, create_func in available_factories.items():
            try:
                # Fallback uses driver name as the lookup key
                async def get_provider_fallback(view_id: str, _key=name):
                    return await get_view_provider(view_id, _key)
                
                checker = make_readiness_checker(name)
                
                router = create_func(
                    get_provider_func=get_provider_fallback,
                    check_snapshot_func=checker,
                    get_view_id_dep=get_view_id_from_api_key
                )
                
                view_router.include_router(router, prefix=f"/{name}")
                logger.info(f"Registered fallback view API routes: {name} at prefix /{name}")
            except Exception as e:
                logger.error(f"Error registering fallback view API routes '{name}': {e}", exc_info=True)


# Register routes when module is loaded
register_view_driver_routes()
