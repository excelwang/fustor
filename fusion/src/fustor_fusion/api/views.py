# fusion/src/fustor_fusion/api/views.py
"""
Data Views API Router Generator.
"""
import logging
from typing import Callable, Any, Optional, Dict
from fastapi import APIRouter, HTTPException, Depends, status
from importlib.metadata import entry_points

from ..auth.dependencies import get_view_id_from_api_key
from ..view_manager.manager import get_cached_view_manager
from ..config.views import views_config

logger = logging.getLogger(__name__)

# Base router for all view-related endpoints
view_router = APIRouter(tags=["Data Views"])

async def get_view_driver_instance(view_id: str, lookup_key: str):
    """
    Helper to get a view driver instance for a specific key (instance name or driver name).
    """
    manager = await get_cached_view_manager(view_id)
    driver_instance = manager.driver_instances.get(lookup_key)
    
    if not driver_instance:
        # Fallback to checking if there's only one driver instance and using that
        # (Useful if queried by driver name but configured with instance name)
        if len(manager.driver_instances) == 1:
            driver_instance = list(manager.driver_instances.values())[0]
        else:
            logger.warning(f"View driver instance '{lookup_key}' not found in ViewManager '{view_id}' (Available: {list(manager.driver_instances.keys())})")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"View '{lookup_key}' not found or not active"
            )
            
    return driver_instance

def make_readiness_checker(view_name: str) -> Callable:
    """Creates a dependency that ensures a view is ready before allowing API access."""
    async def check_ready(view_id: str = Depends(get_view_id_from_api_key)):
        manager = await get_cached_view_manager(view_id)
        driver_instance = manager.driver_instances.get(view_name)
        
        # 1. Check Global Snapshot Status (via ViewStateManager)
        from ..view_state_manager import view_state_manager
        is_snapshot_complete = await view_state_manager.is_snapshot_complete(view_id)
        if not is_snapshot_complete:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"View '{view_name}': Initial snapshot sync phase in progress",
                headers={"Retry-After": "5"}
            )
            
        # 2. Check Driver Instance Specific Readiness
        if not driver_instance and len(manager.driver_instances) == 1:
            driver_instance = list(manager.driver_instances.values())[0]
            
        if not driver_instance:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"View '{view_name}' not found"
            )

        # Check if driver instance has readiness flag (e.g. initial snapshot done)
        # Note: FSViewDriver implementation uses is_ready property
        is_ready = getattr(driver_instance, "is_ready", True)
        if callable(is_ready):
            is_ready = is_ready()
            
        if not is_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"View '{view_name}' is still performing initial synchronization",
                headers={"Retry-After": "5"}
            )
        return True
    return check_ready

def _discover_view_api_factories():
    """Discover API router factories from entry points."""
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

def setup_view_routers():
    """
    Dynamically register API routers for all configured views.
    This is called during application lifespan startup to ensure that 
    newly injected configurations are picked up.
    """
    # Clear existing routes to avoid duplicates
    view_router.routes = []
    
    # Reload config to ensure we have the latest
    views_config.reload()
    current_view_configs = views_config.get_all()
    
    available_factories = {name: func for name, func in _discover_view_api_factories()}
    
    if not available_factories:
        logger.warning("No view API factories discovered. Check if view driver packages are installed.")
        return

    registered_count = 0
    
    # 1. Registered via config
    if current_view_configs:
        for view_name, cfg in current_view_configs.items():
            # Skip disabled views
            if cfg.disabled:
                continue
                
            driver_name = cfg.driver
            create_func = available_factories.get(driver_name)
            
            if create_func:
                try:
                    # Create context-bound driver instance getter and readiness checker
                    async def get_driver_instance_for_instance(view_id: str, _key=view_name):
                        return await get_view_driver_instance(view_id, _key)
                    
                    checker = make_readiness_checker(view_name)
                    
                    router = create_func(
                        get_provider_func=get_driver_instance_for_instance,
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
                async def get_driver_instance_fallback(view_id: str, _key=name):
                    return await get_view_driver_instance(view_id, _key)
                
                checker = make_readiness_checker(name)
                
                router = create_func(
                    get_provider_func=get_driver_instance_fallback,
                    check_snapshot_func=checker,
                    get_view_id_dep=get_view_id_from_api_key
                )
                
                view_router.include_router(router, prefix=f"/{name}")
                logger.info(f"Registered fallback view API routes: {name} at prefix /{name}")
            except Exception as e:
                logger.error(f"Error registering fallback API routes for '{name}': {e}", exc_info=True)

# Initial call to attempt registration (will be called again in lifespan for certainty)
setup_view_routers()
