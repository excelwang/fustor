# fusion/src/fustor_fusion/api/management.py
"""
Management API for dynamic view start/stop operations.
"""
import logging
from fastapi import APIRouter, HTTPException, status

from ..config import views_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/management", tags=["Management"])


@router.post("/views/{view_id}/start")
async def start_view(view_id: str):
    """
    Start a view by ID.
    
    1. Load view config from YAML
    2. Register view with ViewManager
    3. If active session exists, trigger on_session_start
    """
    from ..runtime_objects import view_managers
    from ..core.session_manager import session_manager
    
    # Load view config
    config = views_config.get(view_id)
    if not config:
        # Try reloading in case file was just added
        views_config.reload()
        config = views_config.get(view_id)
    
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"View config '{view_id}' not found in views-config/"
        )
    
    if config.disabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"View '{view_id}' is disabled in config"
        )
    
    # Get or create ViewManager for this datastore
    ds_id = config.datastore_id
    if ds_id not in view_managers:
        from ..view_manager.manager import ViewManager
        view_managers[ds_id] = ViewManager(view_id=ds_id)
    
    vm = view_managers[ds_id]
    
    # Check if view already running
    if view_id in vm.providers:
        return {"status": "already_running", "view_id": view_id}
    
    # Register view provider
    try:
        # Load the driver and create provider
        from fustor_fusion_sdk.loaders import load_view
        provider_class = load_view(config.driver)
        provider = provider_class(
            view_id=view_id,
            datastore_id=ds_id,
            config=config.driver_params
        )
        await provider.initialize()
        vm.providers[view_id] = provider
        logger.info(f"Registered view provider: {view_id}")
        
        # If active session exists, trigger on_session_start
        sessions = await session_manager.get_view_sessions(ds_id)
        if sessions:
            session_id = list(sessions.keys())[0]
            await provider.on_session_start()
            logger.info(f"Triggered on_session_start for view {view_id}")
        
    except Exception as e:
        logger.error(f"Failed to start view {view_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start view: {e}"
        )
    
    return {"status": "started", "view_id": view_id, "datastore_id": ds_id}


@router.post("/views/{view_id}/stop")
async def stop_view(view_id: str):
    """
    Stop a view by ID.
    
    1. Unregister view from ViewManager
    2. If no other views for this datastore, terminate sessions
    3. Return should_shutdown if no views left at all
    """
    from ..runtime_objects import view_managers
    from ..core.session_manager import session_manager
    
    # Find which ViewManager has this view
    ds_id = None
    for manager_id, vm in view_managers.items():
        if view_id in vm.providers:
            ds_id = manager_id
            break
    
    if ds_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"View '{view_id}' is not running"
        )
    
    vm = view_managers[ds_id]
    
    # Unregister view
    try:
        provider = vm.providers.pop(view_id)
        await provider.cleanup()
        logger.info(f"Stopped view: {view_id}")
    except Exception as e:
        logger.error(f"Error stopping view {view_id}: {e}", exc_info=True)
    
    # Check if other views exist for this datastore
    remaining_views = len(vm.providers)
    
    if remaining_views == 0:
        # No views left for this datastore, terminate sessions
        sessions = await session_manager.get_view_sessions(ds_id)
        for session_id in list(sessions.keys()):
            await session_manager.terminate_session(view_id=ds_id, session_id=session_id)
            logger.info(f"Terminated session {session_id} for datastore {ds_id}")
        
        # Remove empty ViewManager
        del view_managers[ds_id]
    
    # Check if any views left globally
    total_views = sum(len(vm.providers) for vm in view_managers.values())
    should_shutdown = total_views == 0
    
    return {
        "status": "stopped",
        "view_id": view_id,
        "should_shutdown": should_shutdown
    }


@router.get("/views")
async def list_views():
    """List all running views."""
    from ..runtime_objects import view_managers
    
    result = {}
    for ds_id, vm in view_managers.items():
        result[ds_id] = list(vm.providers.keys())
    
    return {"running_views": result}
