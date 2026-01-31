# agent/src/fustor_agent/api/management_api.py
"""
Management API for dynamic sync start/stop operations.
"""
import logging
from fastapi import APIRouter, HTTPException, status, Request

from ..config.syncs import syncs_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/management", tags=["Management"])


@router.post("/syncs/{sync_id}/start")
async def start_sync(sync_id: str, request: Request):
    """
    Start a sync task by ID.
    
    1. Load sync config from YAML
    2. Validate source and pusher exist
    3. Start the sync instance
    """
    app = request.app.state.app
    
    # Load sync config
    config = syncs_config.get(sync_id)
    if not config:
        # Try reloading in case file was just added
        syncs_config.reload()
        config = syncs_config.get(sync_id)
    
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync config '{sync_id}' not found in syncs-config/"
        )
    
    if config.disabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Sync '{sync_id}' is disabled in config"
        )
    
    # Check if already running
    sync_instance_service = app.sync_instance_service
    running = sync_instance_service.get_instance(sync_id)
    if running and running.is_running:
        return {"status": "already_running", "sync_id": sync_id}
    
    # Validate source exists
    source = app.source_config_service.get_config(config.source)
    if not source:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Source '{config.source}' not found"
        )
    
    # Validate pusher exists
    pusher = app.pusher_config_service.get_config(config.pusher)
    if not pusher:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pusher '{config.pusher}' not found"
        )
    
    # Start the sync
    try:
        await sync_instance_service.start(sync_id)
        logger.info(f"Started sync: {sync_id}")
    except Exception as e:
        logger.error(f"Failed to start sync {sync_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sync: {e}"
        )
    
    return {"status": "started", "sync_id": sync_id}


@router.post("/syncs/{sync_id}/stop")
async def stop_sync(sync_id: str, request: Request):
    """
    Stop a sync task by ID.
    
    1. Stop the sync instance
    2. Return should_shutdown if no syncs left
    """
    app = request.app.state.app
    sync_instance_service = app.sync_instance_service
    
    # Check if running
    running = sync_instance_service.get_instance(sync_id)
    if not running:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync '{sync_id}' is not running"
        )
    
    # Stop the sync
    try:
        await sync_instance_service.stop(sync_id)
        logger.info(f"Stopped sync: {sync_id}")
    except Exception as e:
        logger.error(f"Error stopping sync {sync_id}: {e}", exc_info=True)
    
    # Check if any syncs left
    all_instances = sync_instance_service.list_all()
    running_count = sum(1 for inst in all_instances.values() if inst.is_running)
    should_shutdown = running_count == 0
    
    return {
        "status": "stopped",
        "sync_id": sync_id,
        "should_shutdown": should_shutdown
    }


@router.get("/syncs")
async def list_syncs(request: Request):
    """List all running sync tasks."""
    app = request.app.state.app
    sync_instance_service = app.sync_instance_service
    
    instances = sync_instance_service.list_all()
    result = {
        sync_id: {
            "is_running": inst.is_running,
            "status": inst.status if hasattr(inst, 'status') else 'unknown'
        }
        for sync_id, inst in instances.items()
    }
    
    return {"running_syncs": result}
