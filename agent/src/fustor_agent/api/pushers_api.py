import logging
from fastapi import APIRouter, Depends, HTTPException, Body, status
from typing import Dict, Union
from pydantic import BaseModel

from fustor_agent.app import App
from fustor_agent.api.dependencies import get_app
from fustor_core.models.config import PusherConfig, Credential
from .schemas import ValidationResponse, CleanupResponse, ConfigCreateResponse

logger = logging.getLogger("fustor_agent")
router = APIRouter()


class AddPusherPayload(BaseModel):
    config: PusherConfig


# === Pusher Configuration Management Endpoints ===
# These interact with PusherConfigService for CRUD operations.
@router.post("/{id}", status_code=status.HTTP_201_CREATED, response_model=ConfigCreateResponse[PusherConfig], summary="Add a new Pusher configuration")
async def add_pusher_config(id: str, payload: AddPusherPayload, app: App = Depends(get_app)):
    """Adds a new Pusher configuration to the config file."""
    try:
        new_config = await app.pusher_config_service.add_config(id, payload.config)
        return {"id": id, "config": new_config}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Pusher configuration")
async def delete_pusher_config(id: str, app: App = Depends(get_app)):
    """Deletes a Pusher configuration. This will also stop any dependent sync tasks."""
    try:
        await app.pusher_config_service.delete_config(id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/_actions/cleanup", response_model=CleanupResponse, summary="Clean up obsolete Pusher configurations")
async def cleanup_obsolete_pushers(app: App = Depends(get_app)):
    """Finds and deletes all disabled Pusher configurations that are not used by any sync task."""
    try:
        deleted_ids = await app.pusher_config_service.cleanup_obsolete_configs()
        count = len(deleted_ids)
        return CleanupResponse(
            message=f"Cleanup successful. Deleted {count} obsolete pusher configurations.",
            deleted_count=count,
            deleted_ids=deleted_ids
        )
    except Exception as e:
        logger.error(f"Failed to cleanup obsolete pusher configs: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during pusher cleanup.")

@router.post("/{id}/_actions/disable", status_code=status.HTTP_200_OK, summary="Disable a Pusher configuration")
async def disable_pusher_config(id: str, app: App = Depends(get_app)):
    try:
        await app.pusher_config_service.disable(id)
        return {"message": f"Pusher config '{id}' disabled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/{id}/_actions/enable", status_code=status.HTTP_200_OK, summary="Enable a Pusher configuration")
async def enable_pusher_config(id: str, app: App = Depends(get_app)):
    try:
        await app.pusher_config_service.enable(id)
        return {"message": f"Pusher config '{id}' enabled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

# === Endpoints Orchestrating Both Services ===
# These use a saved config ID to perform driver actions.
@router.post("/{id}/_actions/test_connection", response_model=ValidationResponse, summary="Test connection for a saved Pusher config")
async def pusher_test_connection(id: str, app: App = Depends(get_app)):
    """Tests the connection for a specific, saved Pusher configuration."""
    try:
        config = app.pusher_config_service.get_config(id)
        if not config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pusher config '{id}' not found.")
        
        # --- REFACTORED: Use keyword arguments to call the service method ---
        ok, msg = await app.pusher_driver_service.test_connection(
            driver_type=config.driver, endpoint=config.endpoint
        )
        # --- END REFACTOR ---
        return ValidationResponse(success=ok, message=msg)
    except Exception as e:
        logger.error(f"Error testing connection for pusher '{id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.post("/{id}/_actions/check_privileges", response_model=ValidationResponse, summary="Check privileges for a saved Pusher config")
async def pusher_check_privileges(id: str, app: App = Depends(get_app)):
    """Checks the credentials for a specific, saved Pusher configuration."""
    try:
        config = app.pusher_config_service.get_config(id)
        if not config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pusher config '{id}' not found.")
        
        # --- REFACTORED: Use keyword arguments to call the service method ---
        ok, msg = await app.pusher_driver_service.check_privileges(
            driver_type=config.driver, endpoint=config.endpoint, credential=config.credential
        )
        # --- END REFACTOR ---
        return ValidationResponse(success=ok, message=msg)
    except Exception as e:
        logger.error(f"Error checking privileges for pusher '{id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")