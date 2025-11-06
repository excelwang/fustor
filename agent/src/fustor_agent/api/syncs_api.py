import logging
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any
from pydantic import BaseModel

from fustor_agent.app import App
from fustor_agent.api.dependencies import get_app
from fustor_core.models.config import SyncConfig
from fustor_agent.services import schema_cache
from .schemas import ConfigCreateResponse

logger = logging.getLogger("fustor_agent")
router = APIRouter()

# ==============================================================================
# NEW: Pydantic model for the request body of the field loading action.
# ==============================================================================
class LoadFieldsRequest(BaseModel):
    source_id: str
    pusher_id: str
# ==============================================================================


# ==============================================================================
# NEW: API endpoint to fetch the dynamic wizard definition for creating a sync task.
# ==============================================================================
@router.get("/wizard", summary="Get the wizard definition for creating a Sync Task")
async def get_sync_wizard_definition(app: App = Depends(get_app)) -> Dict[str, Any]:
    """
    Retrieves the dynamic wizard steps and schema definition for creating a new
    Sync Task. The frontend uses this response to build the wizard UI.
    """
    try:
        definition = app.sync_config_service.get_wizard_definition()
        return definition
    except Exception as e:
        logger.error(f"Failed to retrieve sync wizard definition: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching the sync wizard definition.")
# ==============================================================================


# ==============================================================================
# NEW: API endpoint for the "Field Mapping" step to load source and pusher fields.
# ==============================================================================
@router.post("/_actions/load_fields_for_mapping", summary="Load available and needed fields for mapping")
async def load_fields_for_mapping(payload: LoadFieldsRequest, app: App = Depends(get_app)) -> Dict[str, Any]:
    """
    For a given source_id and pusher_id, this endpoint returns the source's
    cached available fields and the pusher's dynamically fetched needed fields.
    """
    try:
        # 1. Get pusher config and fetch its needed fields
        pusher_config = app.pusher_config_service.get_config(payload.pusher_id)
        if not pusher_config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pusher config '{payload.pusher_id}' not found.")
        
        pusher_schema = await app.pusher_driver_service.get_needed_fields(
            pusher_config.driver, pusher_config.endpoint
        )

        # 2. Load the source's cached schema
        source_schema = schema_cache.load_source_schema(payload.source_id)
        if source_schema is None:
            # This is a critical error for the user to fix.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"未能为数据源 '{payload.source_id}' 加载可用字段。请前往数据源配置页面，编辑此数据源，确保“校验并发现”步骤已成功完成以生成必要的字段缓存。"
            )

        return {
            "source_schema": source_schema,
            "pusher_schema": pusher_schema
        }
    except HTTPException as e:
        # Re-raise HTTP exceptions directly
        raise e
    except Exception as e:
        logger.error(f"Failed to load fields for mapping between source '{payload.source_id}' and pusher '{payload.pusher_id}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred while loading fields for mapping: {str(e)}")
# ==============================================================================


@router.post("/{id}", status_code=status.HTTP_201_CREATED, response_model=ConfigCreateResponse[SyncConfig], summary="Add a new Sync configuration")
async def add_sync_config(id: str, config: SyncConfig, app: App = Depends(get_app)):
    """Adds a new Sync configuration to the config file."""
    try:
        # The SyncConfig model does not contain 'id' and is sent directly.
        new_config = await app.sync_config_service.add_config(id, config)
        return {"id": id, "config": new_config}
    except ValueError as e:
        # This can happen if the source_id or pusher_id in the config don't exist
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Sync configuration")
async def delete_sync_config(id: str, app: App = Depends(get_app)):
    """Deletes a Sync configuration."""
    try:
        await app.sync_config_service.delete_config(id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/{id}/_actions/disable", status_code=status.HTTP_204_NO_CONTENT, summary="Disable a Sync configuration")
async def disable_sync_config(id: str, app: App = Depends(get_app)):
    """Disables a Sync configuration."""
    try:
        await app.sync_config_service.disable(id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/{id}/_actions/enable", status_code=status.HTTP_200_OK, summary="Enable a Sync configuration")
async def enable_sync_config(id: str, app: App = Depends(get_app)):
    """Enables a Sync configuration."""
    try:
        await app.sync_config_service.enable(id)
        return {"message": f"Sync config '{id}' enabled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to enable sync instance {id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")