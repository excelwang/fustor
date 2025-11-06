import logging
from fastapi import APIRouter, Depends, HTTPException, Body, status
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from fustor_agent.app import App
from fustor_agent.api.dependencies import get_app
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_agent.services import schema_cache

from .schemas import ValidationResponse, CleanupResponse, ConfigCreateResponse

logger = logging.getLogger("fustor_agent")
router = APIRouter()


class AddSourcePayload(BaseModel):
    config: SourceConfig
    discovered_fields: Optional[Dict[str, Any]] = None


@router.post("/{id}", status_code=status.HTTP_201_CREATED, response_model=ConfigCreateResponse[SourceConfig], summary="Add a new Source configuration")
async def add_source_config(id: str, payload: AddSourcePayload, app: App = Depends(get_app)):
    """
    [REFACTORED] Adds a new Source configuration and caches its discovered fields if provided.
    This endpoint is now generic and relies on the client to provide the schema.
    """
    try:
        # Step 1: Add the main configuration.
        new_config = await app.source_config_service.add_config(id, payload.config)

        # Step 2: [UNIFIED LOGIC] If discovered fields were provided by the client,
        # cache them, regardless of the driver type.
        if payload.discovered_fields:
            schema_cache.save_source_schema(
                source_id=id,
                schema_data=payload.discovered_fields
            )
            # After saving, mark the schema as valid so the source can be enabled.
            schema_cache.validate_schema(id)

        return {"id": id, "config": new_config}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Source configuration")
async def delete_source_config(id: str, app: App = Depends(get_app)):
    """Deletes a Source configuration.
    This will also stop any dependent sync tasks.""" 
    try:
        await app.source_config_service.delete_config(id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/_actions/cleanup", response_model=CleanupResponse, summary="Clean up obsolete Source configurations")
async def cleanup_obsolete_sources(app: App = Depends(get_app)):
    """Finds and deletes all disabled Source configurations that are not used by any sync task."""
    try:
        deleted_ids = await app.source_config_service.cleanup_obsolete_configs()
        count = len(deleted_ids) 
        return CleanupResponse(
            message=f"Cleanup successful. Deleted {count} obsolete source configurations.",
            deleted_count=count,
            deleted_ids=deleted_ids
        )
    except Exception as e:
        logger.error(f"Failed to cleanup obsolete source configs: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during source cleanup.")

@router.post("/{id}/_actions/disable", status_code=status.HTTP_200_OK, summary="Disable a Source configuration")
async def disable_source_config(id: str, app: App = Depends(get_app)):
    try:
        await app.source_config_service.disable(id)
        return {"message": f"Source config '{id}' disabled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) 

@router.post("/{id}/_actions/enable", status_code=status.HTTP_200_OK, summary="Enable a Source configuration")
async def enable_source_config(id: str, app: App = Depends(get_app)):
    try:
        await app.source_config_service.enable(id)
        return {"message": f"Source config '{id}' enabled successfully."}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) 

@router.post("/{id}/_actions/discover_and_cache_fields", summary="Discover and cache fields for a Source")
async def discover_and_cache_source_fields(id: str, admin_user: str = Body(..., embed=True), admin_password: str = Body(..., embed=True), app: App = Depends(get_app)):
    """
    Connects to a data source using provided admin credentials, discovers
    its available fields, and saves them to a local cache file.
    """ 
    try:
        source_config = app.source_config_service.get_config(id)
        if not source_config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Source config '{id}' not found.")

        # Construct PasswdCredential from admin_user and admin_password
        admin_creds = PasswdCredential(user=admin_user, passwd=admin_password)

        await app.source_config_service.discover_and_cache_fields(
            id, admin_user, admin_password
        )
        # After successful discovery and caching, update the schema_cached status
        await app.source_config_service.update_schema_cached_status(id, True)
        return {"message": "Fields discovered and cached successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to discover and cache fields for source '{id}': {e}") 

 

@router.get("/schema/{driver_type}", summary="Get the configuration schema for a specific Source driver")
async def get_source_driver_schema(driver_type: str, app: App = Depends(get_app)):
    try:
        schema = await app.source_driver_service.get_schema_by_type(driver_type)
        return schema
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) 
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve Source driver schema: {e}")