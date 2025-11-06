import logging
from fastapi import APIRouter, Depends, Body, HTTPException, status
from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel

from fustor_agent.app import App
from .dependencies import get_app
from .schemas import MessageResponse, ValidationResponse, TestSourceConnectionRequest, TestPusherConnectionRequest, AdminCredentials
from fustor_core.models.config import Credential, PasswdCredential, SourceConfig
from fustor_agent.services import schema_cache
from fustor_core.exceptions import DriverError, ConfigError

logger = logging.getLogger("fustor_agent")
router = APIRouter()

class WizardUserActionPayload(BaseModel):
    uri: str
    admin_creds: Credential
    agent_creds: Credential

@router.get("/", summary="List all available drivers")
async def list_available_drivers(app: App = Depends(get_app)) -> Dict[str, List[str]]:
    source_drivers = app.source_driver_service.list_available_drivers()
    pusher_drivers = app.pusher_driver_service.list_available_drivers()
    return {"sources": source_drivers, "pushers": pusher_drivers}

@router.get("/sources/{driver_type}/wizard", summary="Get the wizard definition for a specific Source driver")
async def get_source_driver_wizard(driver_type: str, app: App = Depends(get_app)) -> Dict[str, Any]:
    try:
        wizard_definition = await app.source_driver_service.get_wizard_definition_by_type(driver_type)
        return wizard_definition
    except (DriverError, ConfigError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to retrieve wizard definition for driver '{driver_type}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching the wizard definition.")

@router.get("/pushers/{driver_type}/wizard", summary="Get the wizard definition for a specific Pusher driver")
async def get_pusher_driver_wizard(driver_type: str, app: App = Depends(get_app)) -> Dict[str, Any]:
    try:
        wizard_definition = await app.pusher_driver_service.get_wizard_definition_by_type(driver_type)
        return wizard_definition
    except (DriverError, ConfigError) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to retrieve wizard definition for pusher driver '{driver_type}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred while fetching the wizard definition.")

@router.get("/sources/{id}/_actions/get_available_fields", summary="Get cached available fields for a Source")
async def get_source_available_fields(id: str, app: App = Depends(get_app)):
    fields = schema_cache.load_source_schema(id)
    if fields is None:
        return {"properties": {}}
    return fields

@router.get("/pushers/{id}/_actions/get_needed_fields", summary="Get all needed fields from a configured Pusher")
async def get_pusher_needed_fields(id: str, app: App = Depends(get_app)):
    try:
        pusher_config = app.pusher_config_service.get_config(id)
        if not pusher_config:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pusher config '{id}' not found.")
        fields = await app.pusher_driver_service.get_needed_fields(
            pusher_config.driver, pusher_config.endpoint
        )
        return fields
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve needed fields from pusher '{id}': {e}")

@router.post("/sources/{driver_type}/_actions/test_connection", response_model=ValidationResponse, summary="Test connection for a source driver")
async def test_source_connection(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        ok, msg = await app.source_driver_service.test_connection(driver_type=driver_type, **payload)
        if ok:
            return ValidationResponse(success=True, message=msg)
        else:
            raise ValueError(msg)
    except Exception as e:
        logger.error(f"Failed to test source connection for {driver_type}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/sources/{driver_type}/_actions/check_params", response_model=ValidationResponse, summary="Check runtime parameters for a given Source driver")
async def check_source_params(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        ok, msg = await app.source_driver_service.check_params(driver_type, **payload)
        return ValidationResponse(success=ok, message=msg)
    except Exception as e:
        logger.error(f"Error checking parameters for driver '{driver_type}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.post("/sources/{driver_type}/_actions/discover_fields_no_cache", summary="Discover fields for a driver without caching")
async def discover_source_fields_no_cache(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        fields = await app.source_driver_service.get_available_fields(driver_type, **payload)
        field_count = len(fields.get("properties", {}))
        return {
            "success": True,
            "message": f"成功发现 {field_count} 个可用字段。",
            "fields": fields
        }
    except Exception as e:
        logger.error(f"Error during field discovery for driver '{driver_type}': {e}", exc_info=True)
        if isinstance(e, DriverError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.post("/sources/{driver_type}/_actions/create_agent_user", response_model=ValidationResponse, summary="Create an agent user without a saved config")
async def create_source_user(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        ok, msg = await app.source_driver_service.create_agent_user(driver_type, **payload)
        return ValidationResponse(success=ok, message=msg)
    except Exception as e:
        logger.error(f"Error during create_agent_user for driver '{driver_type}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

@router.post("/sources/{driver_type}/_actions/check_privileges", response_model=ValidationResponse, summary="Check agent user privileges without a saved config")
async def check_source_privileges(driver_type: str, payload: Dict[str, Any], app: App = Depends(get_app)):
    ok, msg = await app.source_driver_service.check_privileges(driver_type, **payload)
    return ValidationResponse(success=ok, message=msg)

@router.post("/pushers/{driver_type}/_actions/test_connection", response_model=ValidationResponse, summary="Test connection for a pusher driver")
async def test_pusher_connection(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        endpoint = payload.get("endpoint")
        if not endpoint:
            raise HTTPException(status_code=400, detail="Missing 'endpoint' in request body.")
        
        # Pass the whole payload to the service layer
        ok, msg = await app.pusher_driver_service.test_connection(
            driver_type=driver_type,
            **payload
        )
        if ok:
            return ValidationResponse(success=True, message=msg or "Pusher connection successful.")
        else:
            # Create a ValidationResponse for failure case for consistency
            return ValidationResponse(success=False, message=msg or "Pusher connection failed.")
    except Exception as e:
        logger.error(f"Failed to test pusher connection for {driver_type}: {e}", exc_info=True)
        # Return a standard validation response on exception
        return ValidationResponse(success=False, message=f"An exception occurred: {e}")

@router.post("/pushers/{driver_type}/_actions/check_privileges", response_model=ValidationResponse, summary="Check privileges for a given Pusher driver")
async def check_pusher_privileges(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    try:
        ok, msg = await app.pusher_driver_service.check_privileges(
            driver_type,
            **payload
        )
        return ValidationResponse(success=ok, message=msg)
    except Exception as e:
        logger.error(f"Error checking privileges for driver '{driver_type}': {e}", exc_info=True)
        return ValidationResponse(success=False, message=f"An exception occurred: {e}")

# --- START: NEW ENDPOINT for Pusher field discovery in wizard ---
@router.post("/pushers/{driver_type}/_actions/discover_fields", summary="Discover needed fields for a Pusher driver configuration")
async def discover_pusher_fields(driver_type: str, payload: Dict[str, Any] = Body(...), app: App = Depends(get_app)):
    """
    Connects to the pusher's endpoint (e.g., openapi.json) and discovers the
    fields it requires. This is used as a validation step in the UI wizard.
    """
    try:
        endpoint = payload.get("endpoint")
        if not endpoint:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must include 'endpoint'.")

        fields = await app.pusher_driver_service.get_needed_fields(driver_type, endpoint)
        field_count = len(fields.get("properties", {}))
        return {
            "success": True,
            "message": f"成功发现 {field_count} 个目标字段。",
            "fields": fields
        }
    except Exception as e:
        logger.error(f"Error discovering pusher fields for driver '{driver_type}': {e}", exc_info=True)
        # Return a standard validation response on error
        return ValidationResponse(success=False, message=f"发现字段时出错: {e}")
# --- END: NEW ENDPOINT ---