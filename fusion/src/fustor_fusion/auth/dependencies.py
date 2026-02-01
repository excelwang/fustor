from fastapi import Header, HTTPException, status
from typing import Optional
import logging

from ..config import receivers_config

logger = logging.getLogger(__name__)


async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipeline/datastore ID.
    
    Uses receivers-config.yaml for API key management.
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    try:
        pipeline_id = receivers_config.validate_api_key(x_api_key)
        if pipeline_id is not None:
            logger.debug(f"Resolved pipeline_id: {pipeline_id}")
            return pipeline_id
    except Exception as e:
        logger.error(f"Error validating API key: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )


async def get_pipeline_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipeline_id.
    
    Uses receivers-config.yaml.
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    try:
        pipeline_id = receivers_config.validate_api_key(x_api_key)
        if pipeline_id is not None:
            logger.debug(f"Resolved pipeline_id: {pipeline_id}")
            return pipeline_id
    except Exception as e:
        logger.error(f"Error validating API key: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )
