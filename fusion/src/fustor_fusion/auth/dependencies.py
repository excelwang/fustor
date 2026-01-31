from fastapi import Header, HTTPException, status, Depends
from typing import Optional
import logging

from ..config import datastores_config

logger = logging.getLogger(__name__)


async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key against the YAML configuration (datastores-config.yaml).
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    # Use YAML config
    try:
        datastore_id = datastores_config.validate_api_key(x_api_key)
        if datastore_id is not None:
            logger.debug(f"Resolved datastore_id from YAML config: {datastore_id}")
            return datastore_id
    except Exception as e:
        logger.error(f"Error validating API key from YAML: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )
