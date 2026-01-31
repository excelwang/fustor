from fastapi import Header, HTTPException, status, Depends
from typing import Optional
import logging

from .cache import api_key_cache

logger = logging.getLogger(__name__)


async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> int:
    """
    Retrieves the datastore_id from:
    1. First, check the YAML config (datastores-config.yaml)
    2. Fallback to the in-memory API key cache (for backward compatibility)
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    # Try YAML config first
    try:
        from ..config import datastores_config
        datastore_id = datastores_config.validate_api_key(x_api_key)
        if datastore_id is not None:
            logger.debug(f"Resolved datastore_id from YAML config: {datastore_id}")
            return datastore_id
    except Exception as e:
        logger.warning(f"Failed to check YAML config: {e}")
    
    # Fallback to legacy cache
    datastore_id = api_key_cache.get_datastore_id(x_api_key)
    if datastore_id is not None:
        logger.debug(f"Resolved datastore_id from legacy cache: {datastore_id}")
        return datastore_id
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )

