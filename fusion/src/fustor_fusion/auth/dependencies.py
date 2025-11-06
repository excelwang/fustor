from fastapi import Header, HTTPException, status, Depends
from typing import Optional
import logging

from .cache import api_key_cache

logger = logging.getLogger(__name__)

async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> int:
    """
    Retrieves the datastore_id from the in-memory API key cache.
    """
    logger.debug(f"Received X-API-Key: {x_api_key}")
    datastore_id = api_key_cache.get_datastore_id(x_api_key)
    logger.debug(f"Resolved datastore_id for key '{x_api_key[:5]}...': {datastore_id}")
    if datastore_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive X-API-Key"
        )
    return datastore_id
