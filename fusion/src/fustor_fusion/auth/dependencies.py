from fastapi import Header, HTTPException, status, Depends
from typing import Optional

from .cache import api_key_cache

async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> int:
    """
    Retrieves the datastore_id from the in-memory API key cache.
    """
    datastore_id = api_key_cache.get_datastore_id(x_api_key)
    if datastore_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive X-API-Key"
        )
    return datastore_id
