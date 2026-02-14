from fastapi import Header, HTTPException, status, Depends
from typing import Optional
import logging

from ..config.unified import fusion_config

logger = logging.getLogger(__name__)


async def _get_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key header is missing"
        )
    return x_api_key

async def get_view_id_from_auth(x_api_key: str = Depends(_get_api_key)) -> str:
    """Returns the View ID authorized by this key."""
    # 1. Check Dedicated View Keys
    views = fusion_config.get_all_views()
    for v_id, v_config in views.items():
        if x_api_key in v_config.api_keys:
            return str(v_id)

    # 2. Check Receiver Keys (Receiver keys can query any view served by their pipe)
    # The view_id is typically in the URL path. The pipe_id returned here
    # is used to verify that the pipe serves the requested view.
    receivers = fusion_config.get_all_receivers()
    for r_id, r_config in receivers.items():
        for ak in r_config.api_keys:
            if ak.key == x_api_key:
                # Return the pipe_id - the API layer will verify that this pipe serves the requested view
                return ak.pipe_id
    
    raise HTTPException(status_code=401, detail="Invalid or inactive X-API-Key")

async def get_pipe_id_from_auth(x_api_key: str = Depends(_get_api_key)) -> str:
    """Returns the Pipe ID authorized by this key. Sessions must use this."""
    receivers = fusion_config.get_all_receivers()
    for r_id, r_config in receivers.items():
        for ak in r_config.api_keys:
            if ak.key == x_api_key:
                logger.debug(f"Authorized Pipe: {ak.pipe_id}")
                return ak.pipe_id
    
    raise HTTPException(
        status_code=403, 
        detail="API Key is not authorized for Pipe ingestion (Sessions)."
    )

# Alias for compatibility while migrating
get_view_id_from_api_key = get_view_id_from_auth


# All APIs must use get_view_id_from_api_key.

