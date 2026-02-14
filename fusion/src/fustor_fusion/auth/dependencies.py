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
    # Note: This returns the first view associated with the pipe for now, or we might
    # need a more complex return. But for queries, the view_id is usually in the URL.
    # The 'authorized_view_id' returned here is used for ownership verification.
    receivers = fusion_config.get_all_receivers()
    for r_id, r_config in receivers.items():
        for ak in r_config.api_keys:
            if ak.key == x_api_key:
                # For a pipe key, the 'authorized identity' is the pipe_id.
                # However, many APIs use this as a view_id for backward compatibility.
                # We return the pipe_id but it must be matched against the view's pipe set.
                return ak.pipe_id
    
    raise HTTPException(status_code=401, detail="Invalid API Key")

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

