from fastapi import Header, HTTPException, status
from typing import Optional
import logging

from ..config.unified import fusion_config

logger = logging.getLogger(__name__)


async def get_view_id_from_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipe/view ID.
    
    Priority:
    1. Dedicated View Query Keys (views.[id].api_keys)
    2. Receiver/Agent Push Keys (kept for backward compatibility, but limited to associated views)
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key header is missing"
        )

    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    try:
        # 1. Check Dedicated View Query Keys
        views = fusion_config.get_all_views()
        for v_id, v_config in views.items():
            if x_api_key in v_config.api_keys:
                logger.debug(f"Authorized via dedicated view key for: {v_id}")
                return str(v_id)

        # 2. Fallback: Validate against all configured receivers
        receivers = fusion_config.get_all_receivers()
        for r_id, r_config in receivers.items():
            for api_key_config in r_config.api_keys:
                if api_key_config.key == x_api_key:
                    # In Receiver/API Key config, pipe_id acts as the scoping identifier (mapped to view_id)
                    view_id = api_key_config.pipe_id
                    logger.debug(f"Authorized via receiver key for: {view_id}")
                    return str(view_id)
        
        # If loop finishes without return
        logger.warning(f"API Key validation failed for key: {x_api_key[:8]}...")
    except Exception as e:
        logger.error(f"Error validating API key: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )


# All APIs must use get_view_id_from_api_key.

