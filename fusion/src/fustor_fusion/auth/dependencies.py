from fastapi import Header, HTTPException, status
from typing import Optional
import logging

from ..config.unified import fusion_config

logger = logging.getLogger(__name__)


async def get_view_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipe/view ID.
    
    Uses receivers-config.yaml for API key management.
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    try:
        # Validate against all configured receivers
        # Unified config doesn't have a single validate_api_key, we need to check all receivers
        receivers = fusion_config.get_all_receivers()
        
        for r_id, r_config in receivers.items():
            for api_key_config in r_config.api_keys:
                if api_key_config.key == x_api_key:
                    # Found matching key
                    view_id = api_key_config.pipe_id
                    logger.debug(f"Resolved view_id: {view_id}")
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

