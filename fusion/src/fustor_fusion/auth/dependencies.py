from fastapi import Header, HTTPException, status
from typing import Optional
import logging

from ..config import receivers_config

logger = logging.getLogger(__name__)


async def get_view_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipeline/view ID.
    
    Uses receivers-config.yaml for API key management.
    """
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    try:
        view_id = receivers_config.validate_api_key(x_api_key)
        if view_id is not None:
            logger.debug(f"Resolved view_id: {view_id}")
            return str(view_id)
    except Exception as e:
        logger.error(f"Error validating API key: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )


# All APIs must use get_view_id_from_api_key.

