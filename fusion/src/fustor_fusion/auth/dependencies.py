from fastapi import Header, HTTPException, status, Depends
from typing import Optional
import logging
import warnings

from ..config import datastores_config, receivers_config

logger = logging.getLogger(__name__)

# Flag to track if we've shown the deprecation warning
_datastores_config_warning_shown = False


async def get_datastore_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key against configuration.
    
    Priority:
    1. receivers-config.yaml (recommended)
    2. datastores-config.yaml (deprecated, for backward compatibility)
    """
    global _datastores_config_warning_shown
    
    logger.debug(f"Received X-API-Key: {x_api_key[:8]}...")
    
    # Try receivers-config.yaml first (new recommended way)
    try:
        pipeline_id = receivers_config.validate_api_key(x_api_key)
        if pipeline_id is not None:
            logger.debug(f"Resolved pipeline_id from receivers-config: {pipeline_id}")
            return pipeline_id
    except Exception as e:
        logger.debug(f"Error checking receivers-config: {e}")
    
    # Fallback to datastores-config.yaml (deprecated)
    try:
        datastore_id = datastores_config.validate_api_key(x_api_key)
        if datastore_id is not None:
            # Show deprecation warning once
            if not _datastores_config_warning_shown:
                warnings.warn(
                    "datastores-config.yaml is deprecated. "
                    "Please migrate to receivers-config.yaml for API key management.",
                    DeprecationWarning,
                    stacklevel=2
                )
                logger.warning(
                    "API key found in datastores-config.yaml. "
                    "This configuration format is deprecated. "
                    "Please migrate to receivers-config.yaml"
                )
                _datastores_config_warning_shown = True
            
            logger.debug(f"Resolved datastore_id from datastores-config (deprecated): {datastore_id}")
            return datastore_id
    except Exception as e:
        logger.error(f"Error validating API key from YAML: {e}", exc_info=True)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or inactive X-API-Key"
    )


async def get_pipeline_id_from_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    """
    Validates API key and returns pipeline_id.
    
    This is the recommended dependency for new Pipeline-based API endpoints.
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
