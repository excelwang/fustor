import asyncio
import logging
from fustor_registry_client.client import RegistryClient
from ..auth.cache import api_key_cache
from ..auth.datastore_cache import datastore_config_cache
from ..config import fusion_config
from ..runtime_objects import task_manager # CORRECTED IMPORT

logger = logging.getLogger(__name__)

async def sync_caches_job() -> None:
    """
    Performs a single synchronization of caches with the register service.
    Raises RuntimeError on failure.
    """
    logger.info("Attempting to sync caches...")

    client_config = {
        "base_url": fusion_config.FUSTOR_FUSION_REGISTRY_URL,
        "token": fusion_config.FUSTOR_REGISTRY_CLIENT_TOKEN
    }
    async with RegistryClient(**client_config) as fustor_registry_client:
        # Sync API keys
        api_keys_data = await fustor_registry_client.get_client_api_keys()
        if api_keys_data is not None:
            api_keys_dict = [key.model_dump() for key in api_keys_data]
            api_key_cache.set_cache(api_keys_dict)
            logger.info(f"API Key cache synced. Total keys: {len(api_key_cache._cache)}")
        else:
            logger.error("Failed to fetch API keys from register service. Aborting startup.")
            raise RuntimeError("Failed to fetch API keys from register service.")

        # Sync datastore configs
        datastore_configs_data = await fustor_registry_client.get_client_datastores_config()
        if datastore_configs_data is not None:
            datastore_config_cache.set_cache(datastore_configs_data)
            logger.info(f"Datastore config cache synced. Total configs: {len(datastore_config_cache._cache)}")

            # --- NEW: Sync processing tasks ---
            if task_manager:
                # Pass the full datastore_configs_data to sync_tasks
                await task_manager.sync_tasks(datastore_configs_data)
            # ------------------------------------

        else:
            logger.error("Failed to fetch datastore configs from register service. Aborting startup.")
            raise RuntimeError("Failed to fetch datastore configs from register service.")