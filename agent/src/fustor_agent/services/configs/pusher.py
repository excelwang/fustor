# src/fustor_agent/services/configs/pusher.py

import logging
from typing import Dict, Optional, List

# Removed legacy import
from fustor_core.models.config import AppConfig, PusherConfig
from fustor_agent.services.instances.sync import SyncInstanceService
from fustor_agent.services.common import config_lock
from .base import BaseConfigService
from fustor_agent_sdk.interfaces import PusherConfigServiceInterface # Import the interface

logger = logging.getLogger("fustor_agent")

class PusherConfigService(BaseConfigService[PusherConfig], PusherConfigServiceInterface): # Inherit from the interface
    """
    Manages the lifecycle of PusherConfig objects.
    This service is responsible for CRUD operations on pusher configurations
    and inherits common logic from BaseConfigService.
    """
    def __init__(self, app_config: AppConfig):
        super().__init__(app_config, None, 'pusher')
        self.sync_instance_service: Optional[SyncInstanceService] = None

    def set_dependencies(self, sync_instance_service: SyncInstanceService):
        """Injects the SyncInstanceService for dependency management."""
        self.sync_instance_service = sync_instance_service

    # All common methods are inherited from BaseConfigService.

    async def cleanup_obsolete_configs(self) -> List[str]:
        """
        Finds and deletes all Pusher configurations that are both disabled and
        not used by any sync tasks.

        Returns:
            A list of the configuration IDs that were deleted.
        """
        all_sync_configs = self.app_config.get_syncs().values()
        in_use_pusher_ids = {sync.pusher for sync in all_sync_configs}

        all_pusher_configs = self.list_configs()
        obsolete_ids = [
            pusher_id for pusher_id, config in all_pusher_configs.items()
            if config.disabled and pusher_id not in in_use_pusher_ids
        ]

        if not obsolete_ids:
            logger.info("No obsolete pusher configurations to clean up.")
            return []

        logger.info(f"Found {len(obsolete_ids)} obsolete pusher configurations to clean up: {obsolete_ids}")

        deleted_ids = []
        async with config_lock:
            pusher_dict = self.app_config.get_pushers()
            for an_id in obsolete_ids:
                if an_id in pusher_dict:
                    pusher_dict.pop(an_id)
                    deleted_ids.append(an_id)
            
            if deleted_ids:
                # Removed legacy persistence call
                pass
        
        logger.info(f"Successfully cleaned up {len(deleted_ids)} pusher configurations.")
        return deleted_ids