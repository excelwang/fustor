# src/fustor_agent/services/configs/sender.py
"""
Sender Configuration Service.

This is the new naming for what was previously called "Pusher Config Service".
The term "sender" aligns with the V2 architecture terminology.
"""
import logging
from typing import Dict, Optional, List

from fustor_core.models.config import AppConfig, SenderConfig
from fustor_agent.services.instances.sync import SyncInstanceService
from fustor_agent.services.common import config_lock
from .base import BaseConfigService
from fustor_agent_sdk.interfaces import SenderConfigServiceInterface

logger = logging.getLogger("fustor_agent")


class SenderConfigService(BaseConfigService[SenderConfig], SenderConfigServiceInterface):
    """
    Manages the lifecycle of SenderConfig objects.
    
    Senders are responsible for transmitting events from Agent to Fusion.
    This replaces the deprecated "PusherConfigService" terminology.
    """
    
    def __init__(self, app_config: AppConfig):
        # Still use 'sender' internally for config file compatibility
        super().__init__(app_config, None, 'sender')
        self.sync_instance_service: Optional[SyncInstanceService] = None

    def set_dependencies(self, sync_instance_service: SyncInstanceService):
        """Injects the SyncInstanceService for dependency management."""
        self.sync_instance_service = sync_instance_service

    async def cleanup_obsolete_configs(self) -> List[str]:
        """
        Finds and deletes all Sender configurations that are both disabled and
        not used by any sync tasks.

        Returns:
            A list of the configuration IDs that were deleted.
        """
        all_sync_configs = self.app_config.get_syncs().values()
        in_use_sender_ids = {sync.sender for sync in all_sync_configs}

        all_sender_configs = self.list_configs()
        obsolete_ids = [
            sender_id for sender_id, config in all_sender_configs.items()
            if config.disabled and sender_id not in in_use_sender_ids
        ]

        if not obsolete_ids:
            logger.info("No obsolete sender configurations to clean up.")
            return []

        logger.info(f"Found {len(obsolete_ids)} obsolete sender configurations to clean up: {obsolete_ids}")

        deleted_ids = []
        async with config_lock:
            sender_dict = self.app_config.get_senders()
            for an_id in obsolete_ids:
                if an_id in sender_dict:
                    sender_dict.pop(an_id)
                    deleted_ids.append(an_id)
            
            if deleted_ids:
                # Config persistence handled by YAML files now
                pass
        
        
        logger.info(f"Successfully cleaned up {len(deleted_ids)} sender configurations.")
        return deleted_ids

