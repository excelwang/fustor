# src/fustor_agent/services/configs/source.py

import logging
from typing import Dict, Optional, List, Any

from fustor_agent import update_app_config_file
from fustor_core.models.config import AppConfig, SourceConfig
from fustor_agent.services.instances.sync import SyncInstanceService
from .base import BaseConfigService
from fustor_agent.services.common import config_lock
from fustor_agent.services import schema_cache # Import schema_cache at the top level
from fustor_agent_sdk.interfaces import SourceConfigServiceInterface # Import the interface

logger = logging.getLogger("fustor_agent")

class SourceConfigService(BaseConfigService[SourceConfig], SourceConfigServiceInterface): # Inherit from the interface
    """
    Manages the lifecycle of SourceConfig objects.
    This service is responsible for CRUD operations on source configurations
    and inherits common logic from BaseConfigService.
    """
    def __init__(self, app_config: AppConfig):
        super().__init__(app_config, None, 'source')
        self.sync_instance_service: Optional[SyncInstanceService] = None

    def set_dependencies(self, sync_instance_service: SyncInstanceService):
        """
        Injects the SyncInstanceService for dependency management.
        This is to resolve circular dependencies between services.
        """
        self.sync_instance_service = sync_instance_service

    async def add_config(self, id: str, config: SourceConfig) -> SourceConfig:
        """
        Adds a new source configuration.
        Note: The schema cache is not created here. It must be generated
        by calling 'discover_and_cache_fields'.
        """
        async with config_lock:
            self._add_config_to_app(id, config)
            update_app_config_file()
        logger.info(f"Source '{id}' configuration added.")
        return config

    async def update_config(self, id: str, updates: Dict[str, Any]) -> SourceConfig:
        """
        Updates a source configuration. If enabling, checks for a valid schema cache.
        """
        # If the user is trying to enable the source, perform the validation check.
        if 'disabled' in updates and not updates['disabled']:
            if not schema_cache.is_schema_valid(id):
                raise ValueError(
                    f"Cannot enable source '{id}': Schema cache is not validated. "
                    f"Please run 'discover-schema' for this source first."
                )
        
        # Proceed with the generic update logic from the base class.
        return await super().update_config(id, updates)

    async def cleanup_obsolete_configs(self) -> List[str]:
        """
        Finds and deletes all Source configurations that are both disabled and
        not used by any sync tasks.

        Returns:
            A list of the configuration IDs that were deleted.
        """
        all_sync_configs = self.app_config.get_syncs().values()
        in_use_source_ids = {sync.source for sync in all_sync_configs}

        all_source_configs = self.list_configs()
        obsolete_ids = [
            source_id for source_id, config in all_source_configs.items()
            if config.disabled and source_id not in in_use_source_ids
        ]

        if not obsolete_ids:
            logger.info("No obsolete source configurations to clean up.")
            return []

        logger.info(f"Found {len(obsolete_ids)} obsolete source configurations to clean up: {obsolete_ids}")

        deleted_ids = []
        async with config_lock:
            source_dict = self.app_config.get_sources()
            for an_id in obsolete_ids:
                if an_id in source_dict:
                    source_dict.pop(an_id)
                    # Also remove the schema cache files associated with the obsolete config
                    schema_cache.delete_schema(an_id)
                    logger.info(f"Removed schema cache for obsolete source '{an_id}'.")
                    deleted_ids.append(an_id)
            
            if deleted_ids:
                update_app_config_file()
        
        logger.info(f"Successfully cleaned up {len(deleted_ids)} source configurations.")
        return deleted_ids

    async def check_and_disable_missing_schema_sources(self) -> List[str]:
        """
        Checks all enabled source configurations for a valid schema cache.
        If a valid schema cache is missing, the source is automatically disabled.

        Returns:
            A list of IDs of sources that were disabled.
        """
        disabled_sources = []
        for source_id, config in self.list_configs().items():
            # If the source is enabled but its schema is not valid, disable it.
            if not config.disabled and not schema_cache.is_schema_valid(source_id):
                logger.warning(
                    f"Source '{source_id}' is enabled but its schema is not validated. "
                    f"Disabling it to prevent runtime errors. "
                    f"Please run 'discover-schema' to re-validate."
                )
                await self.disable(source_id)
                disabled_sources.append(source_id)
        
        return disabled_sources

    async def discover_and_cache_fields(self, source_id: str, admin_user: str, admin_password: str):
        """
        Connects to a data source, discovers its available fields, saves them to a
        local cache file, and creates a validation marker file upon success.
        """
        from fustor_core.models.config import PasswdCredential
        from fustor_agent.services.drivers.source_driver import SourceDriverService

        source_config = self.get_config(source_id)
        if not source_config:
            raise ValueError(f"Source config '{source_id}' not found.")

        source_driver_service = SourceDriverService()

        try:
            # Invalidate old schema first to ensure a clean state
            schema_cache.invalidate_schema(source_id)
            logger.debug(f"Invalidated existing schema for source '{source_id}' before discovery.")

            payload = {"uri": source_config.uri}
            # FIX: Only create PasswdCredential and add it to the payload
            # if admin_user was actually provided.
            if admin_user:
                admin_creds = PasswdCredential(user=admin_user, passwd=admin_password)
                payload["admin_creds"] = admin_creds.model_dump()

            fields = await source_driver_service.get_available_fields(
                source_config.driver, **payload
            )
            
            # Save the new schema
            schema_cache.save_source_schema(source_id, fields)
            
            # **CRITICAL STEP**: Mark the new schema as valid
            schema_cache.validate_schema(source_id)
            
            logger.info(f"Fields for source '{source_id}' discovered, cached, and validated successfully.")
        except Exception as e:
            # If any step fails, ensure the cache is left in an invalid state.
            schema_cache.invalidate_schema(source_id)
            logger.error(f"Failed to discover and cache fields for source '{source_id}': {e}", exc_info=True)
            # Re-raise the exception to be handled by the CLI or calling service.
            raise
