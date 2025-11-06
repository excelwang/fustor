import logging
from typing import Optional, Dict, Any, List

from fustor_core.models.config import AppConfig, SyncConfig
from fustor_agent.services.instances.sync import SyncInstanceService
from .base import BaseConfigService
from .source import SourceConfigService
from .pusher import PusherConfigService

logger = logging.getLogger("fustor_agent")

class SyncConfigService(BaseConfigService[SyncConfig]):
    """
    Manages SyncConfig objects.
    """
    def __init__(
        self,
        app_config: AppConfig,
        source_config_service: SourceConfigService,
        pusher_config_service: PusherConfigService
    ):
        super().__init__(app_config, None, 'sync')
        self.sync_instance_service: Optional[SyncInstanceService] = None
        self.source_config_service = source_config_service
        self.pusher_config_service = pusher_config_service

    def set_dependencies(self, sync_instance_service: SyncInstanceService):
        """Injects the SyncInstanceService for dependency management."""
        self.sync_instance_service = sync_instance_service

    async def enable(self, id: str):
        """Enables a Sync configuration, ensuring its source and pusher are also enabled."""
        # First, call the parent enable method to actually enable the sync config
        await super().enable(id)

        sync_config = self.get_config(id)
        if not sync_config:
            raise ValueError(f"Sync config '{id}' not found after enabling.") # Should not happen

        # Check if source is enabled
        source_config = self.source_config_service.get_config(sync_config.source)
        if not source_config:
            raise ValueError(f"Source '{sync_config.source}' for sync '{id}' not found.")
        if source_config.disabled:
            raise ValueError(f"Source '{sync_config.source}' for sync '{id}' is disabled. Please enable the source first.")

        # Check if pusher is enabled
        pusher_config = self.pusher_config_service.get_config(sync_config.pusher)
        if not pusher_config:
            raise ValueError(f"Pusher '{sync_config.pusher}' for sync '{id}' not found.")
        if pusher_config.disabled:
            raise ValueError(f"Pusher '{sync_config.pusher}' for sync '{id}' is disabled. Please enable the pusher first.")

        logger.info(f"Sync config '{id}' enabled successfully and its dependencies are active.")
        
    def get_wizard_definition(self) -> Dict[str, Any]:
        """
        Returns the step definitions for the Sync Task configuration wizard.
        This structure is fetched by the frontend to dynamically build the UI.
        """
        # Get lists of available (enabled) sources and pushers for dropdowns
        enabled_sources = [
            id for id, cfg in self.source_config_service.list_configs().items() if not cfg.disabled
        ]
        enabled_pushers = [
            id for id, cfg in self.pusher_config_service.list_configs().items() if not cfg.disabled
        ]

        return {
            "steps": [
                {
                    "step_id": "initial_selection",
                    "title": "选择源与目标",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string",
                                "title": "同步任务ID",
                                "description": "为新配置指定一个唯一的、易于识别的名称。"
                            },
                            "source": {
                                "type": "string",
                                "title": "选择 Source 配置",
                                "description": "选择一个已配置并启用的数据源。",
                                "enum": enabled_sources
                            },
                            "pusher": {
                                "type": "string",
                                "title": "选择 Pusher 配置",
                                "description": "选择一个已配置并启用的接收端。",
                                "enum": enabled_pushers
                            }
                        },
                        "required": ["id", "source", "pusher"]
                    },
                    "validations": [] # This step triggers a data load action, not a simple validation
                },
                {
                    "step_id": "field_mapping",
                    "title": "字段映射",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "fields_mapping": {
                                "type": "array",
                                "title": "字段映射规则"
                            }
                        },
                        "description": "将Source提供的可用字段映射到Pusher需要的目标字段。名称相似的字段会被自动映射。"
                    },
                    "validations": [] # Validation is performed client-side (all required fields mapped)
                },
                {
                    "step_id": "advanced_settings",
                    "title": "高级参数",
                    "schema": {
                        "type": "object",
                        "properties": {
                        }
                    },
                    "validations": []
                }
            ]
        }