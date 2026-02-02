import logging
from typing import Optional, Dict, Any, List

from fustor_core.models.config import AppConfig, PipelineConfig, FieldMapping
from fustor_agent.services.instances.pipeline import PipelineInstanceService
from .base import BaseConfigService
from .source import SourceConfigService
from .sender import SenderConfigService
from fustor_agent_sdk.interfaces import SyncConfigServiceInterface # Import the interface
from fustor_agent.config.pipelines import pipelines_config, AgentPipelineConfig # New YAML loader

logger = logging.getLogger("fustor_agent")

class PipelineConfigService(BaseConfigService[PipelineConfig], SyncConfigServiceInterface): # Inherit from the interface
    """
    Manages PipelineConfig objects, supporting both legacy AppConfig and new YAML config files.
    """
    def __init__(
        self,
        app_config: AppConfig,
        source_config_service: SourceConfigService,
        sender_config_service: SenderConfigService
    ):
        super().__init__(app_config, None, 'pipeline')
        self.pipeline_instance_service: Optional[PipelineInstanceService] = None
        self.source_config_service = source_config_service
        self.sender_config_service = sender_config_service
        
        # Ensure YAML configs are loaded
        pipelines_config.ensure_loaded()

    def set_dependencies(self, pipeline_instance_service: PipelineInstanceService):
        """Injects the PipelineInstanceService for dependency management."""
        self.pipeline_instance_service = pipeline_instance_service

    def _convert_yaml_to_model(self, y_cfg: AgentPipelineConfig) -> PipelineConfig:
        """Convert YAML configuration to internal model."""
        fields_mapping = [
            FieldMapping(to=m.to, source=m.source, required=m.required)
            for m in y_cfg.fields_mapping
        ]
        return PipelineConfig(
            source=y_cfg.source,
            sender=y_cfg.sender,
            disabled=y_cfg.disabled,
            fields_mapping=fields_mapping,
            audit_interval_sec=y_cfg.audit_interval_sec,
            sentinel_interval_sec=y_cfg.sentinel_interval_sec,
            heartbeat_interval_sec=y_cfg.heartbeat_interval_sec
        )

    def get_config(self, id: str) -> Optional[PipelineConfig]:
        """Get config by ID, checking YAML first then legacy config."""
        # 1. Try YAML first
        yaml_config = pipelines_config.get(id)
        if yaml_config:
            return self._convert_yaml_to_model(yaml_config)
            
        # 2. Fallback to AppConfig
        return super().get_config(id)

    def list_configs(self) -> Dict[str, PipelineConfig]:
        """List all configs, merging YAML and legacy configurations."""
        # Start with legacy configs
        configs = super().list_configs().copy()
        
        # Merge YAML configs (YAML takes precedence if ID conflicts)
        yaml_configs = pipelines_config.get_all()
        for id, y_cfg in yaml_configs.items():
            configs[id] = self._convert_yaml_to_model(y_cfg)
            
        return configs

    async def enable(self, id: str):
        """Enables a Pipeline configuration, ensuring its source and sender are also enabled."""
        # First, call the parent enable method to actually enable the pipeline config
        await super().enable(id)

        pipeline_config = self.get_config(id)
        if not pipeline_config:
            raise ValueError(f"Pipeline config '{id}' not found after enabling.") # Should not happen

        # Check if source is enabled
        source_config = self.source_config_service.get_config(pipeline_config.source)
        if not source_config:
            raise ValueError(f"Source '{pipeline_config.source}' for pipeline '{id}' not found.")
        if source_config.disabled:
            raise ValueError(f"Source '{pipeline_config.source}' for pipeline '{id}' is disabled. Please enable the source first.")

        # Check if sender is enabled
        sender_config = self.sender_config_service.get_config(pipeline_config.sender)
        if not sender_config:
            raise ValueError(f"Sender '{pipeline_config.sender}' for pipeline '{id}' not found.")
        if sender_config.disabled:
            raise ValueError(f"Sender '{pipeline_config.sender}' for pipeline '{id}' is disabled. Please enable the sender first.")

        logger.info(f"Pipeline config '{id}' enabled successfully and its dependencies are active.")
        
    def get_wizard_definition(self) -> Dict[str, Any]:
        """
        Returns the step definitions for the Pipeline configuration wizard.
        This structure is fetched by the frontend to dynamically build the UI.
        """
        # Get lists of available (enabled) sources and senders for dropdowns
        enabled_sources = [
            id for id, cfg in self.source_config_service.list_configs().items() if not cfg.disabled
        ]
        enabled_senders = [
            id for id, cfg in self.sender_config_service.list_configs().items() if not cfg.disabled
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
                                "title": "Pipeline ID",
                                "description": "为新配置指定一个唯一的、易于识别的名称。"
                            },
                            "source": {
                                "type": "string",
                                "title": "选择 Source 配置",
                                "description": "选择一个已配置并启用的数据源。",
                                "enum": enabled_sources
                            },
                            "sender": {
                                "type": "string",
                                "title": "选择 Sender 配置",
                                "description": "选择一个已配置并启用的接收端。",
                                "enum": enabled_senders
                            }
                        },
                        "required": ["id", "source", "sender"]
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
                        "description": "将Source提供的可用字段映射到目标字段。名称相似的字段会被自动映射。"
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

# Backward compatibility alias
SyncConfigService = PipelineConfigService