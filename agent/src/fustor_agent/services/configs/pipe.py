import logging
from typing import Optional, Dict, Any, List

from fustor_core.models.config import AppConfig, PipeConfig, FieldMapping
from fustor_agent.services.instances.pipe import PipeInstanceService
from .base import BaseConfigService
from .source import SourceConfigService
from .sender import SenderConfigService
from fustor_agent_sdk.interfaces import PipeConfigServiceInterface # Import the interface
from fustor_agent.config.unified import agent_config, AgentPipeConfig # New Unified YAML loader

logger = logging.getLogger("fustor_agent")

class PipeConfigService(BaseConfigService[PipeConfig], PipeConfigServiceInterface): # Inherit from the interface
    """
    Manages PipeConfig objects, supporting both AppConfig and new YAML config files.
    """
    def __init__(
        self,
        app_config: AppConfig,
        source_config_service: SourceConfigService,
        sender_config_service: SenderConfigService
    ):
        super().__init__(app_config, None, 'pipe')
        self.pipe_instance_service: Optional[PipeInstanceService] = None
        self.source_config_service = source_config_service
        self.sender_config_service = sender_config_service
        
        # Ensure YAML configs are loaded
        agent_config.ensure_loaded()

    def set_dependencies(self, pipe_instance_service: PipeInstanceService):
        """Injects the PipeInstanceService for dependency management."""
        self.pipe_instance_service = pipe_instance_service

    def _convert_yaml_to_model(self, y_cfg: AgentPipeConfig) -> PipeConfig:
        """Convert YAML configuration to internal model."""
        fields_mapping = [
            FieldMapping(to=m.to, source=m.source, required=m.required)
            for m in y_cfg.fields_mapping
        ]
        return PipeConfig(
            source=y_cfg.source,
            sender=y_cfg.sender,
            disabled=y_cfg.disabled,
            fields_mapping=fields_mapping,
            audit_interval_sec=y_cfg.audit_interval_sec,
            sentinel_interval_sec=y_cfg.sentinel_interval_sec
        )

    def get_config(self, id: str) -> Optional[PipeConfig]:
        """Get config by ID, checking YAML first then legacy config."""
        # 1. Try YAML first
        yaml_config = agent_config.get_pipe(id)
        if yaml_config:
            return self._convert_yaml_to_model(yaml_config)
            
        # 2. No fallback to AppConfig (Legacy)
        return None

    def list_configs(self) -> Dict[str, PipeConfig]:
        """List all configs, strictly loading from YAML only."""
        configs: Dict[str, PipeConfig] = {}
        
        # Merge YAML configs (YAML is the ONLY source of truth now)
        yaml_configs = agent_config.get_all_pipes()
        for id, y_cfg in yaml_configs.items():
            configs[id] = self._convert_yaml_to_model(y_cfg)
            
        return configs

    async def enable(self, id: str):
        """Enables a Pipe configuration, ensuring its source and sender are also enabled."""
        # First, call the parent enable method to actually enable the pipe config
        await super().enable(id)

        pipe_config = self.get_config(id)
        if not pipe_config:
            raise ValueError(f"Pipe config '{id}' not found after enabling.") # Should not happen

        # Check if source is enabled
        source_config = self.source_config_service.get_config(pipe_config.source)
        if not source_config:
            raise ValueError(f"Source '{pipe_config.source}' for pipe '{id}' not found.")
        if source_config.disabled:
            raise ValueError(f"Source '{pipe_config.source}' for pipe '{id}' is disabled. Please enable the source first.")

        # Check if sender is enabled
        sender_config = self.sender_config_service.get_config(pipe_config.sender)
        if not sender_config:
            raise ValueError(f"Sender '{pipe_config.sender}' for pipe '{id}' not found.")
        if sender_config.disabled:
            raise ValueError(f"Sender '{pipe_config.sender}' for pipe '{id}' is disabled. Please enable the sender first.")

        logger.info(f"Pipe config '{id}' enabled successfully and its dependencies are active.")
        


# Backward compatibility alias
PipeConfigService = PipeConfigService