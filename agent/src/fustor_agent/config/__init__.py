# agent/src/fustor_agent/config/__init__.py
"""
Configuration loaders for YAML-based configuration files.

Config files:
- senders-config.yaml: Sender definitions (HTTP/Fusion endpoints)
- sources-config.yaml: Source definitions (file systems, databases)
- syncs-config/: Sync task definitions
"""
from .pipelines import PipelinesConfigLoader, AgentPipelineConfig, pipelines_config
from .sources import SourcesConfigLoader, sources_config
from .senders import SendersConfigLoader, senders_config, SenderConfig
from .validators import validate_url_safe_id

# Backward compatibility aliases
SyncsConfigLoader = PipelinesConfigLoader
SyncConfigYaml = AgentPipelineConfig
syncs_config = pipelines_config

__all__ = [
    # Senders
    "SendersConfigLoader",
    "SenderConfig",
    "senders_config",
    # Pipelines
    "PipelinesConfigLoader",
    "AgentPipelineConfig",
    "pipelines_config",
    # Sources
    "SourcesConfigLoader",
    "sources_config",
    # Validators
    "validate_url_safe_id",
    # Legacy aliases
    "SyncsConfigLoader",
    "SyncConfigYaml",
    "syncs_config",
]
