# agent/src/fustor_agent/config/__init__.py
"""
Configuration loaders for YAML-based configuration files.

Naming conventions (new -> legacy):
- senders-config.yaml -> pushers-config.yaml
- agent-pipes-config/ -> syncs-config/
"""
from .syncs import SyncsConfigLoader, SyncConfigYaml, syncs_config
from .sources import SourcesConfigLoader, sources_config
from .senders import SendersConfigLoader, senders_config, SenderConfig
from .validators import validate_url_safe_id

# Backward compatibility aliases
from .senders import PushersConfigLoader, pushers_config

__all__ = [
    # New naming
    "SendersConfigLoader",
    "SenderConfig",
    "senders_config",
    # Syncs (will be renamed to pipelines later)
    "SyncsConfigLoader",
    "SyncConfigYaml",
    "syncs_config",
    # Sources
    "SourcesConfigLoader",
    "sources_config",
    # Validators
    "validate_url_safe_id",
    # Legacy aliases
    "PushersConfigLoader",
    "pushers_config",
]
