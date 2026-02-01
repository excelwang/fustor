from __future__ import annotations
import os
import logging
from typing import Optional, Dict
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

from fustor_core.models.config import AppConfig, SyncConfig, SyncConfigDict, SourceConfigDict, SenderConfigDict
from fustor_core.common import get_fustor_home_dir

# Standardize Fustor home directory across all services
home_fustor_dir = get_fustor_home_dir()

CONFIG_DIR = str(home_fustor_dir)

# Order of .env loading: CONFIG_DIR/.env (highest priority), then project root .env
home_dotenv_path = home_fustor_dir / ".env"
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path) 

# Load .env from project root (lowest priority) - will not override already set variables
load_dotenv(find_dotenv())

STATE_FILE_NAME = 'agent-state.json'
STATE_FILE_PATH = os.path.join(CONFIG_DIR, STATE_FILE_NAME)

from fustor_core.exceptions import ConfigError as ConfigurationError

_app_config_instance: Optional[AppConfig] = None 

logger = logging.getLogger("fustor_agent")
logger.setLevel(logging.DEBUG)


def get_app_config() -> AppConfig:
    global _app_config_instance
    if _app_config_instance is None:
        from .config import sources_config, pushers_config, syncs_config

        # 1. Load Sources
        sources_config.reload()
        valid_sources = sources_config.get_all()

        # 2. Load Pushers
        pushers_config.reload()
        valid_pushers = pushers_config.get_all()

        # 3. Load Syncs from directory
        syncs_config.reload()
        valid_syncs_yaml = syncs_config.get_all()
        valid_syncs: Dict[str, SyncConfig] = {}
        
        # Convert SyncConfigYaml to SyncConfig
        for s_id, s_yaml in valid_syncs_yaml.items():
            # SyncConfig doesn't have 'id' field, it's the key in the dict
            s_dict = s_yaml.model_dump(exclude={'id'})
            valid_syncs[s_id] = SyncConfig(**s_dict)

        _app_config_instance = AppConfig(
            sources=SourceConfigDict(root=valid_sources),
            pushers=SenderConfigDict(root=valid_pushers),
            syncs=SyncConfigDict(root=valid_syncs)
        )

    return _app_config_instance

__all__ = ["get_app_config", "CONFIG_DIR", "STATE_FILE_PATH", "ConfigurationError"]