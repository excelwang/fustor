from __future__ import annotations
import os
import yaml
import logging
from typing import Optional, Dict
from dotenv import load_dotenv, find_dotenv
from pydantic import ValidationError
import tempfile
from pathlib import Path

from fustor_core.models.config import AppConfig, SourceConfig, PusherConfig, SyncConfig, SourceConfigDict, PusherConfigDict, SyncConfigDict
from fustor_common.paths import get_fustor_home_dir

# Standardize Fustor home directory across all services
home_fustor_dir = get_fustor_home_dir()

CONFIG_DIR = str(home_fustor_dir)

# Order of .env loading: CONFIG_DIR/.env (highest priority), then project root .env
home_dotenv_path = home_fustor_dir / ".env"
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path) 

# Load .env from project root (lowest priority) - will not override already set variables
load_dotenv(find_dotenv())

CONFIG_FILE_NAME = 'agent-config.yaml' # Renamed from config.yaml
STATE_FILE_NAME = 'agent-state.json'

STATE_FILE_PATH = os.path.join(CONFIG_DIR, STATE_FILE_NAME)

from fustor_common.exceptions import ConfigurationError

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

        # 4. Backward Compatibility Check: Legacy agent-config.yaml
        # If we have no sources and no pushers from the new files, try the old file.
        if not valid_sources and not valid_pushers:
            config_file_path = os.environ.get("FUSTOR_AGENT_CONFIG") or os.path.join(CONFIG_DIR, CONFIG_FILE_NAME)
            if os.path.exists(config_file_path):
                logger.info(f"Attempting to load legacy config from {config_file_path}")
                try:
                    with open(config_file_path, 'r', encoding='utf-8') as f:
                        raw_data = yaml.safe_load(f) or {}
                    
                    # Merge sources if not already loaded from separate file
                    if not valid_sources:
                        for s_id, config_data in raw_data.get('sources', {}).items():
                            try:
                                valid_sources[s_id] = SourceConfig(**config_data)
                            except Exception as e:
                                logger.error(f"Legacy source '{s_id}' invalid: {e}")
                    
                    # Merge pushers if not already loaded from separate file
                    if not valid_pushers:
                        for p_id, config_data in raw_data.get('pushers', {}).items():
                            try:
                                valid_pushers[p_id] = PusherConfig(**config_data)
                            except Exception as e:
                                logger.error(f"Legacy pusher '{p_id}' invalid: {e}")
                                
                    # Merge syncs if not already loaded from directory
                    if not valid_syncs:
                        for sy_id, config_data in raw_data.get('syncs', {}).items():
                            try:
                                valid_syncs[sy_id] = SyncConfig(**config_data)
                            except Exception as e:
                                logger.error(f"Legacy sync '{sy_id}' invalid: {e}")
                except Exception as e:
                    logger.warning(f"Failed to load legacy config: {e}")

        _app_config_instance = AppConfig(
            sources=SourceConfigDict(root=valid_sources),
            pushers=PusherConfigDict(root=valid_pushers),
            syncs=SyncConfigDict(root=valid_syncs)
        )

    return _app_config_instance

def update_app_config_file():
    """Atomically writes the in-memory configuration back to the file."""
    global _app_config_instance
    if _app_config_instance is not None:
        # Ensure the target directory exists.
        if CONFIG_DIR: # Only try to make dirs if CONFIG_DIR is not empty
            os.makedirs(CONFIG_DIR, exist_ok=True) 
        
        # Determine the directory for the temporary file.
        temp_file_parent_dir = CONFIG_DIR if CONFIG_DIR else '.'

        fd, tmp_path = tempfile.mkstemp(dir=temp_file_parent_dir, prefix=".config_tmp_", suffix=".yaml")
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as tmp_file:
                config_dict = _app_config_instance.model_dump(by_alias=True, exclude_none=True)
                yaml.dump(config_dict, tmp_file, default_flow_style=False, allow_unicode=True)
            os.replace(tmp_path, os.path.join(CONFIG_DIR, CONFIG_FILE_NAME))
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

__all__ = ["get_app_config", "update_app_config_file", "CONFIG_DIR", "CONFIG_FILE_NAME", "STATE_FILE_PATH", "ConfigurationError"]