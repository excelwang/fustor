from __future__ import annotations
import os
import logging
from typing import Optional, Dict
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

from fustor_core.models.config import AppConfig, PipelineConfig, PipelineConfigDict, SourceConfigDict, SenderConfigDict
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
        from .config import sources_config, senders_config, pipelines_config

        # 1. Load Sources
        sources_config.reload()
        valid_sources = sources_config.get_all()

        # 2. Load Senders
        senders_config.reload()
        valid_senders = senders_config.get_all()

        # 3. Load Pipelines from directory
        pipelines_config.reload()
        valid_pipelines_yaml = pipelines_config.get_all()
        valid_pipelines: Dict[str, PipelineConfig] = {}
        
        # Convert AgentPipelineConfig to PipelineConfig
        for p_id, p_yaml in valid_pipelines_yaml.items():
            # PipelineConfig doesn't have 'id' field, it's the key in the dict
            p_dict = p_yaml.model_dump(exclude={'id'})
            valid_pipelines[p_id] = PipelineConfig(**p_dict)

        _app_config_instance = AppConfig(
            sources=SourceConfigDict(root=valid_sources),
            senders=SenderConfigDict(root=valid_senders),
            pipelines=PipelineConfigDict(root=valid_pipelines)
        )

    return _app_config_instance

__all__ = ["get_app_config", "CONFIG_DIR", "STATE_FILE_PATH", "ConfigurationError"]