from __future__ import annotations
import os
import yaml
import logging
from typing import Optional
from dotenv import load_dotenv, find_dotenv
from pydantic import ValidationError
import tempfile
from pathlib import Path

from fustor_core.models.config import AppConfig, SourceConfig, PusherConfig, SyncConfig, SourceConfigDict, PusherConfigDict, SyncConfigDict
# Order of .env loading: ~/.fustor/.env (highest priority), then FUAGENT_CONFIG_DIR/.env (if different), then project root .env
home_fustor_dir = Path.home() / ".fustor"
CONFIG_DIR = str(home_fustor_dir)

# Order of .env loading: ~/.fustor/.env (highest priority), then project root .env
home_dotenv_path = home_fustor_dir / ".env"
if home_dotenv_path.is_file():
    load_dotenv(home_dotenv_path) # Load from ~/.fustor/.env first

# Load .env from project root (lowest priority) - will not override already set variables
load_dotenv(find_dotenv())

CONFIG_FILE_NAME = 'config.yaml'
STATE_FILE_NAME = 'fustor_agent.state.json'

STATE_FILE_PATH = os.path.join(CONFIG_DIR, STATE_FILE_NAME)

from fustor_common.exceptions import ConfigurationError

_app_config_instance: Optional[AppConfig] = None 

logger = logging.getLogger("fustor_agent")
logger.setLevel(logging.DEBUG)


def get_app_config() -> AppConfig:
    global _app_config_instance
    if _app_config_instance is None:
        config_file_path = os.path.join(CONFIG_DIR, CONFIG_FILE_NAME)
        raw_data = {}
        
        if CONFIG_DIR and not os.path.exists(CONFIG_DIR):
            try:
                os.makedirs(CONFIG_DIR)
                logger.info(f"Created configuration directory: {CONFIG_DIR}")
            except OSError as e:
                raise ConfigurationError(f"无法创建配置目录 '{CONFIG_DIR}': {e}")

        if os.path.exists(config_file_path):
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = yaml.safe_load(f)
                raw_data = loaded_data if loaded_data is not None else {}
            except (yaml.YAMLError, Exception) as e:
                raise ConfigurationError(f"加载或解析配置文件 '{config_file_path}' 时发生错误: {e}")
        else:
            logger.warning(f"配置文件 '{config_file_path}' 不存在。将使用默认配置启动。")

        # Resiliently load configurations
        valid_sources = {}
        for id, config_data in raw_data.get('sources', {}).items():
            if not config_data:
                logger.error(f"Source '{id}' has an empty configuration and will be skipped.")
                continue
            try:
                for key, value in config_data.items():
                    if isinstance(value, str):
                        config_data[key] = value.strip()
                valid_sources[id] = SourceConfig(**config_data)
            except ValidationError as e:
                logger.error(f"Source '{id}' 配置无效，已作为错误条目加载: {e}")
                valid_sources[id] = SourceConfig(
                    driver='invalid-config',
                    uri=config_data.get('uri', 'N/A'),
                    credential={'user': 'invalid', 'passwd': ''},
                    disabled=True,
                    validation_error=str(e)
                )

        valid_pushers = {}
        for id, config_data in raw_data.get('pushers', {}).items():
            if not config_data:
                logger.error(f"Pusher '{id}' has an empty configuration and will be skipped.")
                continue
            try:
                for key, value in config_data.items():
                    if isinstance(value, str):
                        config_data[key] = value.strip()
                valid_pushers[id] = PusherConfig(**config_data)
            except ValidationError as e:
                logger.error(f"Pusher '{id}' 配置无效，已跳过: {e}")

        valid_syncs = {}
        for id, config_data in raw_data.get('syncs', {}).items():
            if not config_data:
                logger.error(f"Sync '{id}' has an empty configuration and will be skipped.")
                continue
            try:
                for key, value in config_data.items():
                    if isinstance(value, str):
                        config_data[key] = value.strip()
                valid_syncs[id] = SyncConfig(**config_data)
            except ValidationError as e:
                logger.error(f"Sync '{id}' 配置无效，已跳过: {e}")

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