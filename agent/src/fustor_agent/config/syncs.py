# agent/src/fustor_agent/config/syncs.py
"""
Sync configuration loader from YAML files in a directory.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List
from pydantic import BaseModel, field_validator

from fustor_common.paths import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class FieldMappingYaml(BaseModel):
    """Field mapping configuration."""
    to: str
    source: List[str]
    required: bool = False


class SyncConfigYaml(BaseModel):
    """Configuration for a single sync task loaded from YAML."""
    id: str
    source: str
    pusher: str
    disabled: bool = False
    fields_mapping: List[FieldMappingYaml] = []
    audit_interval_sec: int = 600
    sentinel_interval_sec: int = 120
    heartbeat_interval_sec: int = 10
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate that ID is URL-safe."""
        errors = validate_url_safe_id(v, "sync id")
        if errors:
            raise ValueError("; ".join(errors))
        return v


class SyncsConfigLoader:
    """
    Loads and manages sync configurations from a directory of YAML files.
    
    Directory structure:
    ```
    syncs-config/
      sync-research.yaml
      sync-archive.yaml
    ```
    
    Each file format:
    ```yaml
    id: sync-research
    source: fs-research
    pusher: fusion-main
    disabled: false
    ```
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            config_dir = get_fustor_home_dir() / "syncs-config"
        self.dir = Path(config_dir)
        self._syncs: Dict[str, SyncConfigYaml] = {}
        self._loaded = False
    
    def scan(self) -> Dict[str, SyncConfigYaml]:
        """
        Scan directory and load all sync configurations.
        
        Returns:
            Dict of sync_id -> SyncConfigYaml
        """
        self._syncs.clear()
        
        if not self.dir.exists():
            logger.debug(f"Syncs config directory not found: {self.dir}")
            self._loaded = True
            return {}
        
        for yaml_file in self.dir.glob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                
                if not data:
                    logger.warning(f"Empty config file: {yaml_file}")
                    continue
                
                config = SyncConfigYaml(**data)
                self._syncs[config.id] = config
                logger.debug(f"Loaded sync config: {config.id}")
                
            except Exception as e:
                logger.error(f"Failed to load sync config from {yaml_file}: {e}")
        
        self._loaded = True
        logger.info(f"Loaded {len(self._syncs)} sync configs from {self.dir}")
        return self._syncs
    
    def ensure_loaded(self) -> None:
        """Ensure configurations are loaded."""
        if not self._loaded:
            self.scan()
    
    def get(self, sync_id: str) -> Optional[SyncConfigYaml]:
        """Get sync configuration by ID."""
        self.ensure_loaded()
        return self._syncs.get(sync_id)
    
    def get_all(self) -> Dict[str, SyncConfigYaml]:
        """Get all sync configurations."""
        self.ensure_loaded()
        return self._syncs.copy()
    
    def get_enabled(self) -> Dict[str, SyncConfigYaml]:
        """Get all enabled (not disabled) sync configurations."""
        self.ensure_loaded()
        return {k: v for k, v in self._syncs.items() if not v.disabled}
    
    def reload(self) -> Dict[str, SyncConfigYaml]:
        """Force reload all configurations."""
        self._loaded = False
        return self.scan()


# Global instance
syncs_config = SyncsConfigLoader()
