# agent/src/fustor_agent/config/pipelines.py
"""
Agent Pipeline configuration loader from YAML files in a directory.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from pydantic import BaseModel, field_validator, model_validator

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class FieldMappingYaml(BaseModel):
    """Field mapping configuration."""
    to: str
    source: List[str]
    required: bool = False


class AgentPipelineConfig(BaseModel):
    """Configuration for a single agent pipeline loaded from YAML."""
    id: str
    source: str
    sender: Optional[str] = None
    disabled: bool = False
    fields_mapping: List[FieldMappingYaml] = []
    audit_interval_sec: float = 600.0
    sentinel_interval_sec: float = 120.0
    heartbeat_interval_sec: float = 10.0
    



    @field_validator('sender')
    @classmethod
    def validate_sender(cls, v: Optional[str]) -> str:
        if v is None:
            raise ValueError("Field 'sender' is required")
        return v
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate that ID is URL-safe."""
        errors = validate_url_safe_id(v, "pipeline id")
        if errors:
            raise ValueError("; ".join(errors))
        return v






class PipelinesConfigLoader:
    """
    Loads and manages pipeline configurations from a directory of YAML files.
    
    Directory structure:
    ```
    agent-pipes-config/
      pipe-research.yaml
      pipe-archive.yaml
    ```
    
    Backward compatibility:
    None.
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            home = get_fustor_home_dir()
            config_dir = home / "agent-pipes-config"
            
        self.dir = Path(config_dir)
        self._pipelines: Dict[str, AgentPipelineConfig] = {}
        self._loaded = False
    
    def scan(self) -> Dict[str, AgentPipelineConfig]:
        """
        Scan directory and load all pipeline configurations.
        
        Returns:
            Dict of pipeline_id -> AgentPipelineConfig
        """
        self._pipelines.clear()
        
        if not self.dir.exists():
            logger.debug(f"Pipelines config directory not found: {self.dir}")
            self._loaded = True
            return {}
        
        for yaml_file in self.dir.glob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                
                if not data:
                    logger.warning(f"Empty config file: {yaml_file}")
                    continue
                
                config = AgentPipelineConfig(**data)
                self._pipelines[config.id] = config
                logger.debug(f"Loaded pipeline config: {config.id}")
                
            except Exception as e:
                logger.error(f"Failed to load pipeline config from {yaml_file}: {e}")
        
        self._loaded = True
        logger.info(f"Loaded {len(self._pipelines)} pipeline configs from {self.dir}")
        return self._pipelines
    
    def ensure_loaded(self) -> None:
        """Ensure configurations are loaded."""
        if not self._loaded:
            self.scan()
    
    def get(self, pipeline_id: str) -> Optional[AgentPipelineConfig]:
        """Get pipeline configuration by ID."""
        self.ensure_loaded()
        return self._pipelines.get(pipeline_id)
    
    def get_all(self) -> Dict[str, AgentPipelineConfig]:
        """Get all pipeline configurations."""
        self.ensure_loaded()
        return self._pipelines.copy()
    
    def get_enabled(self) -> Dict[str, AgentPipelineConfig]:
        """Get all enabled (not disabled) pipeline configurations."""
        self.ensure_loaded()
        return {k: v for k, v in self._pipelines.items() if not v.disabled}
    
    def reload(self) -> Dict[str, AgentPipelineConfig]:
        """Force reload all configurations."""
        self._loaded = False
        return self.scan()


# Global instance
pipelines_config = PipelinesConfigLoader()
