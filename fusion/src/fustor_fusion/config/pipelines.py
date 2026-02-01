# fusion/src/fustor_fusion/config/pipelines.py
"""
Fusion Pipeline configuration loader from YAML files.

This defines how events flow from Receivers to Views in Fusion.
Replaces the old datastore_id concept with direct Pipeline → View binding.

Config file format (fusion-pipes-config/pipe-http.yaml):
```yaml
id: pipe-http
receiver: http-main
views:
  - fs-research
  - fs-archive
enabled: true
session_timeout_seconds: 30
```
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from pydantic import BaseModel, Field, field_validator

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class FusionPipelineConfig(BaseModel):
    """Configuration for a single Fusion pipeline."""
    id: str
    receiver: str  # Receiver ID to accept events from
    views: List[str] = Field(default_factory=list)  # View IDs to dispatch events to
    enabled: bool = True
    session_timeout_seconds: int = 30
    allow_concurrent_push: bool = False
    
    # Additional configuration
    extra: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate that ID is URL-safe."""
        errors = validate_url_safe_id(v, "pipeline id")
        if errors:
            raise ValueError("; ".join(errors))
        return v


class FusionPipelinesConfigLoader:
    """
    Loads and manages Fusion pipeline configurations from a directory of YAML files.
    
    Directory structure:
    ```
    fusion-pipes-config/
      pipe-http.yaml
      pipe-grpc.yaml
    ```
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the loader.
        
        Args:
            config_dir: Explicit path to config directory. If None, will search for
                       fusion-pipes-config/ in FUSTOR_FUSION_HOME.
        """
        self._explicit_dir = config_dir
        self._pipelines: Dict[str, FusionPipelineConfig] = {}
        self._api_key_to_pipeline: Dict[str, str] = {}  # Built from receivers config
        self._loaded = False
        self._active_dir: Optional[Path] = None
    
    def _resolve_config_dir(self) -> Optional[Path]:
        """Find the configuration directory."""
        if self._explicit_dir is not None:
            return Path(self._explicit_dir)
        
        home = get_fustor_home_dir()
        return home / "fusion-pipes-config"
    
    @property
    def dir(self) -> Path:
        """Get the active configuration directory path."""
        if self._active_dir is None:
            self._active_dir = self._resolve_config_dir()
        return self._active_dir
    
    def scan(self) -> Dict[str, FusionPipelineConfig]:
        """
        Scan directory and load all pipeline configurations.
        
        Returns:
            Dict of pipeline_id -> FusionPipelineConfig
        """
        config_dir = self._resolve_config_dir()
        self._active_dir = config_dir
        self._pipelines.clear()
        
        if config_dir is None or not config_dir.exists():
            logger.debug(f"Fusion pipelines config directory not found: {config_dir}")
            self._loaded = True
            return {}
        
        for yaml_file in config_dir.glob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                
                if not data:
                    logger.warning(f"Empty config file: {yaml_file}")
                    continue
                
                config = FusionPipelineConfig(**data)
                self._pipelines[config.id] = config
                logger.debug(f"Loaded Fusion pipeline config: {config.id}")
                
            except Exception as e:
                logger.error(f"Failed to load pipeline config from {yaml_file}: {e}")
        
        self._loaded = True
        logger.info(f"Loaded {len(self._pipelines)} Fusion pipeline configs from {config_dir}")
        return self._pipelines
    
    def ensure_loaded(self) -> None:
        """Ensure configurations are loaded."""
        if not self._loaded:
            self.scan()
    
    def get(self, pipeline_id: str) -> Optional[FusionPipelineConfig]:
        """Get pipeline configuration by ID."""
        self.ensure_loaded()
        return self._pipelines.get(pipeline_id)
    
    def get_all(self) -> Dict[str, FusionPipelineConfig]:
        """Get all pipeline configurations."""
        self.ensure_loaded()
        return self._pipelines.copy()
    
    def get_enabled(self) -> Dict[str, FusionPipelineConfig]:
        """Get all enabled pipeline configurations."""
        self.ensure_loaded()
        return {k: v for k, v in self._pipelines.items() if v.enabled}
    
    def get_by_receiver(self, receiver_id: str) -> List[FusionPipelineConfig]:
        """Get all pipeline configurations for a specific receiver."""
        self.ensure_loaded()
        return [p for p in self._pipelines.values() if p.receiver == receiver_id]
    
    def get_by_view(self, view_id: str) -> List[FusionPipelineConfig]:
        """Get all pipeline configurations that dispatch to a specific view."""
        self.ensure_loaded()
        return [p for p in self._pipelines.values() if view_id in p.views]
    
    def reload(self) -> Dict[str, FusionPipelineConfig]:
        """Force reload all configurations."""
        self._loaded = False
        self._active_dir = None
        return self.scan()


# Global instance
fusion_pipelines_config = FusionPipelinesConfigLoader()
