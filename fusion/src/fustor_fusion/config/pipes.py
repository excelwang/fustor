# fusion/src/fustor_fusion/config/pipes.py
"""
Unified Fusion Pipe configuration loader.

New config structure:
$FUSTOR_HOME/pipes/
  ├── default.yaml        # List of pipes to start by default
  ├── ingest-pipe.yaml    # Single pipe config with embedded receiver and views
  └── ...
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any, Union
from pydantic import BaseModel, field_validator

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class APIKeyConfig(BaseModel):
    """API Key configuration for receiver."""
    key: str
    pipe_id: str


class EmbeddedReceiverConfig(BaseModel):
    """Embedded receiver configuration within a pipe."""
    driver: str = "http"
    bind_host: str = "0.0.0.0"
    port: int = 8102
    session_timeout_seconds: int = 3600
    api_keys: List[APIKeyConfig] = []


class FusionPipeConfig(BaseModel):
    """
    Unified Fusion pipe configuration with embedded receiver and views.
    
    Example:
    ```yaml
    id: ingest-pipe
    receiver:
      driver: http
      port: 8102
      api_keys:
        - key: fk_dev_key
          pipe_id: test-sync-task
    views:
      - fs-group-1
    allow_concurrent_push: true
    session_timeout_seconds: 3600
    enabled: true
    ```
    """
    id: str
    receiver: EmbeddedReceiverConfig
    views: List[str] = []
    allow_concurrent_push: bool = True
    session_timeout_seconds: int = 3600
    enabled: bool = True
    extra: Dict[str, Any] = {}
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate that ID is URL-safe."""
        errors = validate_url_safe_id(v, "pipe id")
        if errors:
            raise ValueError("; ".join(errors))
        return v


class DefaultPipesConfig(BaseModel):
    """List of pipes to start by default."""
    pipes: List[str] = []


class PipesConfigLoader:
    """
    Loads Fusion pipe configurations from $FUSTOR_HOME/pipes/ directory.
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            home = get_fustor_home_dir()
            config_dir = home / "config"
        
        self.dir = Path(config_dir)
        self._pipes: Dict[str, FusionPipeConfig] = {}
        self._default_list: List[str] = []
        self._loaded = False
    
    def scan(self) -> Dict[str, FusionPipeConfig]:
        """Scan directory and load all pipe configurations."""
        self._pipes.clear()
        self._default_list.clear()
        
        if not self.dir.exists():
            self.dir.mkdir(parents=True, exist_ok=True)
            self._loaded = True
            return {}
        
        # Load default.yaml
        default_file = self.dir / "default.yaml"
        if default_file.exists():
            try:
                with open(default_file) as f:
                    data = yaml.safe_load(f) or {}
                self._default_list = DefaultPipesConfig(**data).pipes
            except Exception as e:
                logger.error(f"Failed to load default.yaml: {e}")
        
        # Load individual pipe configs
        for yaml_file in self.dir.glob("*.yaml"):
            if yaml_file.name == "default.yaml":
                continue
            
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                if not data:
                    continue
                
                # Fusion specific: we might need to distinguish Agent vs Fusion configs
                # For now, if it has 'receiver' or 'views', it's likely a Fusion pipe config
                if 'receiver' in data or 'views' in data:
                    config = FusionPipeConfig(**data)
                    self._pipes[config.id] = config
                    logger.debug(f"Loaded Fusion pipe config: {config.id}")
            except Exception as e:
                logger.debug(f"Skipping non-Fusion config {yaml_file}: {e}")
        
        self._loaded = True
        return self._pipes
    
    def ensure_loaded(self) -> None:
        if not self._loaded:
            self.scan()
    
    def get(self, pipe_id: str) -> Optional[FusionPipeConfig]:
        self.ensure_loaded()
        return self._pipes.get(pipe_id)
    
    def get_all(self) -> Dict[str, FusionPipeConfig]:
        self.ensure_loaded()
        return self._pipes.copy()
    
    def get_enabled(self) -> Dict[str, FusionPipeConfig]:
        self.ensure_loaded()
        return {k: v for k, v in self._pipes.items() if v.enabled}
    
    def get_default_list(self) -> List[str]:
        self.ensure_loaded()
        return self._default_list.copy()

    def resolve_config_path(self, name_or_path: str) -> Optional[Path]:
        path = Path(name_or_path)
        if path.is_absolute():
            return path if path.exists() else None
        if path.suffix in ('.yaml', '.yml'):
            resolved = Path.cwd() / path
            return resolved if resolved.exists() else None
        for ext in ('.yaml', '.yml'):
            candidate = self.dir / f"{name_or_path}{ext}"
            if candidate.exists():
                return candidate
        return None

    def load_from_path(self, path: Path) -> Optional[FusionPipeConfig]:
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            if not data:
                return None
            return FusionPipeConfig(**data)
        except Exception as e:
            logger.error(f"Failed to load Fusion pipe config from {path}: {e}")
            return None


# Global instance
pipes_config = PipesConfigLoader()
