# fusion/src/fustor_fusion/config/unified.py
"""
Unified Configuration Loader for Fusion.

Config directory: $FUSTOR_HOME/fusion-config/
All YAML files in the directory share the same namespace.
Components (receivers, views, pipes) can reference each other across files.

Example:
  fusion-config/default.yaml:
    receivers:
      http-main:
        driver: http
        port: 8102
        api_keys:
          - {key: fk_key, pipe_id: research-sync}
    views:
      fs-group-1:
        driver: fs
        base_path: /data/target
    pipes:
      ingest-main:
        receiver: http-main
        views: [fs-group-1]
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any, Set
from pydantic import BaseModel

from fustor_core.common import get_fustor_home_dir

logger = logging.getLogger(__name__)


class APIKeyConfig(BaseModel):
    """API Key configuration."""
    key: str
    pipe_id: str


class ReceiverConfig(BaseModel):
    """Configuration for a receiver."""
    driver: str = "http"
    bind_host: str = "0.0.0.0"
    port: int = 8102
    session_timeout_seconds: int = 3600
    api_keys: List[APIKeyConfig] = []


class ViewConfig(BaseModel):
    """Configuration for a view."""
    driver: str = "fs"
    base_path: str = ""
    extra: Dict[str, Any] = {}


class FusionPipeConfig(BaseModel):
    """Configuration for a single Fusion pipe."""
    receiver: str  # Reference to receiver ID
    views: List[str] = []  # References to view IDs
    enabled: bool = True
    allow_concurrent_push: bool = True
    session_timeout_seconds: int = 3600


class FusionConfigLoader:
    """
    Loads and merges all config files from fusion-config/ directory.
    
    All files share the same namespace, allowing cross-file references.
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            home = get_fustor_home_dir()
            config_dir = home / "fusion-config"
        
        self.dir = Path(config_dir)
        
        # Merged namespace
        self._receivers: Dict[str, ReceiverConfig] = {}
        self._views: Dict[str, ViewConfig] = {}
        self._pipes: Dict[str, FusionPipeConfig] = {}
        
        # Track which file defines which pipes
        self._pipes_by_file: Dict[str, Set[str]] = {}
        
        self._loaded = False
    
    def load_all(self) -> None:
        """Load and merge all YAML files from config directory."""
        self._receivers.clear()
        self._views.clear()
        self._pipes.clear()
        self._pipes_by_file.clear()
        
        if not self.dir.exists():
            logger.info(f"Config directory not found: {self.dir}. Creating it.")
            self.dir.mkdir(parents=True, exist_ok=True)
            self._loaded = True
            return
        
        # Load all YAML files
        for yaml_file in sorted(self.dir.glob("*.yaml")):
            self._load_file(yaml_file)
        
        self._loaded = True
        logger.info(
            f"Loaded config: {len(self._receivers)} receivers, "
            f"{len(self._views)} views, {len(self._pipes)} pipes"
        )
    
    def _load_file(self, path: Path) -> None:
        """Load a single config file and merge into namespace."""
        try:
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            
            file_key = path.name
            
            # Merge receivers
            for r_id, r_data in data.get("receivers", {}).items():
                if r_id in self._receivers:
                    logger.warning(f"Receiver '{r_id}' redefined in {path}")
                self._receivers[r_id] = ReceiverConfig(**r_data)
            
            # Merge views
            for v_id, v_data in data.get("views", {}).items():
                if v_id in self._views:
                    logger.warning(f"View '{v_id}' redefined in {path}")
                self._views[v_id] = ViewConfig(**v_data)
            
            # Merge pipes
            pipe_ids = set()
            for pipe_id, pipe_data in data.get("pipes", {}).items():
                if pipe_id in self._pipes:
                    logger.warning(f"Pipe '{pipe_id}' redefined in {path}")
                self._pipes[pipe_id] = FusionPipeConfig(**pipe_data)
                pipe_ids.add(pipe_id)
            
            self._pipes_by_file[file_key] = pipe_ids
            
        except Exception as e:
            logger.error(f"Failed to load config from {path}: {e}")
    
    def ensure_loaded(self) -> None:
        if not self._loaded:
            self.load_all()
    
    def get_receiver(self, receiver_id: str) -> Optional[ReceiverConfig]:
        self.ensure_loaded()
        return self._receivers.get(receiver_id)
    
    def get_view(self, view_id: str) -> Optional[ViewConfig]:
        self.ensure_loaded()
        return self._views.get(view_id)
    
    def get_pipe(self, pipe_id: str) -> Optional[FusionPipeConfig]:
        self.ensure_loaded()
        return self._pipes.get(pipe_id)
    
    def get_all_receivers(self) -> Dict[str, ReceiverConfig]:
        self.ensure_loaded()
        return self._receivers.copy()
    
    def get_all_views(self) -> Dict[str, ViewConfig]:
        self.ensure_loaded()
        return self._views.copy()
    
    def get_all_pipes(self) -> Dict[str, FusionPipeConfig]:
        self.ensure_loaded()
        return self._pipes.copy()
    
    def get_pipes_from_file(self, filename: str) -> Dict[str, FusionPipeConfig]:
        """Get pipes defined in a specific file."""
        self.ensure_loaded()
        pipe_ids = self._pipes_by_file.get(filename, set())
        return {pid: self._pipes[pid] for pid in pipe_ids if pid in self._pipes}
    
    def get_default_pipes(self) -> Dict[str, FusionPipeConfig]:
        """Get pipes from default.yaml."""
        return self.get_pipes_from_file("default.yaml")
    
    def get_enabled_pipes(self) -> Dict[str, FusionPipeConfig]:
        """Get all enabled pipes."""
        self.ensure_loaded()
        return {k: v for k, v in self._pipes.items() if v.enabled}
    
    def resolve_pipe_refs(self, pipe_id: str) -> Optional[Dict[str, Any]]:
        """Resolve a pipe's receiver and view references."""
        pipe = self.get_pipe(pipe_id)
        if not pipe:
            return None
        
        receiver = self.get_receiver(pipe.receiver)
        if not receiver:
            logger.error(f"Pipe '{pipe_id}' references unknown receiver '{pipe.receiver}'")
            return None
        
        views = {}
        for v_id in pipe.views:
            view = self.get_view(v_id)
            if view:
                views[v_id] = view
            else:
                logger.warning(f"Pipe '{pipe_id}' references unknown view '{v_id}'")
        
        return {
            "pipe": pipe,
            "receiver": receiver,
            "views": views,
        }
    
    def reload(self) -> None:
        """Force reload all configurations."""
        self._loaded = False
        self.load_all()


# Global instance
fusion_config = FusionConfigLoader()
