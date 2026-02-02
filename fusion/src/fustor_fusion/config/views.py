# fusion/src/fustor_fusion/config/views.py
"""
View configuration loader from YAML files in a directory.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List
from typing import Dict, Optional, List, Any
from pydantic import BaseModel, field_validator, Field, AliasChoices, model_validator

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class ViewConfig(BaseModel):
    """Configuration for a single view."""
    id: str
    view_id: str
    driver: str
    disabled: bool = False
    driver_params: dict = {}
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate that ID is URL-safe."""
        errors = validate_url_safe_id(v, "view id")
        if errors:
            raise ValueError("; ".join(errors))
        return v

    @field_validator('view_id', mode='before')
    @classmethod
    def validate_view_id(cls, v: Any) -> str:
        """Validate that ID is URL-safe and convert to string."""
        s = str(v)
        errors = validate_url_safe_id(s, "view group id")
        if errors:
            raise ValueError("; ".join(errors))
        return s

    @model_validator(mode='before')
    @classmethod
    def set_default_view_id(cls, data: Any) -> Any:
        # If view_id is not provided, default to id
        if isinstance(data, dict):
            if 'view_id' not in data and 'id' in data:
                data['view_id'] = str(data['id'])
        return data
    



class ViewsConfigLoader:
    """
    Loads and manages view configurations from a directory of YAML files.
    
    Directory structure:
    ```
    views-config/
      research-fs.yaml
      archive-fs.yaml
    ```
    
    Each file format:
    ```yaml
    id: research-fs
    view_id: 1
    driver: fs
    disabled: false
    driver_params:
      hot_file_threshold: 600
    ```
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            config_dir = get_fustor_home_dir() / "views-config"
        self.dir = Path(config_dir)
        self._views: Dict[str, ViewConfig] = {}
        self._loaded = False
    
    def scan(self) -> Dict[str, ViewConfig]:
        """
        Scan directory and load all view configurations.
        
        Returns:
            Dict of view_id -> ViewConfig
        """
        self._views.clear()
        
        if not self.dir.exists():
            logger.debug(f"Views config directory not found: {self.dir}")
            self._loaded = True
            return {}
        
        for yaml_file in self.dir.glob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                
                if not data:
                    logger.warning(f"Empty config file: {yaml_file}")
                    continue
                


                config = ViewConfig(**data)
                self._views[config.id] = config
                logger.debug(f"Loaded view config: {config.id}")
                
            except Exception as e:
                logger.error(f"Failed to load view config from {yaml_file}: {e}")
        
        self._loaded = True
        logger.info(f"Loaded {len(self._views)} view configs from {self.dir}")
        return self._views
    
    def ensure_loaded(self) -> None:
        """Ensure configurations are loaded."""
        if not self._loaded:
            self.scan()
    
    def get(self, view_id: str) -> Optional[ViewConfig]:
        """Get view configuration by ID."""
        self.ensure_loaded()
        return self._views.get(view_id)
    
    def get_all(self) -> Dict[str, ViewConfig]:
        """Get all view configurations."""
        self.ensure_loaded()
        return self._views.copy()
    
    def get_enabled(self) -> Dict[str, ViewConfig]:
        """Get all enabled (not disabled) view configurations."""
        self.ensure_loaded()
        return {k: v for k, v in self._views.items() if not v.disabled}
    
    def get_by_view(self, view_id: str) -> List[ViewConfig]:
        """Get all view configurations for a specific view group."""
        self.ensure_loaded()
        v_id_str = str(view_id)
        return [v for v in self._views.values() if v.view_id == v_id_str]


    
    def reload(self) -> Dict[str, ViewConfig]:
        """Force reload all configurations."""
        self._loaded = False
        return self.scan()


# Global instance
views_config = ViewsConfigLoader()
