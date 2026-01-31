# agent/src/fustor_agent/config/sources.py
"""
Source configuration loader from YAML files.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from pydantic import ValidationError

from fustor_common.paths import get_fustor_home_dir
from fustor_core.models.config import SourceConfig
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class SourcesConfigLoader:
    """
    Loads and manages source configurations from YAML file.
    
    Config file format:
    ```yaml
    fs-research:
      driver: fs
      uri: /path/to/data
      credential:
        user: admin
        key: secret
    ```
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = get_fustor_home_dir() / "sources-config.yaml"
        self.path = Path(config_path)
        self._sources: Dict[str, SourceConfig] = {}
        self._loaded = False
    
    def load(self) -> Dict[str, SourceConfig]:
        """Load configuration from YAML file."""
        if not self.path.exists():
            logger.warning(f"Sources config not found at {self.path}")
            self._loaded = True
            return {}
        
        try:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}
            
            self._sources.clear()
            
            # Ensure data is a dict
            if not isinstance(data, dict):
                data = {}

            # Support both flat and nested 'sources' key for compatibility
            candidates = []
            if "sources" in data and isinstance(data["sources"], dict):
                candidates.extend(data["sources"].items())
            
            for k, v in data.items():
                if k != "sources":
                    candidates.append((k, v))
            
            for s_id, s_data in candidates:
                if not isinstance(s_data, dict):
                    continue
                
                try:
                    s_id_str = str(s_id)
                    # Validate ID early
                    errors = validate_url_safe_id(s_id_str, "source id")
                    if errors:
                        logger.error(f"Invalid source ID {s_id_str}: {'; '.join(errors)}")
                        continue
                    
                    # Create config model
                    config = SourceConfig(**s_data)
                    self._sources[s_id_str] = config
                except Exception as e:
                    logger.error(f"Failed to parse source {s_id}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self._sources)} sources from {self.path}")
            return self._sources
            
        except Exception as e:
            logger.error(f"Failed to load sources config: {e}")
            return {}
    
    def ensure_loaded(self) -> None:
        """Ensure configuration is loaded."""
        if not self._loaded:
            self.load()
    
    def get(self, source_id: str) -> Optional[SourceConfig]:
        """Get source configuration by ID."""
        self.ensure_loaded()
        return self._sources.get(str(source_id))

    def get_all(self) -> Dict[str, SourceConfig]:
        """Get all source configurations."""
        self.ensure_loaded()
        return self._sources.copy()
    
    def reload(self) -> Dict[str, SourceConfig]:
        """Force reload configuration from YAML file."""
        self._loaded = False
        return self.load()


# Global instance
sources_config = SourcesConfigLoader()
