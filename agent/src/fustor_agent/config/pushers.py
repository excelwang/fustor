# agent/src/fustor_agent/config/pushers.py
"""
Pusher configuration loader from YAML files.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from pydantic import ValidationError

from fustor_common.paths import get_fustor_home_dir
from fustor_core.models.config import PusherConfig
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class PushersConfigLoader:
    """
    Loads and manages pusher configurations from YAML file.
    
    Config file format:
    ```yaml
    fusion-main:
      driver: fusion
      endpoint: http://fusion:8101
      credential:
        key: fk_abc123
    ```
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = get_fustor_home_dir() / "pushers-config.yaml"
        self.path = Path(config_path)
        self._pushers: Dict[str, PusherConfig] = {}
        self._loaded = False
    
    def load(self) -> Dict[str, PusherConfig]:
        """Load configuration from YAML file."""
        if not self.path.exists():
            logger.warning(f"Pushers config not found at {self.path}")
            self._loaded = True
            return {}
        
        try:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}
            
            self._pushers.clear()
            
            # Ensure data is a dict
            if not isinstance(data, dict):
                data = {}

            for p_id, p_data in data.items():
                if not isinstance(p_data, dict):
                    continue
                
                try:
                    p_id_str = str(p_id)
                    # Validate ID early
                    errors = validate_url_safe_id(p_id_str, "pusher id")
                    if errors:
                        logger.error(f"Invalid pusher ID {p_id_str}: {'; '.join(errors)}")
                        continue
                    
                    # Create config model
                    config = PusherConfig(**p_data)
                    self._pushers[p_id_str] = config
                except Exception as e:
                    logger.error(f"Failed to parse pusher {p_id}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self._pushers)} pushers from {self.path}")
            return self._pushers
            
        except Exception as e:
            logger.error(f"Failed to load pushers config: {e}")
            return {}
    
    def ensure_loaded(self) -> None:
        """Ensure configuration is loaded."""
        if not self._loaded:
            self.load()
    
    def get(self, pusher_id: str) -> Optional[PusherConfig]:
        """Get pusher configuration by ID."""
        self.ensure_loaded()
        return self._pushers.get(str(pusher_id))

    def get_all(self) -> Dict[str, PusherConfig]:
        """Get all pusher configurations."""
        self.ensure_loaded()
        return self._pushers.copy()
    
    def reload(self) -> Dict[str, PusherConfig]:
        """Force reload configuration from YAML file."""
        self._loaded = False
        return self.load()


# Global instance
pushers_config = PushersConfigLoader()
