# agent/src/fustor_agent/config/senders.py
"""
Sender configuration loader from YAML files.

This is the new naming for what was previously called "pushers".
For backward compatibility, this loader will try to load from:
1. senders-config.yaml (new)
2. pushers-config.yaml (legacy)
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, Any

from fustor_core.common import get_fustor_home_dir
from fustor_core.models.config import PusherConfig  # Still use PusherConfig internally
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


# Type alias for clarity - SenderConfig is the same as PusherConfig
SenderConfig = PusherConfig


class SendersConfigLoader:
    """
    Loads and manages sender configurations from YAML file.
    
    Config file format:
    ```yaml
    fusion-main:
      driver: http  # or 'fusion' for compatibility
      endpoint: http://fusion:8101
      credential:
        key: fk_abc123
    ```
    
    For backward compatibility, also supports pushers-config.yaml format.
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the loader.
        
        Args:
            config_path: Explicit path to config file. If None, will search for
                        senders-config.yaml or pushers-config.yaml in FUSTOR_HOME.
        """
        self._explicit_path = config_path
        self._senders: Dict[str, SenderConfig] = {}
        self._loaded = False
        self._active_path: Optional[Path] = None
    
    def _resolve_config_path(self) -> Optional[Path]:
        """Find the configuration file, preferring new naming."""
        if self._explicit_path is not None:
            return Path(self._explicit_path)
        
        home = get_fustor_home_dir()
        
        # Try new naming first
        new_path = home / "senders-config.yaml"
        if new_path.exists():
            return new_path
        
        # Fall back to legacy naming
        legacy_path = home / "pushers-config.yaml"
        if legacy_path.exists():
            logger.info(f"Using legacy pushers-config.yaml (migrate to senders-config.yaml)")
            return legacy_path
        
        # Neither exists
        return new_path  # Return new path for error messages
    
    @property
    def path(self) -> Path:
        """Get the active configuration file path."""
        if self._active_path is None:
            self._active_path = self._resolve_config_path()
        return self._active_path
    
    def load(self) -> Dict[str, SenderConfig]:
        """Load configuration from YAML file."""
        config_path = self._resolve_config_path()
        self._active_path = config_path
        
        if config_path is None or not config_path.exists():
            logger.warning(f"Senders config not found at {config_path}")
            self._loaded = True
            return {}
        
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}
            
            self._senders.clear()
            
            if not isinstance(data, dict):
                data = {}

            for s_id, s_data in data.items():
                if not isinstance(s_data, dict):
                    continue
                
                try:
                    s_id_str = str(s_id)
                    errors = validate_url_safe_id(s_id_str, "sender id")
                    if errors:
                        logger.error(f"Invalid sender ID {s_id_str}: {'; '.join(errors)}")
                        continue
                    
                    # Map 'http' driver to 'fusion' for backward compatibility
                    driver = s_data.get('driver', 'fusion')
                    if driver == 'http':
                        s_data['driver'] = 'fusion'
                    
                    config = SenderConfig(**s_data)
                    self._senders[s_id_str] = config
                except Exception as e:
                    logger.error(f"Failed to parse sender {s_id}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self._senders)} senders from {config_path}")
            return self._senders
            
        except Exception as e:
            logger.error(f"Failed to load senders config: {e}")
            return {}
    
    def ensure_loaded(self) -> None:
        """Ensure configuration is loaded."""
        if not self._loaded:
            self.load()
    
    def get(self, sender_id: str) -> Optional[SenderConfig]:
        """Get sender configuration by ID."""
        self.ensure_loaded()
        return self._senders.get(str(sender_id))
    
    def get_all(self) -> Dict[str, SenderConfig]:
        """Get all sender configurations."""
        self.ensure_loaded()
        return self._senders.copy()
    
    def reload(self) -> Dict[str, SenderConfig]:
        """Force reload configuration from YAML file."""
        self._loaded = False
        self._active_path = None
        return self.load()


# Global instance
senders_config = SendersConfigLoader()

# Backward compatibility alias
PushersConfigLoader = SendersConfigLoader
pushers_config = senders_config
