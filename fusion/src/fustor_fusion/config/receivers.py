# fusion/src/fustor_fusion/config/receivers.py
"""
Receiver configuration loader from YAML files.

This replaces the API key management that was in views-config.yaml.
Receivers define transport endpoints and their associated credentials.

Config file format:
```yaml
http-main:
  driver: http
  bind_host: 0.0.0.0
  port: 8101
  session_timeout_seconds: 30
  api_keys:
    - key: fk_abc123
      pipeline_id: pipeline-1
    - key: fk_def456
      pipeline_id: pipeline-2
```
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from pydantic import BaseModel, Field

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class ApiKeyMapping(BaseModel):
    """Mapping of API key to pipeline ID."""
    key: str
    pipeline_id: str


class ReceiverConfig(BaseModel):
    """Configuration for a single receiver."""
    driver: str = "http"  # Transport driver type
    bind_host: str = "0.0.0.0"
    port: int = 8101
    session_timeout_seconds: int = 30
    allow_concurrent_push: bool = False
    api_keys: List[ApiKeyMapping] = Field(default_factory=list)
    
    # Additional driver-specific configuration
    extra: Dict[str, Any] = Field(default_factory=dict)


class ReceiversConfigLoader:
    """
    Loads and manages receiver configurations from YAML file.
    
    This class handles:
    - Loading receiver configurations (transport endpoints)
    - API key to pipeline ID mapping
    - Session timeout configuration
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the loader.
        
        Args:
            config_path: Explicit path to config file. If None, will search for
                        receivers-config.yaml in FUSTOR_HOME.
        """
        self._explicit_path = config_path
        self._receivers: Dict[str, ReceiverConfig] = {}
        self._api_key_to_pipeline: Dict[str, str] = {}  # api_key -> pipeline_id
        self._api_key_to_receiver: Dict[str, str] = {}  # api_key -> receiver_id
        self._loaded = False
        self._active_path: Optional[Path] = None
    
    def _resolve_config_path(self) -> Optional[Path]:
        """Find the configuration file."""
        if self._explicit_path is not None:
            return Path(self._explicit_path)
        
        home = get_fustor_home_dir()
        return home / "receivers-config.yaml"
    
    @property
    def path(self) -> Path:
        """Get the active configuration file path."""
        if self._active_path is None:
            self._active_path = self._resolve_config_path()
        return self._active_path
    
    def load(self) -> Dict[str, ReceiverConfig]:
        """Load configuration from YAML file."""
        config_path = self._resolve_config_path()
        self._active_path = config_path
        
        if config_path is None or not config_path.exists():
            logger.warning(f"Receivers config not found at {config_path}")
            self._loaded = True
            return {}
        
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}
            
            self._receivers.clear()
            self._api_key_to_pipeline.clear()
            self._api_key_to_receiver.clear()
            
            if not isinstance(data, dict):
                data = {}
            
            for r_id, r_data in data.items():
                if not isinstance(r_data, dict):
                    continue
                
                try:
                    r_id_str = str(r_id)
                    errors = validate_url_safe_id(r_id_str, "receiver id")
                    if errors:
                        logger.error(f"Invalid receiver ID {r_id_str}: {'; '.join(errors)}")
                        continue
                    
                    # Parse API keys list
                    api_keys_raw = r_data.pop("api_keys", [])
                    api_keys = []
                    for ak in api_keys_raw:
                        if isinstance(ak, dict):
                            api_keys.append(ApiKeyMapping(**ak))
                        elif isinstance(ak, str):
                            # Simple format: just the key, pipeline_id = receiver_id
                            api_keys.append(ApiKeyMapping(key=ak, pipeline_id=r_id_str))
                    
                    config = ReceiverConfig(api_keys=api_keys, **r_data)
                    self._receivers[r_id_str] = config
                    
                    # Build API key mappings
                    for ak in config.api_keys:
                        self._api_key_to_pipeline[ak.key] = ak.pipeline_id
                        self._api_key_to_receiver[ak.key] = r_id_str
                        
                except Exception as e:
                    logger.error(f"Failed to parse receiver {r_id}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self._receivers)} receivers from {config_path}")
            return self._receivers
            
        except Exception as e:
            logger.error(f"Failed to load receivers config: {e}")
            return {}
    
    def ensure_loaded(self) -> None:
        """Ensure configuration is loaded."""
        if not self._loaded:
            self.load()
    
    def get(self, receiver_id: str) -> Optional[ReceiverConfig]:
        """Get receiver configuration by ID."""
        self.ensure_loaded()
        return self._receivers.get(str(receiver_id))
    
    def get_all(self) -> Dict[str, ReceiverConfig]:
        """Get all receiver configurations."""
        self.ensure_loaded()
        return self._receivers.copy()
    
    def validate_api_key(self, api_key: str) -> Optional[str]:
        """
        Validate API key and return associated pipeline_id.
        
        Args:
            api_key: The API key to validate
            
        Returns:
            pipeline_id if valid, None otherwise
        """
        self.ensure_loaded()
        return self._api_key_to_pipeline.get(api_key)
    
    def get_receiver_for_api_key(self, api_key: str) -> Optional[str]:
        """
        Get the receiver ID associated with an API key.
        
        Args:
            api_key: The API key
            
        Returns:
            receiver_id if found, None otherwise
        """
        self.ensure_loaded()
        return self._api_key_to_receiver.get(api_key)
    
    def get_active_pipelines(self) -> Dict[str, Any]:
        """
        Get all active pipeline IDs from configured API keys.
        
        This returns a dict compatible with processing_manager.pipeline_tasks().
        
        Returns:
            Dict mapping pipeline_id to configuration info
        """
        self.ensure_loaded()
        pipelines = {}
        for api_key, pipeline_id in self._api_key_to_pipeline.items():
            if pipeline_id not in pipelines:
                # Get the receiver for this API key
                receiver_id = self._api_key_to_receiver.get(api_key)
                receiver = self._receivers.get(receiver_id) if receiver_id else None
                pipelines[pipeline_id] = {
                    "id": pipeline_id,
                    "receiver_id": receiver_id,
                    "session_timeout_seconds": receiver.session_timeout_seconds if receiver else 30,
                    "allow_concurrent_push": receiver.allow_concurrent_push if receiver else False,
                }
        return pipelines
    
    def reload(self) -> Dict[str, ReceiverConfig]:
        """Force reload configuration from YAML file."""
        self._loaded = False
        self._active_path = None
        return self.load()


# Global instance
receivers_config = ReceiversConfigLoader()
