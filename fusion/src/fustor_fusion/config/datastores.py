# fusion/src/fustor_fusion/config/datastores.py
"""
Datastore configuration loader from YAML files.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List
from pydantic import BaseModel, field_validator

from fustor_core.common import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class DatastoreConfig(BaseModel):
    """Configuration for a single datastore."""
    session_timeout_seconds: int = 30
    allow_concurrent_push: bool = False
    api_key: str


class DatastoresConfigLoader:
    """
    Loads and manages datastore configurations from YAML file.
    
    Config file format:
    ```yaml
    research-data:
      session_timeout_seconds: 30
      api_key: fk_abc123xyz
    ```
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = get_fustor_home_dir() / "datastores-config.yaml"
        self.path = Path(config_path)
        self._datastores: Dict[str, DatastoreConfig] = {}
        self._api_key_map: Dict[str, str] = {}  # api_key -> datastore_id
        self._loaded = False
    
    def load(self) -> None:
        """Load configuration from YAML file."""
        if not self.path.exists():
            logger.warning(f"Datastores config not found at {self.path}")
            return
        
        try:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}
            
            self._datastores.clear()
            self._api_key_map.clear()
            
            for ds_id, ds_data in data.items():
                if not isinstance(ds_data, dict):
                    continue
                    
                try:
                    ds_id_str = str(ds_id)
                    # Validate ID
                    errors = validate_url_safe_id(ds_id_str, "datastore id")
                    if errors:
                        logger.error(f"Invalid datastore ID {ds_id_str}: {'; '.join(errors)}")
                        continue
                        
                    config = DatastoreConfig(**ds_data)
                    self._datastores[ds_id_str] = config
                    self._api_key_map[config.api_key] = ds_id_str
                except Exception as e:
                    logger.error(f"Failed to parse datastore {ds_id}: {e}")
            
            self._loaded = True
            logger.info(f"Loaded {len(self._datastores)} datastores from {self.path}")
            
        except Exception as e:
            logger.error(f"Failed to load datastores config: {e}")
    
    def ensure_loaded(self) -> None:
        """Ensure configuration is loaded."""
        if not self._loaded:
            self.load()
    
    def get_datastore(self, datastore_id: str) -> Optional[DatastoreConfig]:
        """Get datastore configuration by ID."""
        self.ensure_loaded()
        return self._datastores.get(str(datastore_id))

    def reload(self) -> None:
        """Force reload configuration from YAML file."""
        self.load()

    def get_all_ids(self) -> List[str]:
        """Get all registered datastore IDs."""
        self.ensure_loaded()
        return list(self._datastores.keys())
    
    def get_all_datastores(self) -> Dict[str, DatastoreConfig]:
        """Get all datastore configurations."""
        self.ensure_loaded()
        return self._datastores.copy()
    
    def validate_api_key(self, api_key: str) -> Optional[str]:
        """
        Validate API key and return associated datastore_id.
        
        Args:
            api_key: The API key to validate
            
        Returns:
            datastore_id if valid, None otherwise
        """
        self.ensure_loaded()
        return self._api_key_map.get(api_key)
    
    def save_api_key(self, datastore_id: str, api_key: str) -> None:
        """
        Save a new API key to the YAML file.
        
        Args:
            datastore_id: The datastore ID
            api_key: The new API key
        """
        ds_id_str = str(datastore_id)
        if not self.path.exists():
            data = {}
        else:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}
        
        # Use flat format only
        if ds_id_str in data:
            data[ds_id_str]["api_key"] = api_key
        else:
            data[ds_id_str] = {
                "session_timeout_seconds": 30,
                "api_key": api_key
            }
        
        with open(self.path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        # Reload to update internal state
        self.load()
        logger.info(f"Saved API key for datastore {datastore_id}")


# Global instance
datastores_config = DatastoresConfigLoader()
