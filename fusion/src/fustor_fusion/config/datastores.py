# fusion/src/fustor_fusion/config/datastores.py
"""
Datastore configuration loader from YAML files.
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional
from pydantic import BaseModel, field_validator

from fustor_common.paths import get_fustor_home_dir
from .validators import validate_url_safe_id

logger = logging.getLogger(__name__)


class DatastoreConfig(BaseModel):
    """Configuration for a single datastore."""
    name: str
    session_timeout_seconds: int = 30
    allow_concurrent_push: bool = False
    api_key: str


class DatastoresConfigLoader:
    """
    Loads and manages datastore configurations from YAML file.
    
    Config file format:
    ```yaml
    datastores:
      1:
        name: research-data
        session_timeout_seconds: 30
        api_key: fk_abc123xyz
    ```
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = get_fustor_home_dir() / "datastores-config.yaml"
        self.path = Path(config_path)
        self._datastores: Dict[int, DatastoreConfig] = {}
        self._api_key_map: Dict[str, int] = {}  # api_key -> datastore_id
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
            
            for ds_id, ds_data in data.get("datastores", {}).items():
                try:
                    config = DatastoreConfig(**ds_data)
                    int_id = int(ds_id)
                    self._datastores[int_id] = config
                    self._api_key_map[config.api_key] = int_id
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
    
    def get_datastore(self, datastore_id: int) -> Optional[DatastoreConfig]:
        """Get datastore configuration by ID."""
        self.ensure_loaded()
        return self._datastores.get(datastore_id)
    
    def get_all_datastores(self) -> Dict[int, DatastoreConfig]:
        """Get all datastore configurations."""
        self.ensure_loaded()
        return self._datastores.copy()
    
    def validate_api_key(self, api_key: str) -> Optional[int]:
        """
        Validate API key and return associated datastore_id.
        
        Args:
            api_key: The API key to validate
            
        Returns:
            datastore_id if valid, None otherwise
        """
        self.ensure_loaded()
        return self._api_key_map.get(api_key)
    
    def save_api_key(self, datastore_id: int, api_key: str) -> None:
        """
        Save a new API key to the YAML file.
        
        Args:
            datastore_id: The datastore ID
            api_key: The new API key
        """
        if not self.path.exists():
            data = {"datastores": {}}
        else:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {"datastores": {}}
        
        if "datastores" not in data:
            data["datastores"] = {}
        
        # Handle both int and str keys (YAML may use either)
        existing_key = None
        for k in [datastore_id, str(datastore_id)]:
            if k in data["datastores"]:
                existing_key = k
                break
        
        if existing_key is not None:
            data["datastores"][existing_key]["api_key"] = api_key
        else:
            # Use int key for new entries
            data["datastores"][datastore_id] = {
                "name": f"datastore-{datastore_id}",
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
