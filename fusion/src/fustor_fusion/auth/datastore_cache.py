from typing import Dict, Optional, List, Any
from pydantic import BaseModel
from fustor_registry.api.internal.keys_api import InternalDatastoreConfigResponse

class DatastoreConfig(BaseModel):
    datastore_id: int
    allow_concurrent_push: bool
    session_timeout_seconds: int

class DatastoreConfigCache:
    def __init__(self):
        self._cache: Dict[int, DatastoreConfig] = {}

    def set_cache(self, datastore_configs_data: List[InternalDatastoreConfigResponse]): # Changed type hint
        """
        Sets the entire cache from a list of datastore config data.
        """
        new_cache = {item.datastore_id: item for item in datastore_configs_data} # Store the full InternalDatastoreConfigResponse object
        self._cache = new_cache
        print(f"Datastore config cache updated. Total datastores: {len(self._cache)}")

    def get_datastore_config(self, datastore_id: int) -> Optional[DatastoreConfig]:
        """
        Retrieves the configuration for a given datastore.
        """
        return self._cache.get(datastore_id)

datastore_config_cache = DatastoreConfigCache()
