from typing import Dict, Optional, List, Any
from fustor_common.models import DatastoreConfig
from fustor_registry_client.models import InternalDatastoreConfigResponse

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
