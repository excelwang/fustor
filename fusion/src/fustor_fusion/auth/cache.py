from typing import Dict, Optional, List, Any
from fustor_fusion_sdk.interfaces import ApiKeyCacheInterface # Import the interface

class ApiKeyCache(ApiKeyCacheInterface): # Inherit from the interface
    def __init__(self):
        self._cache: Dict[str, int] = {}

    def set_cache(self, api_keys_data: List[Dict[str, Any]]):
        """
        Sets the entire cache from a list of API key data.
        Expected format: [{'key': '...', 'datastore_id': 1}, ...]
        """
        new_cache = {item['key']: item['datastore_id'] for item in api_keys_data if 'key' in item and 'datastore_id' in item}
        self._cache = new_cache
    def get_datastore_id(self, api_key: str) -> Optional[int]:
        """
        Retrieves the datastore_id for a given API key.
        """
        return self._cache.get(api_key)

api_key_cache = ApiKeyCache()
