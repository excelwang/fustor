import httpx
from typing import List, Dict, Any
from ..config import ingestor_config
from fustor_registry.api.internal.keys_api import InternalDatastoreConfigResponse # Moved import to top

class RegisterServiceClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=self.base_url)

    async def get_all_api_keys(self) -> List[Dict[str, Any]]:
        """
        Fetches all active API keys and their associated datastore_id from the register service.
        """
        try:
            response = await self.client.get("/internal/api-keys")
            response.raise_for_status()  # Raise an exception for 4xx/5xx responses
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching API keys: {e}")
            return None
        except httpx.RequestError as e:
            print(f"Request error fetching API keys: {e}")
            return None

    async def get_all_datastores_config(self) -> List[InternalDatastoreConfigResponse]:
        """
        Fetches all datastore configurations from the register service.
        """
        try:
            response = await self.client.get("/internal/datastores-config")
            response.raise_for_status()
            # Parse the list of dictionaries into a list of InternalDatastoreConfigResponse objects
            return [InternalDatastoreConfigResponse(**item) for item in response.json()]
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching datastore configs: {e}")
            return None
        except httpx.RequestError as e:
            print(f"Request error fetching datastore configs: {e}")
            return None

fustor_registry_client = RegisterServiceClient(ingestor_config.FUSTOR_REGISTER_SERVICE_URL)