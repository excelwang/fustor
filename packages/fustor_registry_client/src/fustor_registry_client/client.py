import httpx
import os
from typing import Optional, Dict, Any, List

from fustor_common.models import TokenResponse, MessageResponse, DatastoreBase, ApiKeyBase
from fustor_registry_client.models import ClientApiKeyResponse, ClientDatastoreConfigResponse

class RegistryClient:
    def __init__(self, base_url: str, token: Optional[str] = None, client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url
        self.headers = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        if client:
            self.client = client
            self.client.headers.update(self.headers)
        else:
            self.client = httpx.AsyncClient(base_url=self.base_url, headers=self.headers)

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        response = await self.client.request(method, path, **kwargs)
        response.raise_for_status()
        return response.json()

    async def login(self, email: str, password: str) -> TokenResponse:
        data = {"username": email, "password": password}
        response = await self.client.post("/v1/auth/login", data=data)
        response.raise_for_status()
        return TokenResponse(**response.json())

    async def list_datastores(self) -> List[DatastoreBase]:
        response_data = await self._request("GET", "/v1/datastores/")
        return [DatastoreBase(**data) for data in response_data]

    async def create_datastore(self, name: str, meta: Optional[Dict] = None, visible: bool = False, allow_concurrent_push: bool = False, session_timeout_seconds: int = 30) -> DatastoreBase:
        payload = {
            "name": name,
            "meta": meta,
            "visible": visible,
            "allow_concurrent_push": allow_concurrent_push,
            "session_timeout_seconds": session_timeout_seconds
        }
        response_data = await self._request("POST", "/v1/datastores/", json=payload)
        return DatastoreBase(**response_data)

    async def get_datastore(self, datastore_id: int) -> DatastoreBase:
        response_data = await self._request("GET", f"/v1/datastores/{datastore_id}")
        return DatastoreBase(**response_data)

    async def update_datastore(self, datastore_id: int, name: Optional[str] = None, meta: Optional[Dict] = None, visible: Optional[bool] = None, allow_concurrent_push: Optional[bool] = None, session_timeout_seconds: Optional[int] = None) -> DatastoreBase:
        payload = {}
        if name is not None: payload["name"] = name
        if meta is not None: payload["meta"] = meta
        if visible is not None: payload["visible"] = visible
        if allow_concurrent_push is not None: payload["allow_concurrent_push"] = allow_concurrent_push
        if session_timeout_seconds is not None: payload["session_timeout_seconds"] = session_timeout_seconds
        response_data = await self._request("PUT", f"/v1/datastores/{datastore_id}", json=payload)
        return DatastoreBase(**response_data)

    async def delete_datastore(self, datastore_id: int) -> MessageResponse:
        response_data = await self._request("DELETE", f"/v1/datastores/{datastore_id}")
        return MessageResponse(**response_data)

    async def list_api_keys(self) -> List[ApiKeyBase]:
        response_data = await self._request("GET", "/v1/keys/")
        return [ApiKeyBase(**data) for data in response_data]

    async def create_api_key(self, name: str, datastore_id: int) -> ApiKeyBase:
        payload = {"name": name, "datastore_id": datastore_id}
        response_data = await self._request("POST", "/v1/keys/", json=payload)
        return ApiKeyBase(**response_data)

    async def get_api_key(self, key_id: int) -> ApiKeyBase:
        # The original API doesn't have a GET /v1/keys/{key_id} endpoint.
        # For now, we'll simulate by listing and filtering, or raise an error.
        # A better approach would be to add this endpoint to the registry API.
        raise NotImplementedError("GET /v1/keys/{key_id} is not implemented in the Registry API.")

    async def delete_api_key(self, key_id: int) -> MessageResponse:
        response_data = await self._request("DELETE", f"/v1/keys/{key_id}")
        return MessageResponse(**response_data)

    async def get_client_api_keys(self) -> List[ClientApiKeyResponse]:
        response_data = await self._request("GET", "/client/api-keys")
        return [ClientApiKeyResponse(**data) for data in response_data]

    async def get_client_datastores_config(self) -> List[ClientDatastoreConfigResponse]:
        response_data = await self._request("GET", "/client/datastores-config")
        return [ClientDatastoreConfigResponse(**data) for data in response_data]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
