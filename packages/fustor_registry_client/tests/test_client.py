import pytest
from fastapi import FastAPI, APIRouter, Depends, HTTPException, status
from fastapi.testclient import TestClient
from typing import List, Dict, Any, Optional
from fustor_registry_client.client import RegistryClient
from fustor_registry_client.models import InternalApiKeyResponse, InternalDatastoreConfigResponse
from fustor_common.models import TokenResponse, MessageResponse, DatastoreBase, ApiKeyBase, Password, LoginRequest

# --- Mock FastAPI App for Registry Service ---
mock_app = FastAPI()
mock_router_v1 = APIRouter(prefix="/v1")
mock_internal_router = APIRouter(prefix="/internal")

# Mock data
MOCK_DATASTORES = {
    1: {"name": "test_datastore_1", "visible": False, "meta": {}, "allow_concurrent_push": False, "session_timeout_seconds": 30},
    2: {"name": "test_datastore_2", "visible": True, "meta": {"env": "prod"}, "allow_concurrent_push": True, "session_timeout_seconds": 60},
}
MOCK_API_KEYS = {
    101: {"id": 101, "name": "api_key_1", "key": "key123", "datastore_id": 1},
    102: {"id": 102, "name": "api_key_2", "key": "key456", "datastore_id": 2},
}
MOCK_USERS = {
    "admin@example.com": {"password": "hashed_password", "token": "mock_jwt_token"},
}

import copy

_INITIAL_MOCK_DATASTORES = {
    1: {"name": "test_datastore_1", "visible": False, "meta": {}, "allow_concurrent_push": False, "session_timeout_seconds": 30},
    2: {"name": "test_datastore_2", "visible": True, "meta": {"env": "prod"}, "allow_concurrent_push": True, "session_timeout_seconds": 60},
}
_INITIAL_MOCK_API_KEYS = {
    101: {"id": 101, "name": "api_key_1", "key": "key123", "datastore_id": 1},
    102: {"id": 102, "name": "api_key_2", "key": "key456", "datastore_id": 2},
}
_INITIAL_MOCK_USERS = {
    "admin@example.com": {"password": "hashed_password", "token": "mock_jwt_token"},
}

@pytest.fixture(autouse=True)
def reset_mock_data():
    global MOCK_DATASTORES
    global MOCK_API_KEYS
    global MOCK_USERS

    MOCK_DATASTORES = copy.deepcopy(_INITIAL_MOCK_DATASTORES)
    MOCK_API_KEYS = copy.deepcopy(_INITIAL_MOCK_API_KEYS)
    MOCK_USERS = copy.deepcopy(_INITIAL_MOCK_USERS)
    yield

# Mock Auth Endpoints
@mock_router_v1.post("/auth/login", response_model=TokenResponse)
async def mock_login(login_request: LoginRequest):
    if login_request.username == "admin@example.com" and login_request.password == "admin_password": # Simplified mock password check
        return TokenResponse(access_token=MOCK_USERS[login_request.username]["token"], token_type="bearer")
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

# Mock Datastore Endpoints
@mock_router_v1.get("/datastores/", response_model=List[DatastoreBase])
async def mock_list_datastores():
    return [DatastoreBase(**data) for data in MOCK_DATASTORES.values()]

@mock_router_v1.post("/datastores/", response_model=DatastoreBase)
async def mock_create_datastore(datastore: DatastoreBase):
    new_id = max(MOCK_DATASTORES.keys()) + 1 if MOCK_DATASTORES else 1
    datastore_data = datastore.model_dump()
    datastore_data["id"] = new_id
    MOCK_DATASTORES[new_id] = datastore_data
    return DatastoreBase(**datastore_data)

@mock_router_v1.get("/datastores/{datastore_id}", response_model=DatastoreBase)
async def mock_get_datastore(datastore_id: int):
    if datastore_id not in MOCK_DATASTORES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    return DatastoreBase(**MOCK_DATASTORES[datastore_id])

@mock_router_v1.put("/datastores/{datastore_id}", response_model=DatastoreBase)
async def mock_update_datastore(datastore_id: int, updated_data: Dict[str, Any]):
    if datastore_id not in MOCK_DATASTORES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    MOCK_DATASTORES[datastore_id].update(updated_data)
    return DatastoreBase(**MOCK_DATASTORES[datastore_id])

@mock_router_v1.delete("/datastores/{datastore_id}", response_model=MessageResponse)
async def mock_delete_datastore(datastore_id: int):
    if datastore_id not in MOCK_DATASTORES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    del MOCK_DATASTORES[datastore_id]
    return MessageResponse(message="Datastore deleted successfully")

# Mock API Key Endpoints
@mock_router_v1.get("/keys/", response_model=List[ApiKeyBase])
async def mock_list_api_keys():
    return [ApiKeyBase(**data) for data in MOCK_API_KEYS.values()]

@mock_router_v1.post("/keys/", response_model=ApiKeyBase)
async def mock_create_api_key(api_key: ApiKeyBase):
    new_id = max(MOCK_API_KEYS.keys()) + 1 if MOCK_API_KEYS else 1
    api_key_data = api_key.model_dump()
    api_key_data["id"] = new_id
    MOCK_API_KEYS[new_id] = api_key_data
    return ApiKeyBase(**api_key_data)

@mock_router_v1.delete("/keys/{key_id}", response_model=MessageResponse)
async def mock_delete_api_key(key_id: int):
    if key_id not in MOCK_API_KEYS:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API Key not found")
    del MOCK_API_KEYS[key_id]
    return MessageResponse(message="API Key deleted successfully")

# Mock Internal Endpoints
@mock_internal_router.get("/api-keys", response_model=List[InternalApiKeyResponse])
async def mock_get_internal_api_keys():
    return [InternalApiKeyResponse(key=data["key"], name=data["name"], datastore_id=data["datastore_id"]) for data in MOCK_API_KEYS.values()]

@mock_internal_router.get("/datastores-config", response_model=List[InternalDatastoreConfigResponse])
async def mock_get_internal_datastores_config():
    configs = []
    for ds_id, ds_data in MOCK_DATASTORES.items():
        configs.append(InternalDatastoreConfigResponse(
            datastore_id=ds_id,
            allow_concurrent_push=ds_data["allow_concurrent_push"],
            session_timeout_seconds=ds_data["session_timeout_seconds"]
        ))
    return configs

mock_app.include_router(mock_router_v1)
mock_app.include_router(mock_internal_router)

@pytest.fixture
def test_client():
    with TestClient(mock_app) as client:
        yield client

import httpx
from httpx import Request, Response, AsyncBaseTransport

class TestClientTransport(AsyncBaseTransport):
    def __init__(self, test_client: TestClient):
        self._test_client = test_client

    async def handle_async_request(self, request: Request) -> Response:
        method = request.method
        url = str(request.url)
        headers = dict(request.headers)
        content = request.content

        response = self._test_client.request(
            method,
            url,
            headers=headers,
            content=content,
        )
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=response.content,
            request=request,
        )

@pytest.fixture
def registry_client(test_client: TestClient):
    transport = TestClientTransport(test_client)
    async_client = httpx.AsyncClient(transport=transport, base_url="http://mock-registry")
    return RegistryClient(base_url="http://mock-registry", client=async_client)

@pytest.mark.asyncio
async def test_login(registry_client):
    token_response = await registry_client.login(email="admin@example.com", password="admin_password")
    assert isinstance(token_response, TokenResponse)
    assert token_response.access_token is not None
    assert token_response.token_type == "bearer"

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await registry_client.login(email="admin@example.com", password="wrong_password")
    assert exc_info.value.response.status_code == status.HTTP_401_UNAUTHORIZED

@pytest.mark.asyncio
async def test_list_datastores(registry_client: RegistryClient):
    datastores = await registry_client.list_datastores()
    assert len(datastores) == len(MOCK_DATASTORES)
    assert all(isinstance(ds, DatastoreBase) for ds in datastores)

@pytest.mark.asyncio
async def test_create_datastore(registry_client: RegistryClient):
    new_datastore = await registry_client.create_datastore(name="new_ds", visible=True)
    assert isinstance(new_datastore, DatastoreBase)
    assert new_datastore.name == "new_ds"
    assert new_datastore.visible == True
    # Check if it's added to mock data (TestClient doesn't persist state across requests by default,
    # but our mock app uses global MOCK_DATASTORES, so it will)
    assert new_datastore.id in MOCK_DATASTORES

@pytest.mark.asyncio
async def test_get_datastore(registry_client: RegistryClient):
    datastore = await registry_client.get_datastore(datastore_id=1)
    assert isinstance(datastore, DatastoreBase)
    assert datastore.name == "test_datastore_1"

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await registry_client.get_datastore(datastore_id=999)
    assert exc_info.value.response.status_code == status.HTTP_404_NOT_FOUND

@pytest.mark.asyncio
async def test_update_datastore(registry_client: RegistryClient):
    updated_ds = await registry_client.update_datastore(datastore_id=1, name="updated_name", session_timeout_seconds=100)
    assert isinstance(updated_ds, DatastoreBase)
    assert updated_ds.name == "updated_name"
    assert updated_ds.session_timeout_seconds == 100
    assert MOCK_DATASTORES[1]["name"] == "updated_name"

@pytest.mark.asyncio
async def test_delete_datastore(registry_client: RegistryClient):
    initial_count = len(MOCK_DATASTORES)
    response = await registry_client.delete_datastore(datastore_id=1)
    assert isinstance(response, MessageResponse)
    assert response.message == "Datastore deleted successfully"
    assert len(MOCK_DATASTORES) == initial_count - 1
    assert 1 not in MOCK_DATASTORES

@pytest.mark.asyncio
async def test_list_api_keys(registry_client: RegistryClient):
    api_keys = await registry_client.list_api_keys()
    assert len(api_keys) == len(MOCK_API_KEYS)
    assert all(isinstance(ak, ApiKeyBase) for ak in api_keys)

@pytest.mark.asyncio
async def test_create_api_key(registry_client: RegistryClient):
    new_api_key = await registry_client.create_api_key(name="new_api_key", datastore_id=1)
    assert isinstance(new_api_key, ApiKeyBase)
    assert new_api_key.name == "new_api_key"
    assert new_api_key.datastore_id == 1
    assert new_api_key.id in MOCK_API_KEYS

@pytest.mark.asyncio
async def test_delete_api_key(registry_client: RegistryClient):
    initial_count = len(MOCK_API_KEYS)
    response = await registry_client.delete_api_key(key_id=101)
    assert isinstance(response, MessageResponse)
    assert response.message == "API Key deleted successfully"
    assert len(MOCK_API_KEYS) == initial_count - 1
    assert 101 not in MOCK_API_KEYS

@pytest.mark.asyncio
async def test_get_internal_api_keys(registry_client: RegistryClient):
    internal_keys = await registry_client.get_internal_api_keys()
    assert len(internal_keys) == len(MOCK_API_KEYS)
    assert all(isinstance(ik, InternalApiKeyResponse) for ik in internal_keys)

@pytest.mark.asyncio
async def test_get_internal_datastores_config(registry_client: RegistryClient):
    internal_configs = await registry_client.get_internal_datastores_config()
    assert len(internal_configs) == len(MOCK_DATASTORES)
    assert all(isinstance(ic, InternalDatastoreConfigResponse) for ic in internal_configs)
