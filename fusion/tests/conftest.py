import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import asyncio

from fustor_fusion.main import app
from fustor_fusion.api.session import get_datastore_id_from_api_key

@pytest_asyncio.fixture(scope="function")
async def async_client() -> AsyncClient:
    def override_get_datastore_id():
        return 1 # Mock datastore_id

    app.dependency_overrides[get_datastore_id_from_api_key] = override_get_datastore_id

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        client.headers["X-API-Key"] = "test-api-key"
        yield client
    
    app.dependency_overrides.clear()