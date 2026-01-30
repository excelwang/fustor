import pytest
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
    
@pytest.fixture(scope="session", autouse=True)
def register_dummy_route_for_middleware_test():
    """Register a dummy route that uses check_snapshot_status for middleware testing"""
    from fastapi import APIRouter, Depends
    from fustor_fusion.api.views import check_snapshot_status, view_router
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    
    # Define a simple router that explicitly uses the middleware we want to test
    dummy_router = APIRouter()
    
    @dummy_router.get("/status_check")
    async def status_check(
        datastore_id: int = Depends(get_datastore_id_from_api_key)
    ):
        # Explicitly call the dependency function if it's not automatically run by Depends in this context?
        # Standard usage: Depends(check_snapshot_status) in signature.
        # But create_fs_router calls it manually inside the handler usually?
        # Let's match create_fs_router pattern: explicit await
        await check_snapshot_status(datastore_id)
        return {"status": "ok"}
        
    # Register on app to ensure visibility
    # Path: /api/v1/views/test/status_check
    app.include_router(dummy_router, prefix="/api/v1/views/test", tags=["test"])
