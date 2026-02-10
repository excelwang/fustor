import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import asyncio

from fustor_fusion.main import app
from fustor_fusion.api.session import get_view_id_from_api_key  # Using view_id internally

@pytest.fixture(scope="session", autouse=True)
def prevent_logging_reconfiguration():
    """Prevent the application from reconfiguring logging during tests."""
    with patch("fustor_core.common.logging_config.setup_logging"):
        yield

@pytest_asyncio.fixture(scope="function")
async def async_client() -> AsyncClient:
    from fustor_fusion.auth.dependencies import get_view_id_from_api_key
    
    def override_get_view_id():
        return "1" # Mock view_id as string
    
    app.dependency_overrides[get_view_id_from_api_key] = override_get_view_id
    
    # Ensure lifespan is triggered so routes are registered
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            client.headers["X-API-Key"] = "test-api-key"
            yield client
    
@pytest.fixture(scope="session", autouse=True)
def register_dummy_route_for_middleware_test():
    """Register a dummy route that uses make_readiness_checker for middleware testing"""
    from fastapi import APIRouter, Depends
    from fustor_fusion.api.views import view_router
    from fustor_fusion.auth.dependencies import get_view_id_from_api_key
    
    # Define a simple router that explicitly uses the middleware we want to test
    dummy_router = APIRouter()
    
    # Create a specialized checker logic for 'test_driver' using the real factory
    from fustor_fusion.api.views import make_readiness_checker
    
    # Usage of factory ensures we test the same logic as production
    test_checker = make_readiness_checker("test_driver")

    @dummy_router.get("/status_check")
    async def status_check(
        view_id: str = Depends(get_view_id_from_api_key)
    ):
        await test_checker(view_id)
        return {"status": "ok"}
        
    # Register on app to ensure visibility
    # Path: /api/v1/views/test/status_check
    app.include_router(dummy_router, prefix="/api/v1/views/test", tags=["test"])
