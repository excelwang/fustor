# fusion/tests/integration/test_view_lifecycle_api.py
"""
Integration test for View Management API (Start/Stop Lifecycle).
Replaces the deleted test_lifecycle_flow.py.
"""
import pytest
import pytest_asyncio
import asyncio
import os
import yaml
from pathlib import Path
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch, AsyncMock
from fustor_fusion.main import app
from fustor_fusion.runtime_objects import view_managers
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.view_state_manager import view_state_manager

@pytest_asyncio.fixture
async def client():
    # Clear view managers before and after test
    view_managers.clear()
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    view_managers.clear()

@pytest.fixture
def view_config_file(tmp_path):
    """Create a temporary view config file."""
    config_dir = tmp_path / "views"
    config_dir.mkdir()
    
    view_id = "test_view"
    view_config = {
        "id": view_id,
        "datastore_id": "1",
        "driver": "fs",
        "disabled": False,
        "driver_params": {
            "root_path": "/tmp/fustor_test"
        }
    }
    
    config_file = config_dir / f"{view_id}.yaml"
    with open(config_file, "w") as f:
        yaml.dump(view_config, f)
        
    # Patch views_config to use this directory
    from fustor_fusion.config import views_config
    original_dir = views_config.dir
    views_config.dir = Path(config_dir)
    views_config.reload()
    
    yield view_id
    
    # Restore
    views_config.dir = original_dir
    views_config.reload()

@pytest.mark.asyncio
async def test_view_api_start_stop_lifecycle(client, view_config_file):
    """
    Test the full lifecycle of a view via API:
    1. List views (should be empty)
    2. Start view
    3. Verify running
    4. Start again (idempotency)
    5. Stop view
    6. Verify stopped
    """
    view_id = view_config_file
    
    # 1. Initial State
    resp = await client.get("/api/v1/management/views")
    assert resp.status_code == 200
    assert resp.json()["running_views"] == {}
    
    # 2. Start View
    with patch("fustor_fusion_sdk.loaders.load_view") as mock_load:
        MockProvider = AsyncMock()
        MockProviderClass = MagicMock(return_value=MockProvider)
        mock_load.return_value = MockProviderClass
        
        resp = await client.post(f"/api/v1/management/views/{view_id}/start")
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"
        
        # 3. Verify Running
        resp = await client.get("/api/v1/management/views")
        # Management API returns string keys for JSON compatibility
        assert "1" in resp.json()["running_views"]
        assert view_id in resp.json()["running_views"]["1"]
        
        # 4. Idempotency
        resp = await client.post(f"/api/v1/management/views/{view_id}/start")
        assert resp.status_code == 200
        assert resp.json()["status"] == "already_running"
        
        # 5. Stop View
        resp = await client.post(f"/api/v1/management/views/{view_id}/stop")
        assert resp.status_code == 200
        assert resp.json()["status"] == "stopped"
        
        # 6. Verify Stopped
        resp = await client.get("/api/v1/management/views")
        running_views = resp.json()["running_views"]
        assert "1" not in running_views or view_id not in running_views["1"]

@pytest.mark.asyncio
async def test_view_stop_terminates_session(client, view_config_file):
    """Stopping the last view of a datastore should terminate its sessions."""
    view_id = view_config_file
    datastore_id = "1" # Use STRING here to match Fusion's config-based routing
    
    with patch("fustor_fusion_sdk.loaders.load_view") as mock_load:
        MockProvider = AsyncMock()
        MockProvider.initialize = AsyncMock()
        MockProviderClass = MagicMock(return_value=MockProvider)
        mock_load.return_value = MockProviderClass
        
        # Start view
        await client.post(f"/api/v1/management/views/{view_id}/start")
        
        # Manually create a session for this datastore
        session_id = str(os.urandom(8).hex())
        await session_manager.create_session_entry(
            view_id=datastore_id,
            session_id=session_id,
            task_id="test-task",
            client_ip="127.0.0.1"
        )
        assert await session_manager.get_session_info(datastore_id, session_id) is not None
        
        # Stop view
        resp = await client.post(f"/api/v1/management/views/{view_id}/stop")
        assert resp.status_code == 200
        
        # Verify session is terminated
        assert await session_manager.get_session_info(datastore_id, session_id) is None
