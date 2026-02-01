# fusion/tests/integration/test_lifecycle_flow.py
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient

from fustor_fusion.main import app
from fustor_fusion.config.datastores import DatastoreConfig
from fustor_fusion.config.views import ViewConfig
from fustor_fusion.core.session_manager import SessionManager
from fustor_fusion.view_manager.manager import ViewManager

client = TestClient(app)

@pytest.fixture
def mock_managers():
    # Mock global managers to avoid side effects
    with patch("fustor_fusion.runtime_objects.view_managers", {}) as vms, \
         patch("fustor_fusion.core.session_manager.session_manager", SessionManager()) as sm:
        
        # Mock session terminate to verify call
        sm.terminate_session = AsyncMock()
        sm.get_datastore_sessions = AsyncMock(return_value={"sess-1": "mock"})
        
        yield vms, sm

@pytest.fixture
def mock_configs():
    with patch("fustor_fusion.config.datastores_config.get_datastore") as mock_ds, \
         patch("fustor_fusion.config.views_config.get") as mock_view, \
         patch("fustor_fusion.config.views_config.reload") as mock_reload:
        
        # Setup default mocks
        mock_ds.return_value = DatastoreConfig(
            id="ds-1",
            api_key="key", 
            session_timeout_seconds=30
        )
        
        mock_view.return_value = ViewConfig(
            id="view-1",
            datastore_id="1",
            driver="fs" # Will need to mock loader
        )
        
        # Mock scan/get_enabled for list/auto-start logic if needed
        mock_reload.return_value = None
        
        yield mock_ds, mock_view

@pytest.fixture
def mock_loader():
    with patch("fustor_fusion_sdk.loaders.load_view") as mock_load:
        # Create a mock provider class
        mock_provider_instance = AsyncMock()
        mock_provider_instance.initialize = AsyncMock()
        mock_provider_instance.cleanup = AsyncMock()
        mock_provider_instance.on_session_start = AsyncMock()
        
        mock_provider_class = MagicMock(return_value=mock_provider_instance)
        mock_load.return_value = mock_provider_class
        
        yield mock_provider_instance

@pytest.mark.asyncio
async def test_start_stop_view_lifecycle(mock_managers, mock_configs, mock_loader):
    """
    Test the full lifestyle:
    1. Start View -> Registers Provider
    2. Start View (again) -> Idempotent
    3. Stop View -> Unregisters & Checks Session Termination
    """
    view_managers, session_mgr = mock_managers
    mock_provider = mock_loader
    
    # --- 1. Start View ---
    response = client.post("/api/v1/management/views/view-1/start")
    assert response.status_code == 200
    assert response.json()["status"] == "started"
    
    # Verify ViewManager created and provider registered
    assert "1" in view_managers
    assert "view-1" in view_managers["1"].providers
    
    # Verify on_session_start called (because we mocked active session)
    mock_provider.on_session_start.assert_called_with()
    
    # --- 2. Start View Again ---
    response = client.post("/api/v1/management/views/view-1/start")
    assert response.status_code == 200
    assert response.json()["status"] == "already_running"
    
    # --- 3. Stop View ---
    # Since we mocked session_manager, logic should proceed to terminate
    response = client.post("/api/v1/management/views/view-1/stop")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "stopped"
    assert data["should_shutdown"] is True # Only 1 view, so yes
    
    # Verify cleaned up
    mock_provider.cleanup.assert_called_once()
    mock_provider.cleanup.assert_called_once()
    session_mgr.terminate_session.assert_called_with("1", "sess-1")
    assert "1" not in view_managers # Should be removed as empty

@pytest.mark.asyncio
async def test_multiple_views_lifecycle(mock_managers, mock_configs, mock_loader):
    """
    Test multiple views on same datastore:
    stopping one should NOT terminate session.
    """
    view_managers, session_mgr = mock_managers
    mock_ds_get, mock_view_get = mock_configs
    
    # Setup 2 views
    def get_view_side_effect(view_id):
        if view_id == "view-1":
            return ViewConfig(id="view-1", datastore_id="1", driver="fs")
        elif view_id == "view-2":
            return ViewConfig(id="view-2", datastore_id="1", driver="fs")
        return None
    mock_view_get.side_effect = get_view_side_effect
    
    # Start View 1
    client.post("/api/v1/management/views/view-1/start")
    
    # Start View 2
    client.post("/api/v1/management/views/view-2/start")
    
    assert len(view_managers["1"].providers) == 2
    
    # Stop View 1
    response = client.post("/api/v1/management/views/view-1/stop")
    assert response.json()["status"] == "stopped"
    assert response.json()["should_shutdown"] is False
    
    # Verify Session NOT terminated (View 2 still there)
    session_mgr.terminate_session.assert_not_called()
    assert "view-2" in view_managers["1"].providers
    
    # Stop View 2
    response = client.post("/api/v1/management/views/view-2/stop")
    assert response.json()["should_shutdown"] is True
    
    # Verify Session Terminated
    session_mgr.terminate_session.assert_called()
