
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

# Assuming your main app structure
from fustor_fusion.main import app
from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
from fustor_fusion.view_manager.manager import ViewManager

# Override auth dependency
app.dependency_overrides[get_datastore_id_from_api_key] = lambda: 1

client = TestClient(app)

@pytest.fixture
def mock_view_manager():
    with patch("fustor_fusion.api.consistency.get_cached_view_manager", new_callable=AsyncMock) as mock:
        manager = MagicMock(spec=ViewManager)
        mock.return_value = manager
        yield mock, manager

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_audit_start_endpoint(mock_view_manager):
    """Test that the audit start endpoint calls handle_audit_start on the provider."""
    mock_get, manager = mock_view_manager
    
    provider = AsyncMock()
    # Mock iterator and getter
    manager.get_available_providers.return_value = ["file_directory"]
    manager.get_provider.return_value = provider
    
    response = client.post("/api/v1/pipe/consistency/audit/start")
    
    assert response.status_code == 200
    assert response.json()["status"] == "audit_started"
    provider.handle_audit_start.assert_called_once()

@pytest.mark.asyncio
async def test_audit_end_endpoint(mock_view_manager):
    """Test that the audit end endpoint calls handle_audit_end on the provider."""
    mock_get, manager = mock_view_manager
    
    provider = AsyncMock()
    manager.get_available_providers.return_value = ["file_directory"]
    manager.get_provider.return_value = provider
    
    # Patch queue / processing manager to simulate drained queue
    with patch("fustor_fusion.api.consistency.memory_event_queue") as mock_queue, \
         patch("fustor_fusion.api.consistency.processing_manager") as mock_pm:
        mock_queue.get_queue_size.return_value = 0
        mock_pm.get_inflight_count.return_value = 0
        
        response = client.post("/api/v1/pipe/consistency/audit/end")
    
    assert response.status_code == 200
    assert response.json()["status"] == "audit_ended"
    provider.handle_audit_end.assert_called_once()

@pytest.mark.asyncio
async def test_get_sentinel_tasks_with_suspects(mock_view_manager):
    """Test that sentinel tasks are returned when suspects exist."""
    mock_get, manager = mock_view_manager
    
    provider = AsyncMock()
    provider.get_suspect_list = AsyncMock(return_value={"/file1.txt": 123.0, "/file2.txt": 456.0})
    manager.get_available_providers.return_value = ["file_directory"]
    manager.get_provider.return_value = provider
    
    response = client.get("/api/v1/pipe/consistency/sentinel/tasks")
    
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "suspect_check"
    assert "/file1.txt" in data["paths"]
    assert "/file2.txt" in data["paths"]

@pytest.mark.asyncio
async def test_get_sentinel_tasks_empty(mock_view_manager):
    """Test that empty dict is returned when no suspects."""
    mock_get, manager = mock_view_manager
    
    provider = AsyncMock()
    provider.get_suspect_list = AsyncMock(return_value={})
    manager.get_available_providers.return_value = ["file_directory"]
    manager.get_provider.return_value = provider
    
    response = client.get("/api/v1/pipe/consistency/sentinel/tasks")
    
    assert response.status_code == 200
    assert response.json() == {}

@pytest.mark.asyncio
async def test_submit_sentinel_feedback(mock_view_manager):
    """Test submitting sentinel feedback updates suspects."""
    mock_get, manager = mock_view_manager
    
    provider = AsyncMock()
    manager.get_available_providers.return_value = ["file_directory"]
    manager.get_provider.return_value = provider
    
    response = client.post("/api/v1/pipe/consistency/sentinel/feedback", json={
        "type": "suspect_update",
        "updates": [
            {"path": "/file1.txt", "mtime": 999.0}
        ]
    })
    
    assert response.status_code == 200
    assert response.json()["status"] == "processed"
    provider.update_suspect.assert_called_once_with("/file1.txt", 999.0)
