
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

# Assuming your main app structure
from fustor_fusion.main import app
from fustor_fusion.auth.dependencies import get_view_id_from_api_key
from fustor_fusion.view_manager.manager import ViewManager

# Note: No longer setting override here, moving to fixture

@pytest.fixture
def client():
    # Override auth dependency
    from fustor_fusion.auth.dependencies import get_view_id_from_api_key
    app.dependency_overrides[get_view_id_from_api_key] = lambda: "1"
    
    with TestClient(app, headers={"X-API-Key": "test"}) as c:
        yield c
        
    app.dependency_overrides.clear()

@pytest.fixture
def mock_view_manager():
    with patch("fustor_fusion.api.consistency.get_cached_view_manager", new_callable=AsyncMock) as mock:
        manager = MagicMock(spec=ViewManager)
        mock.return_value = manager
        yield mock, manager

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_audit_start_endpoint(client, mock_view_manager):
    """Test that the audit start endpoint calls handle_audit_start on the provider."""
    mock_get, manager = mock_view_manager
    
    driver_instance = AsyncMock()
    # Mock iterator and getter
    manager.get_available_driver_ids.return_value = ["file_directory"]
    manager.get_driver_instance.return_value = driver_instance
    
    response = client.post("/api/v1/pipe/consistency/audit/start")
    if response.status_code != 200:
        print(f"DEBUG: 422 error details: {response.json()}")
    
    assert response.status_code == 200
    assert response.json()["status"] == "audit_started"
    driver_instance.handle_audit_start.assert_called_once()

@pytest.mark.asyncio
async def test_audit_end_endpoint(client, mock_view_manager):
    """Test that the audit end endpoint calls handle_audit_end on the provider."""
    mock_get, manager = mock_view_manager
    
    driver_instance = AsyncMock()
    manager.get_available_driver_ids.return_value = ["file_directory"]
    manager.get_driver_instance.return_value = driver_instance
    
    # Patch queue / processing manager to simulate drained queue
    # Patch runtime_objects to simulate drained queue via pipeline manager
    with patch("fustor_fusion.api.consistency.runtime_objects") as mock_ro:
        mock_pm = MagicMock()
        mock_ro.pipeline_manager = mock_pm
        
        mock_pipeline = AsyncMock()
        mock_pipeline.view_id = "1"
        mock_pipeline.get_dto.return_value = {"queue_size": 0}
        
        mock_pm.get_pipelines.return_value = {'pipe1': mock_pipeline}
        
        response = client.post("/api/v1/pipe/consistency/audit/end")
    
    assert response.status_code == 200
    assert response.json()["status"] == "audit_ended"
    driver_instance.handle_audit_end.assert_called_once()

@pytest.mark.asyncio
async def test_get_sentinel_tasks_with_suspects(client, mock_view_manager):
    """Test that sentinel tasks are returned when suspects exist."""
    mock_get, manager = mock_view_manager
    
    driver_instance = AsyncMock()
    driver_instance.get_suspect_list = AsyncMock(return_value={"/file1.txt": 123.0, "/file2.txt": 456.0})
    manager.get_available_driver_ids.return_value = ["file_directory"]
    manager.get_driver_instance.return_value = driver_instance
    
    response = client.get("/api/v1/pipe/consistency/sentinel/tasks")
    
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "suspect_check"
    assert "/file1.txt" in data["paths"]
    assert "/file2.txt" in data["paths"]

@pytest.mark.asyncio
async def test_get_sentinel_tasks_empty(client, mock_view_manager):
    """Test that empty dict is returned when no suspects."""
    mock_get, manager = mock_view_manager
    
    driver_instance = AsyncMock()
    driver_instance.get_suspect_list = AsyncMock(return_value={})
    manager.get_available_driver_ids.return_value = ["file_directory"]
    manager.get_driver_instance.return_value = driver_instance
    
    response = client.get("/api/v1/pipe/consistency/sentinel/tasks")
    
    assert response.status_code == 200
    assert response.json() == {}

@pytest.mark.asyncio
async def test_submit_sentinel_feedback(client, mock_view_manager):
    """Test submitting sentinel feedback updates suspects."""
    mock_get, manager = mock_view_manager
    
    driver_instance = AsyncMock()
    manager.get_available_driver_ids.return_value = ["file_directory"]
    manager.get_driver_instance.return_value = driver_instance
    
    response = client.post("/api/v1/pipe/consistency/sentinel/feedback", json={
        "type": "suspect_update",
        "updates": [
            {"path": "/file1.txt", "mtime": 999.0}
        ]
    })
    
    assert response.status_code == 200
    assert response.json()["status"] == "processed"
    driver_instance.update_suspect.assert_called_once_with("/file1.txt", 999.0)
