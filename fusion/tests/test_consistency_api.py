
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

# Assuming your main app structure
from fustor_fusion.main import app
from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
from fustor_fusion.parsers.manager import ParserManager

# Override auth dependency
app.dependency_overrides[get_datastore_id_from_api_key] = lambda: 1

client = TestClient(app)

@pytest.fixture
def mock_parser_manager():
    with patch("fustor_fusion.api.consistency.get_cached_parser_manager", new_callable=AsyncMock) as mock:
        manager = MagicMock(spec=ParserManager)
        mock.return_value = manager
        yield manager

@pytest.mark.asyncio
async def test_audit_start_endpoint(mock_parser_manager):
    # Setup mock parser
    mock_parser = AsyncMock()
    mock_parser_manager.get_file_directory_parser.return_value = mock_parser

    # Call API
    response = client.post("/api/v1/ingest/consistency/audit/start")
    
    assert response.status_code == 200
    assert response.json() == {"status": "audit_started"}
    
    # Verify manager call (Note: get_cached_parser_manager is mocked at module level patch)
    # The patch target "fustor_fusion.api.consistency.get_cached_parser_manager" is called with datastore_id=1
    # Check if the mocked function was called correctly
    import fustor_fusion.api.consistency
    fustor_fusion.api.consistency.get_cached_parser_manager.assert_called_with(1)
    
    # Verify parser method call
    mock_parser.handle_audit_start.assert_called_once()

@pytest.mark.asyncio
async def test_audit_end_endpoint(mock_parser_manager):
    # Setup mock parser
    mock_parser = AsyncMock()
    mock_parser_manager.get_file_directory_parser.return_value = mock_parser

    # Call API
    response = client.post("/api/v1/ingest/consistency/audit/end")
    
    assert response.status_code == 200
    assert response.json() == {"status": "audit_ended"}
    
    # Verify manager call
    mock_parser_manager.get_file_directory_parser.assert_called()
    # Verify parser method call
    mock_parser.handle_audit_end.assert_called_once()

@pytest.mark.asyncio
async def test_sentinel_tasks_endpoint(mock_parser_manager):
    mock_parser = AsyncMock()
    mock_parser_manager.get_file_directory_parser.return_value = mock_parser
    # Mock suspect list return
    mock_parser.get_suspect_list.return_value = {"/foo/bar": 12345.0}

    response = client.get("/api/v1/ingest/consistency/sentinel/tasks")
    
    assert response.status_code == 200
    expected = {
        "type": "suspect_check",
        "paths": ["/foo/bar"],
        "source_id": 1 # From overridden dependency
    }
    assert response.json() == expected
    
@pytest.mark.asyncio
async def test_sentinel_feedback_endpoint(mock_parser_manager):
    mock_parser = AsyncMock()
    mock_parser_manager.get_file_directory_parser.return_value = mock_parser
    
    payload = {
        "type": "suspect_update",
        "updates": [{"path": "/foo", "mtime": 123}]
    }
    response = client.post("/api/v1/ingest/consistency/sentinel/feedback", json=payload)
    
    assert response.status_code == 200
    assert response.json() == {"status": "processed", "count": 1}
    
    mock_parser.update_suspect.assert_called_with("/foo", 123.0)
