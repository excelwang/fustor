import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from fustor_fusion.main import app

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.mark.asyncio
async def test_get_component_health(client):
    with patch("fustor_fusion.api.health.runtime_objects.pipe_manager") as mock_pm:
        # Mock pipe
        mock_pipe = MagicMock()
        mock_pipe.id = "pipe-1"
        mock_pipe.state = "RUNNING"
        
        # Mock handlers
        mock_h1 = MagicMock()
        # configuring mock attributes
        mock_h1.schema_name = "view_fs"
        
        mock_pipe._view_handlers = {"h1": mock_h1}
        # default disabled handlers to empty
        mock_pipe._disabled_handlers = []
        
        mock_pipe._last_agent_status = {
            "agent_id": "test-agent",
            "component_health": {
                "source": {"status": "ok"},
                "sender": {"status": "ok"},
                "data_plane": {"status": "ok"}
            }
        }
        
        mock_pm.get_pipes.return_value = {"pipe-1": mock_pipe}
        
        response = client.get("/api/v1/health/components")
        assert response.status_code == 200
        data = response.json()
        
        assert "pipe-1" in data
        p_data = data["pipe-1"]
        assert p_data["state"] == "RUNNING"
        assert p_data["agent_id"] == "test-agent"
        assert p_data["agent_health"]["source"]["status"] == "ok"
        assert p_data["handlers"]["h1"]["enabled"] is True
        assert p_data["handlers"]["h1"]["type"] == "view_fs"

@pytest.mark.asyncio
async def test_get_component_health_empty(client):
    with patch("fustor_fusion.api.health.runtime_objects.pipe_manager") as mock_pm:
        mock_pm.get_pipes.return_value = {}
        
        response = client.get("/api/v1/health/components")
        assert response.status_code == 200
        assert response.json() == {}
