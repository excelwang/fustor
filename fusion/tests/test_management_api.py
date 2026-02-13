import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import hmac

from fustor_fusion.main import app
from fustor_fusion.config.unified import fusion_config
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion_sdk.interfaces import SessionInfo

client = TestClient(app)

@pytest.fixture
def mock_mgmt_key():
    with patch.object(fusion_config.fusion, "management_api_key", "test-secret-key"):
        yield "test-secret-key"

@pytest.fixture
def open_mgmt():
    with patch.object(fusion_config.fusion, "management_api_key", None):
        yield

def test_auth_required_when_key_configured(mock_mgmt_key):
    """Test that 401/403 is returned when a key is configured but missing/wrong."""
    # Missing header
    response = client.get("/api/v1/management/dashboard")
    assert response.status_code == 401
    assert "X-Management-Key header is required" in response.json()["detail"]

    # Wrong header
    response = client.get("/api/v1/management/dashboard", headers={"X-Management-Key": "wrong-key"})
    assert response.status_code == 403
    assert "Invalid management key" in response.json()["detail"]

    # Correct header
    response = client.get("/api/v1/management/dashboard", headers={"X-Management-Key": mock_mgmt_key})
    assert response.status_code == 200

def test_auth_open_when_no_key_configured(open_mgmt):
    """Test that dashboard is open when no key is configured (backward compat)."""
    response = client.get("/api/v1/management/dashboard")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_dashboard_aggregation():
    """Test that dashboard correctly aggregates sessions into agents."""
    # Setup mock sessions
    sid1 = "sess-1"
    si1 = SessionInfo(
        session_id=sid1, view_id="view-1", task_id="agent-A:task-1",
        last_activity=100.0, created_at=90.0, client_ip="127.0.0.1",
        agent_status={"agent_id": "agent-A", "cpu": 10}
    )
    
    sid2 = "sess-2"
    si2 = SessionInfo(
        session_id=sid2, view_id="view-2", task_id="agent-A:task-2",
        last_activity=105.0, created_at=95.0, client_ip="127.0.0.1"
    )

    with patch.object(session_manager, "get_all_active_sessions", return_value={
        "view-1": {sid1: si1},
        "view-2": {sid2: si2}
    }):
        response = client.get("/api/v1/management/dashboard")
        assert response.status_code == 200
        data = response.json()
        
        assert "agents" in data
        assert "agent-A" in data["agents"]
        agent = data["agents"]["agent-A"]
        assert len(agent["sessions"]) == 2
        assert agent["status"]["cpu"] == 10

def test_agent_command_dispatch():
    """Test queuing a command for an agent."""
    agent_id = "agent-1"
    sid = "sess-1"
    si = SessionInfo(session_id=sid, view_id="v1", task_id=f"{agent_id}:t1", last_activity=1, created_at=1)
    
    with patch.object(session_manager, "get_all_active_sessions", return_value={"v1": {sid: si}}):
        with patch.object(session_manager, "queue_command", return_value=True) as mock_queue:
            payload = {"type": "scan", "path": "/test/path"}
            response = client.post(f"/api/v1/management/agents/{agent_id}/command", json=payload)
            
            assert response.status_code == 200
            assert response.json()["sessions_queued"] == 1
            mock_queue.assert_called_once_with("v1", sid, {"type": "scan", "path": "/test/path", "recursive": True})

def test_agent_config_push():
    """Test pushing config to agent."""
    agent_id = "agent-1"
    sid = "sess-1"
    si = SessionInfo(session_id=sid, view_id="v1", task_id=f"{agent_id}:t1", last_activity=1, created_at=1)
    
    with patch.object(session_manager, "get_all_active_sessions", return_value={"v1": {sid: si}}):
        with patch.object(session_manager, "queue_command", return_value=True) as mock_queue:
            payload = {"config_yaml": "foo: bar", "filename": "test.yaml"}
            response = client.post(f"/api/v1/management/agents/{agent_id}/config", json=payload)
            
            assert response.status_code == 200
            mock_queue.assert_called_once()
            cmd = mock_queue.call_args[0][2]
            assert cmd["type"] == "update_config"
            assert cmd["config_yaml"] == "foo: bar"

def test_agent_config_get_cached():
    """Test retrieving cached config."""
    agent_id = "agent-1"
    sid = "sess-1"
    si = SessionInfo(session_id=sid, view_id="v1", task_id=f"{agent_id}:t1", last_activity=1, created_at=1)
    si.reported_config = "cached: yaml" # Our new field
    
    with patch.object(session_manager, "get_all_active_sessions", return_value={"v1": {sid: si}}):
        response = client.get(f"/api/v1/management/agents/{agent_id}/config")
        assert response.status_code == 200
        assert response.json()["config_yaml"] == "cached: yaml"

def test_agent_config_get_trigger():
    """Test triggering config report when not cached."""
    agent_id = "agent-1"
    sid = "sess-1"
    si = SessionInfo(session_id=sid, view_id="v1", task_id=f"{agent_id}:t1", last_activity=1, created_at=1)
    
    with patch.object(session_manager, "get_all_active_sessions", return_value={"v1": {sid: si}}):
        with patch.object(session_manager, "queue_command", return_value=True) as mock_queue:
            # First trial: pending
            response = client.get(f"/api/v1/management/agents/{agent_id}/config")
            assert response.status_code == 200
            assert response.json()["status"] == "pending"
            
            # Second trial: trigger
            response = client.get(f"/api/v1/management/agents/{agent_id}/config?trigger=true")
            assert response.status_code == 200
            assert response.json()["status"] == "triggered"
            mock_queue.assert_called_once()
            assert mock_queue.call_args[0][2]["type"] == "report_config"
