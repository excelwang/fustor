import pytest
import pytest_asyncio
import os
import signal
import time
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport
from pydantic import BaseModel

from fustor_fusion.api.management import router, require_management_key
from fustor_fusion.core.session_manager import SessionManager, SessionInfo
from fustor_fusion.config.unified import fusion_config

@pytest.fixture
def app():
    _app = FastAPI()
    _app.include_router(router, prefix="/api/v1")
    return _app


@pytest_asyncio.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest.fixture(autouse=True)
def mock_fusion_config():
    with patch("fustor_fusion.api.management.fusion_config") as mock:
        mock.fusion.management_api_key = None
        yield mock


@pytest.fixture(autouse=True)
def mock_session_manager():
    with patch("fustor_fusion.api.management.session_manager") as mock:
        mock.get_all_active_sessions = AsyncMock(return_value={})
        mock.queue_command = AsyncMock(return_value=True)
        yield mock


# --- Auth Tests ---

@pytest.mark.asyncio
async def test_auth_no_key_configured(client):
    """If no key is configured, management API should be open."""
    response = await client.get("/api/v1/management/dashboard")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_auth_key_required_but_missing(client, mock_fusion_config):
    """If key is configured, missing header should return 401."""
    mock_fusion_config.fusion.management_api_key = "secret"
    response = await client.get("/api/v1/management/dashboard")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_auth_key_wrong(client, mock_fusion_config):
    """If key is configured, wrong header should return 403."""
    mock_fusion_config.fusion.management_api_key = "secret"
    response = await client.get("/api/v1/management/dashboard", headers={"X-Management-Key": "wrong"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_auth_key_correct(client, mock_fusion_config):
    """If key is configured, correct header should return 200."""
    mock_fusion_config.fusion.management_api_key = "secret"
    response = await client.get("/api/v1/management/dashboard", headers={"X-Management-Key": "secret"})
    assert response.status_code == 200


# --- Dashboard Tests ---

@pytest.mark.asyncio
async def test_dashboard_empty(client):
    """Dashboard should return empty structures if no data."""
    response = await client.get("/api/v1/management/dashboard")
    assert response.status_code == 200
    data = response.json()
    assert data["agents"] == {}
    assert data["sessions"] == {}


@pytest.mark.asyncio
async def test_dashboard_with_sessions(client, mock_session_manager):
    """Dashboard should aggregate sessions by agent."""
    si = MagicMock(spec=SessionInfo)
    si.task_id = "agent-1:pipe-1"
    si.client_ip = "1.2.3.4"
    si.can_realtime = True
    si.created_at = time.monotonic() - 100
    si.last_activity = time.monotonic() - 10
    si.source_uri = "fs:///tmp"
    si.agent_status = {"version": "0.8.17", "role": "leader"}
    
    mock_session_manager.get_all_active_sessions.return_value = {
        "view-1": {"sess-1": si}
    }
    
    response = await client.get("/api/v1/management/dashboard")
    assert response.status_code == 200
    data = response.json()
    
    assert "agent-1" in data["agents"]
    assert data["agents"]["agent-1"]["client_ip"] == "1.2.3.4"
    assert data["agents"]["agent-1"]["status"]["version"] == "0.8.17"
    assert len(data["sessions"]["view-1"]) == 1
    assert data["sessions"]["view-1"][0]["session_id"] == "sess-1"


# --- Command Tests ---

@pytest.mark.asyncio
async def test_dispatch_command_success(client, mock_session_manager):
    """Dispatching a command to an existing agent."""
    si = MagicMock()
    si.task_id = "agent-1:pipe-1"
    mock_session_manager.get_all_active_sessions.return_value = {
        "view-1": {"sess-1": si}
    }
    
    response = await client.post(
        "/api/v1/management/agents/agent-1/command",
        json={"type": "reload_config"}
    )
    assert response.status_code == 200
    assert response.json()["sessions_queued"] == 1
    mock_session_manager.queue_command.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_command_not_found(client, mock_session_manager):
    """Dispatching a command to a non-existent agent."""
    response = await client.post(
        "/api/v1/management/agents/ghost/command",
        json={"type": "reload_config"}
    )
    assert response.status_code == 404


# --- Config Push Tests ---

@pytest.mark.asyncio
async def test_push_config_success(client, mock_session_manager):
    """Pushing config to an agent."""
    si = MagicMock()
    si.task_id = "agent-1:pipe-1"
    mock_session_manager.get_all_active_sessions.return_value = {
        "view-1": {"sess-1": si}
    }
    
    response = await client.post(
        "/api/v1/management/agents/agent-1/config",
        json={"config_yaml": "agent_id: agent-1", "filename": "new.yaml"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    
    # Verify command was queued
    args, kwargs = mock_session_manager.queue_command.call_args
    assert args[2]["type"] == "update_config"
    assert args[2]["config_yaml"] == "agent_id: agent-1"


# --- Upgrade Tests ---

@pytest.mark.asyncio
async def test_upgrade_agent_success(client, mock_session_manager):
    """Triggering agent upgrade."""
    si = MagicMock()
    si.task_id = "agent-1:pipe-1"
    mock_session_manager.get_all_active_sessions.return_value = {
        "view-1": {"sess-1": si}
    }
    
    response = await client.post(
        "/api/v1/management/agents/agent-1/upgrade",
        json={"version": "1.0.0"}
    )
    assert response.status_code == 200
    assert response.json()["target_version"] == "1.0.0"
    
    args, kwargs = mock_session_manager.queue_command.call_args
    assert args[2]["type"] == "upgrade_agent"
    assert args[2]["version"] == "1.0.0"


# --- Fusion Reload Tests ---

@pytest.mark.asyncio
async def test_fusion_reload_success(client):
    """Triggering Fusion self-reload."""
    with patch("os.kill") as mock_kill:
        response = await client.post("/api/v1/management/reload")
        assert response.status_code == 200
        mock_kill.assert_called_once()
        # Should be called with current pid and SIGHUP
        args, _ = mock_kill.call_args
        assert args[0] == os.getpid()
        assert args[1] == signal.SIGHUP
