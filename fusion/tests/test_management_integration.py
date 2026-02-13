import pytest
import pytest_asyncio
import asyncio
import os
import signal
import time
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

# Fusion side
from fustor_fusion.api.management import router as management_router
from fustor_fusion.api.session import session_router
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.view_state_manager import view_state_manager

# Agent side
from fustor_agent.runtime.agent_pipe import AgentPipe

# Global mark for asyncio tests
pytestmark = pytest.mark.asyncio(loop_scope="function")

@pytest.fixture
def fusion_app():
    app = FastAPI()
    # Mock authentication for session router to return a fixed view_id
    from fustor_fusion.auth.dependencies import get_view_id_from_api_key
    app.dependency_overrides[get_view_id_from_api_key] = lambda: "view1"
    
    app.include_router(management_router, prefix="/api/v1")
    app.include_router(session_router, prefix="/api/v1/pipe/session")
    return app

@pytest_asyncio.fixture
async def client(fusion_app):
    # Reset global managers
    await session_manager.clear_all_sessions("view1")
    view_state_manager._states.clear()
    
    async with AsyncClient(transport=ASGITransport(app=fusion_app), base_url="http://test") as ac:
        yield ac

async def test_full_management_command_flow(fusion_app, client):
    """
    Integration Test:
    1. Agent creates a session via Fusion API.
    2. Fusion Management API is used to send a 'reload_config' command.
    3. Agent performs a heartbeat and receives the command.
    4. Verify Agent executes the command (mocked os.kill).
    """
    # --- Step 1: Agent Registration ---
    task_id = "agent1:pipe1"
    reg_resp = await client.post("/api/v1/pipe/session/", json={
        "task_id": task_id,
        "client_info": {"source_uri": "fs:///tmp"},
        "session_timeout_seconds": 30
    })
    assert reg_resp.status_code == 200
    session_id = reg_resp.json()["session_id"]
    
    # --- Step 2: Fusion sends command via Management API ---
    cmd_resp = await client.post("/api/v1/management/agents/agent1/command", json={
        "type": "reload_config"
    })
    assert cmd_resp.status_code == 200
    assert cmd_resp.json()["sessions_queued"] == 1
    
    # --- Step 3: Agent Heartbeat ---
    with patch("os.kill") as mock_kill:
        with patch("fustor_agent.__version__", "0.8.17"):
            hb_resp = await client.post(f"/api/v1/pipe/session/{session_id}/heartbeat", json={
                "can_realtime": True,
                "agent_status": {
                    "agent_id": "agent1",
                    "version": "0.8.17",
                    "pipe_id": "pipe1",
                    "state": "RUNNING"
                }
            })
            assert hb_resp.status_code == 200
            data = hb_resp.json()
            assert "commands" in data
            assert len(data["commands"]) == 1
            assert data["commands"][0]["type"] == "reload_config"
            
            # --- Step 4: Agent processes command ---
            mock_source = MagicMock()
            mock_sender = MagicMock()
            pipe = AgentPipe(
                pipe_id="pipe1",
                task_id=task_id,
                config={"source": "s1", "sender": "sn1"},
                source_handler=mock_source,
                sender_handler=mock_sender
            )
            
            await pipe._handle_commands(data["commands"])
            
            # Verify Agent triggered the reload (SIGHUP to its own process)
            mock_kill.assert_called_once_with(os.getpid(), signal.SIGHUP)

async def test_management_dashboard_integration(fusion_app, client):
    """
    Verify that the Dashboard correctly reflects Agent status after heartbeats.
    """
    task_id = "agent-multi:p1"
    reg_resp = await client.post("/api/v1/pipe/session/", json={"task_id": task_id})
    session_id = reg_resp.json()["session_id"]
    
    # Send heartbeat with detailed status
    await client.post(f"/api/v1/pipe/session/{session_id}/heartbeat", json={
        "can_realtime": True,
        "agent_status": {
            "version": "1.2.3",
            "uptime_seconds": 123.4,
            "events_pushed": 5000
        }
    })
    
    # Check Dashboard
    dash_resp = await client.get("/api/v1/management/dashboard")
    assert dash_resp.status_code == 200
    dash = dash_resp.json()
    
    assert "agent-multi" in dash["agents"]
    agent_info = dash["agents"]["agent-multi"]
    assert agent_info["status"]["version"] == "1.2.3"
    assert agent_info["status"]["events_pushed"] == 5000
