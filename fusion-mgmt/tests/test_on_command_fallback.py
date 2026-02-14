import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_fusion_mgmt.on_command import on_command_fallback
from fustor_fusion.runtime.session_bridge import PipeSessionBridge
from fustor_fusion import runtime_objects
from fastapi import HTTPException

@pytest.mark.asyncio
async def test_on_command_fallback_logic():
    """Test the core fallback orchestration logic."""
    view_id = "view-1"
    session_id = "session-123"
    
    # Setup Mocks
    mock_pipe = MagicMock()
    mock_pipe.id = "pipe-1"
    mock_pipe.leader_session = session_id
    
    mock_bridge = AsyncMock(spec=PipeSessionBridge)
    mock_bridge.send_command_and_wait.return_value = {
        "files": [{"name": "foo.txt"}],
        "agent_id": "agent-1"
    }
    
    # Explicitly create a MagicMock for PipeManager
    mock_pm = MagicMock()
    mock_pm.resolve_pipes_for_view.return_value = ["pipe-1"]
    mock_pm.get_pipe.return_value = mock_pipe
    mock_pm.get_bridge.return_value = mock_bridge
    
    # Use patch.object on the module to be absolutely sure
    with patch.object(runtime_objects, "pipe_manager", mock_pm):
        # Execute
        result = await on_command_fallback(view_id, {"path": "/foo"})
        
        # Verify
        assert result["path"] == "/foo"
        assert len(result["entries"]) == 1
        assert result["entries"][0]["name"] == "foo.txt"
        assert result["metadata"]["source"] == "remote_fallback"
        
        # Verify bridge call
        mock_bridge.send_command_and_wait.assert_called_once()

@pytest.mark.asyncio
async def test_on_command_fallback_no_session():
    """Test error when no session available."""
    view_id = "view-1"
    
    mock_pm = MagicMock()
    mock_pm.resolve_pipes_for_view.return_value = ["pipe-1"]
    mock_pm.get_pipe.return_value = MagicMock(leader_session=None)
    mock_pm.get_bridge.return_value = AsyncMock(get_all_sessions=AsyncMock(return_value={}))
    
    with patch.object(runtime_objects, "pipe_manager", mock_pm):
        with pytest.raises(HTTPException, match="Fallback command failed"):
            await on_command_fallback(view_id, {})
