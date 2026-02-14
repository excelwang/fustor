# fusion/tests/test_on_command_fallback.py

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_fusion.api.views import FallbackDriverWrapper
from fustor_fusion.api.on_command import on_command_fallback
from fustor_fusion.runtime.fusion_pipe import FusionPipe
from fustor_fusion.runtime.session_bridge import PipeSessionBridge
from fustor_fusion import runtime_objects

@pytest.mark.asyncio
async def test_fallback_driver_wrapper_success():
    """Test that wrapper passes through successful calls."""
    mock_driver = AsyncMock()
    mock_driver.get_data_view.return_value = {"status": "ok"}
    
    wrapper = FallbackDriverWrapper(mock_driver, "test-view")
    result = await wrapper.get_data_view(path="/")
    
    assert result == {"status": "ok"}
    mock_driver.get_data_view.assert_called_once()

@pytest.mark.asyncio
async def test_fallback_driver_wrapper_triggers_fallback():
    """Test that wrapper checks triggering fallback on exception."""
    mock_driver = AsyncMock()
    mock_driver.get_data_view.side_effect = Exception("View not ready")
    
    wrapper = FallbackDriverWrapper(mock_driver, "test-view")
    
    # Mock the on_command_fallback module function
    with patch("fustor_fusion.api.views.on_command_fallback", new_callable=AsyncMock) as mock_fallback:
        mock_fallback.return_value = {"fallback": "data"}
        
        result = await wrapper.get_data_view(path="/")
        
        assert result == {"fallback": "data"}
        mock_fallback.assert_called_once_with("test-view", {"path": "/"})

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
    
    # Mock pipe manager
    with patch("fustor_fusion.runtime_objects.pipe_manager") as mock_pm:
        mock_pm.get_pipe.return_value = mock_pipe
        mock_pm.get_bridge.return_value = mock_bridge
        
        # Execute
        result = await on_command_fallback(view_id, {"path": "/foo"})
        
        # Verify
        assert result["path"] == "/foo"
        assert len(result["entries"]) == 1
        assert result["entries"][0]["name"] == "foo.txt"
        assert result["metadata"]["source"] == "remote_fallback"
        assert result["metadata"]["agent_id"] == "agent-1"
        
        # Verify bridge call
        mock_bridge.send_command_and_wait.assert_called_once()
        call_args = mock_bridge.send_command_and_wait.call_args
        assert call_args.kwargs["session_id"] == session_id
        assert call_args.kwargs["command"] == "remote_scan"
        assert call_args.kwargs["params"]["path"] == "/foo"

@pytest.mark.asyncio
async def test_on_command_fallback_no_session():
    """Test error when no session available."""
    view_id = "view-1"
    
    mock_pipe = MagicMock()
    mock_pipe.leader_session = None # No leader
    
    mock_bridge = AsyncMock()
    mock_bridge.get_all_sessions.return_value = {} # No active sessions
    
    with patch("fustor_fusion.runtime_objects.pipe_manager") as mock_pm:
        mock_pm.get_pipe.return_value = mock_pipe
        mock_pm.get_bridge.return_value = mock_bridge
        
        with pytest.raises(RuntimeError, match="No active sessions"):
            await on_command_fallback(view_id, {})
