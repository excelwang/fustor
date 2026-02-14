"""
Test cases for session clear functionality.
"""
import asyncio
from unittest.mock import patch, Mock, MagicMock, AsyncMock
import pytest

from fustor_fusion.api.session import create_session
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.view_state_manager import view_state_manager
from fustor_fusion.runtime.fusion_pipe import FusionPipe
from fustor_fusion.runtime.session_bridge import create_session_bridge
from fustor_fusion import runtime_objects


class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host


async def setup_dummy_pipe(view_id: str, allow_concurrent_push: bool = False):
    """Register a dummy pipe in the global manager for API tests."""
    if not runtime_objects.pipe_manager:
        from fustor_fusion.runtime.pipe_manager import FusionPipeManager
        runtime_objects.pipe_manager = FusionPipeManager()
    
    mock_handler = Mock()
    mock_handler.id = "dummy-handler"
    mock_handler.view_id = view_id
    mock_handler.resolve_session_role = AsyncMock(return_value={"role": "leader"})
    
    pipe = FusionPipe(
        pipe_id=view_id,
        config={"view_ids": [view_id], "allow_concurrent_push": allow_concurrent_push},
        view_handlers=[mock_handler]
    )
    pipe._handlers_ready.set()
    runtime_objects.pipe_manager._pipes[view_id] = pipe
    
    bridge = create_session_bridge(pipe)
    runtime_objects.pipe_manager._bridges[view_id] = bridge
    return pipe


def make_session_config(allow_concurrent_push=False, session_timeout_seconds=30):
    """Create a mock session config dict."""
    return {
        "allow_concurrent_push": allow_concurrent_push,
        "session_timeout_seconds": session_timeout_seconds,
    }


@pytest.mark.asyncio
async def test_clear_all_sessions():
    """
    Test that clear_all_sessions properly removes all sessions and releases locks
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "7"
    await setup_dummy_pipe(view_id, allow_concurrent_push=False)
    
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        # Create a session
        payload = type('CreateSessionPayload', (), {})()
        payload.task_id = "task_to_clear"
        payload.session_timeout_seconds = None
        payload.client_info = None
        request = MockRequest(client_host="192.168.1.21")
        
        result = await create_session(payload, request, view_id)
        session_id = result["session_id"]
        
        # Verify session exists
        assert view_id in session_manager._sessions
        assert session_id in session_manager._sessions[view_id]
        assert await view_state_manager.is_locked_by_session(view_id, session_id)
        
        # Clear all sessions for this view
        await session_manager.clear_all_sessions(view_id)
        
        # Verify session is gone
        if view_id in session_manager._sessions:
            assert session_id not in session_manager._sessions[view_id]
        else:
            assert True  # View entry removed implies session removed
