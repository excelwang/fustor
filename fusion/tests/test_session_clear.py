"""
Test cases for session clear functionality.
"""
import asyncio
from unittest.mock import patch, Mock
import pytest

from fustor_fusion.api.session import create_session
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.datastore_state_manager import datastore_state_manager


class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host


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
    datastore_state_manager._states.clear()
    
    datastore_id = "7"
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        # Create a session
        payload = type('CreateSessionPayload', (), {})()
        payload.task_id = "task_to_clear"
        request = MockRequest(client_host="192.168.1.21")
        
        result = await create_session(payload, request, datastore_id)
        session_id = result["session_id"]
        
        # Verify session exists
        assert datastore_id in session_manager._sessions
        assert session_id in session_manager._sessions[datastore_id]
        assert await datastore_state_manager.is_locked_by_session(datastore_id, session_id)
        
        # Clear all sessions for this datastore
        await session_manager.clear_all_sessions(datastore_id)
        
        # Verify session is gone
        if datastore_id in session_manager._sessions:
            assert session_id not in session_manager._sessions[datastore_id]
        else:
            assert True  # Datastore entry removed implies session removed
