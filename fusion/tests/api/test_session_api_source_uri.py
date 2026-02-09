
import pytest
from unittest.mock import patch, Mock
from fustor_fusion.api.session import create_session, list_sessions, CreateSessionPayload
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.view_state_manager import view_state_manager

class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host

@pytest.mark.asyncio
async def test_session_source_uri_exposure():
    # Setup
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    view_id = "test_view_uri"
    
    # Mock config
    config = {
        "allow_concurrent_push": True,
        "session_timeout_seconds": 30,
    }
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        # 1. Create Session with source_uri
        payload = CreateSessionPayload(
            task_id="task_with_uri",
            client_info={"source_uri": "file:///tmp/test.txt"},
            session_timeout_seconds=30
        )
        request = MockRequest()
        
        result = await create_session(payload, request, view_id)
        session_id = result["session_id"]
        
        # 2. List Sessions and verify source_uri
        list_result = await list_sessions(view_id)
        
        assert list_result["count"] == 1
        session_data = list_result["active_sessions"][0]
        
        assert session_data["session_id"] == session_id
        assert session_data["source_uri"] == "file:///tmp/test.txt"
        assert session_data["client_ip"] == "127.0.0.1"

    # Cleanup
    await session_manager.terminate_session(view_id, session_id)
