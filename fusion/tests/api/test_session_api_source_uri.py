
import pytest
from unittest.mock import patch, Mock, AsyncMock
from fustor_fusion.api.session import create_session, CreateSessionPayload
from fustor_fusion.api.views import list_view_sessions
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
    pipe_id = "test_pipe_uri"
    
    # Mock config
    config = {
        "allow_concurrent_push": True,
        "session_timeout_seconds": 30,
    }
    
    # Mock pipe and pipe_manager
    mock_pipe = Mock()
    mock_pipe.pipe_id = pipe_id
    mock_pipe.view_ids = [view_id]
    mock_pipe.get_session_role = AsyncMock(return_value="leader")
    
    mock_pipe_manager = Mock()
    mock_pipe_manager.get_pipe = Mock(return_value=mock_pipe)
    
    # Create a mock session info that will be returned by _on_session_created
    mock_session_info = Mock(
        session_id="test-session-id",
        role="leader",
        audit_interval_sec=30,
        sentinel_interval_sec=10,
        task_id="task_with_uri",
        client_ip="127.0.0.1",
        source_uri="file:///tmp/test.txt",
        last_activity=Mock(),
        created_at=Mock(),
        allow_concurrent_push=True,
        session_timeout_seconds=30,
        can_realtime=False
    )
    mock_pipe_manager._on_session_created = AsyncMock(return_value=mock_session_info)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        with patch('fustor_fusion.api.session.runtime_objects') as mock_runtime:
            mock_runtime.pipe_manager = mock_pipe_manager
            
            # 1. Create Session with source_uri
            payload = CreateSessionPayload(
                task_id="task_with_uri",
                client_info={"source_uri": "file:///tmp/test.txt"},
                session_timeout_seconds=30
            )
            request = MockRequest()
            
            # Mock get_pipe_id_from_auth dependency
            with patch('fustor_fusion.api.session.get_pipe_id_from_auth', return_value=pipe_id):
                result = await create_session(payload, request, pipe_id=pipe_id)
                session_id = result["session_id"]
                
                # 2. List Sessions and verify source_uri
                # Mock session_manager to return our session
                mock_session_dict = {session_id: mock_session_info}
                with patch('fustor_fusion.core.session_manager.session_manager') as mock_sm:
                    mock_sm.get_view_sessions = AsyncMock(return_value=mock_session_dict)
                    
                    # Also need to mock runtime_objects in views.py
                    with patch('fustor_fusion.api.views.runtime_objects') as mock_views_runtime:
                        mock_views_runtime.pipe_manager = mock_pipe_manager
                        
                        list_result = await list_view_sessions(view_id, authorized_view_id=view_id)
                        
                        assert list_result["count"] == 1
                        session_data = list_result["active_sessions"][0]
                        
                        assert session_data["session_id"] == session_id
                        assert session_data["source_uri"] == "file:///tmp/test.txt"
                        assert session_data["client_ip"] == "127.0.0.1"

    # Cleanup
    await session_manager.terminate_session(view_id, session_id)
