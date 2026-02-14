import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.runtime.session_bridge import PipeSessionBridge

@pytest.fixture
def mock_pipe():
    pipe = MagicMock()
    pipe.view_id = "global-view"
    pipe.id = "pipe-1"  # Forest mode
    pipe.on_session_created = AsyncMock()
    pipe.get_session_role = AsyncMock(return_value="leader")
    # Mock get_view_handler to return a handler
    mock_handler = AsyncMock()
    # Default behavior: standard leader
    mock_handler.resolve_session_role.return_value = {"role": "leader"}
    pipe.get_view_handler.return_value = mock_handler
    
    # Force locking behavior for tests
    pipe.allow_concurrent_push = False
    return pipe

@pytest.fixture
def mock_session_manager():
    sm = AsyncMock()
    sm.create_session_entry = AsyncMock()
    return sm

@pytest.mark.asyncio
async def test_create_session_delegation(mock_pipe, mock_session_manager):
    """Test that SessionBridge delegates election to handler.resolve_session_role."""
    bridge = PipeSessionBridge(mock_pipe, mock_session_manager)
    
    # Setup handler response
    mock_handler = mock_pipe.get_view_handler.return_value
    mock_handler.resolve_session_role.return_value = {
        "role": "leader", 
        "election_key": "view:pipe-1"
    }
    
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        mock_vsm.lock_for_session = AsyncMock()
        
        # 1. Create session
        await bridge.create_session(
            task_id="task-1",
            session_id="sess-1"
        )
        
        # 2. Verify delegation
        mock_handler.resolve_session_role.assert_called_with("sess-1", pipe_id="pipe-1")
        
        # 3. Verify locking on returned election key
        mock_vsm.lock_for_session.assert_called_with("view:pipe-1", "sess-1")

@pytest.mark.asyncio
async def test_create_session_follower(mock_pipe, mock_session_manager):
    """Test that SessionBridge respects 'follower' role from handler."""
    bridge = PipeSessionBridge(mock_pipe, mock_session_manager)
    
    mock_handler = mock_pipe.get_view_handler.return_value
    mock_handler.resolve_session_role.return_value = {
        "role": "follower", 
        "election_key": "view:pipe-1"
    }
    
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        mock_vsm.lock_for_session = AsyncMock()
        
        await bridge.create_session(task_id="task-1", session_id="sess-1")
        
        # Verify NO locking occurred
        mock_vsm.lock_for_session.assert_not_called()
