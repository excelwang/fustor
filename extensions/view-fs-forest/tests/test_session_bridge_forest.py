import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.runtime.session_bridge import PipeSessionBridge

@pytest.fixture
def mock_pipe():
    pipe = MagicMock()
    pipe.view_id = "global-view"
    pipe.pipe_id = "pipe-1"  # Forest mode
    pipe.on_session_created = AsyncMock()
    pipe.get_session_role = AsyncMock(return_value="leader")
    # Mock get_view_handler to return a handler with a driver
    mock_handler = MagicMock()
    mock_driver = MagicMock()
    mock_driver.use_scoped_session_leader = True # Default for this fixture
    mock_handler.manager.driver = mock_driver
    pipe.get_view_handler.return_value = mock_handler
    return pipe

@pytest.fixture
def mock_session_manager():
    sm = AsyncMock()
    sm.create_session_entry = AsyncMock()
    return sm

@pytest.mark.asyncio
async def test_create_session_forest_scoped_leader(mock_pipe, mock_session_manager):
    """Test that SessionBridge uses {view_id}:{pipe_id} for leader election in Forest mode."""
    bridge = PipeSessionBridge(mock_pipe, mock_session_manager)
    
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        mock_vsm.try_become_leader = AsyncMock(return_value=True)
        mock_vsm.set_authoritative_session = AsyncMock()
        mock_vsm.lock_for_session = AsyncMock()
        
        # 1. Create session
        await bridge.create_session(
            task_id="task-1",
            session_id="sess-1"
        )
        
        # 2. Verify VSM was called with SCOPED view_id
        expected_election_id = "global-view:pipe-1"
        mock_vsm.try_become_leader.assert_any_call(expected_election_id, "sess-1")
        mock_vsm.set_authoritative_session.assert_called_with(expected_election_id, "sess-1")

@pytest.mark.asyncio
async def test_create_session_standard_view(mock_pipe, mock_session_manager):
    """Test that SessionBridge uses standard view_id when pipe_id is missing (legacy mode)."""
    # Even if pipe_id exists, if driver doesn't opt-in, use standard view_id
    mock_handler = mock_pipe.get_view_handler.return_value
    mock_handler.manager.driver.use_scoped_session_leader = False
    
    bridge = PipeSessionBridge(mock_pipe, mock_session_manager)
    
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        mock_vsm.try_become_leader = AsyncMock(return_value=True)
        mock_vsm.set_authoritative_session = AsyncMock()
        mock_vsm.lock_for_session = AsyncMock() # Mock this too as loop might hit it
        
        await bridge.create_session(task_id="task-1", session_id="sess-1")
        
        # Verify VSM called with RAW view_id (Standard behavior)
        mock_vsm.try_become_leader.assert_any_call("global-view", "sess-1")
