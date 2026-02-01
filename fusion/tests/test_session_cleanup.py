import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta

from fustor_fusion.core.session_manager import SessionManager, SessionInfo
from fustor_fusion.in_memory_queue import InMemoryEventQueue
from fustor_fusion.datastore_state_manager import DatastoreStateManager, DatastoreState
from fustor_core.event import InsertEvent

@pytest.fixture
def mock_in_memory_queue():
    """Mocks the global InMemoryEventQueue instance."""
    mock = AsyncMock(spec=InMemoryEventQueue)
    mock._queues = {}
    mock._positions = {}
    mock._lock = asyncio.Lock() # Provide a real lock if internal methods use it
    mock.clear_datastore_data = AsyncMock()
    mock.get_queue_size.return_value = 1 # Configure return value
    return mock

@pytest.fixture
def mock_datastore_state_manager():
    """Mocks the global DatastoreStateManager instance."""
    mock = AsyncMock(spec=DatastoreStateManager)
    mock._states = {}
    mock._lock = asyncio.Lock() # Provide a real lock
    mock.unlock_for_session = AsyncMock()
    mock.clear_state = AsyncMock()
    return mock

@pytest.fixture(autouse=True)
def patch_globals(mock_in_memory_queue, mock_datastore_state_manager, mocker):
    """Patches global instances with mocks."""
    mocker.patch('fustor_fusion.core.session_manager.memory_event_queue', mock_in_memory_queue)
    mocker.patch('fustor_fusion.datastore_state_manager.datastore_state_manager', mock_datastore_state_manager)
    mocker.patch('fustor_fusion.in_memory_queue.memory_event_queue', mock_in_memory_queue) # Also patch in_memory_queue itself
    mocker.patch('fustor_fusion.datastore_state_manager.datastore_state_manager', mock_datastore_state_manager) # Also patch datastore_state_manager itself

@pytest.mark.asyncio
async def test_last_session_cleanup_clears_all_datastore_data(
    mock_in_memory_queue: AsyncMock,
    mock_datastore_state_manager: AsyncMock,
    mocker
):
    """
    Tests that when the last session for a datastore is removed,
    all associated data in InMemoryEventQueue and DatastoreStateManager is cleared.
    """
    # Arrange
    datastore_id = 123
    session_id_1 = "session-1"
    session_id_2 = "session-2"
    task_id_1 = "task-1"
    
    # Setup SessionManager
    session_manager = SessionManager(default_session_timeout=0.1) # Short timeout for quick testing
    
    # Mock DatastoreStateManager and InMemoryEventQueue internal states for assertion
    mock_datastore_state_manager._states = {
        datastore_id: DatastoreState(datastore_id=datastore_id, status="ACTIVE", locked_by_session_id=session_id_1)
    }
    mock_in_memory_queue._queues = {datastore_id: asyncio.Queue()}
    mock_in_memory_queue._positions = {(datastore_id, task_id_1): 10}
    
    # Create two sessions for the same datastore
    session_info_1 = await session_manager.create_session_entry(datastore_id, session_id_1, task_id=task_id_1, session_timeout_seconds=0.1)
    session_info_2 = await session_manager.create_session_entry(datastore_id, session_id_2, session_timeout_seconds=0.1)
    
    # Add some dummy events to the queue
    await mock_in_memory_queue.add_event(datastore_id, InsertEvent(event_schema="s", table="t", rows=[{'id': 1}], fields=["id"]), task_id_1)
    assert mock_in_memory_queue.get_queue_size(datastore_id) == 1
    
    # Act & Assert: Terminate the first session - data should NOT be cleared yet
    await session_manager.terminate_session(datastore_id, session_id_1)
    assert datastore_id in session_manager._sessions # Datastore entry still exists
    assert session_id_2 in session_manager._sessions[datastore_id] # Second session still there
    mock_datastore_state_manager.unlock_for_session.assert_called_with(datastore_id, session_id_1)
    mock_in_memory_queue.clear_datastore_data.assert_not_called()
    
    # Act & Assert: Terminate the second (last) session - data SHOULD be cleared
    await session_manager.terminate_session(datastore_id, session_id_2)
    assert datastore_id not in session_manager._sessions # Datastore entry removed
    mock_datastore_state_manager.unlock_for_session.assert_called_with(datastore_id, session_id_2)
    mock_in_memory_queue.clear_datastore_data.assert_called_once_with(datastore_id)
    
    # Verify that the mock states are cleared (reflecting the calls)
    mock_in_memory_queue.clear_datastore_data.assert_called_once_with(datastore_id)
    
    # Clean up background tasks
    session_info_1.cleanup_task.cancel()
    session_info_2.cleanup_task.cancel()
    try:
        await session_info_1.cleanup_task
        await session_info_2.cleanup_task
    except asyncio.CancelledError:
        pass
