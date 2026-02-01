import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fustor_agent.runtime.sync import SyncInstance, SyncState
from fustor_core.exceptions import DriverError
from fustor_core.models.config import SyncConfig, SourceConfig, PusherConfig, ApiKeyCredential
from fustor_core.event import EventBase, InsertEvent

# Mock dependencies
@pytest.fixture
def mock_bus_service():
    return AsyncMock()

@pytest.fixture
def mock_pusher_driver_service():
    with patch('fustor_agent.services.drivers.sender_driver.SenderDriverService') as mock_service:
        mock_driver_class = MagicMock()
        mock_driver_class.return_value.push = AsyncMock()
        mock_driver_class.return_value.create_session = AsyncMock()
        mock_service.return_value._get_driver_by_type.return_value = mock_driver_class
        yield mock_service.return_value

@pytest.fixture
def mock_source_driver_service():
    with patch('fustor_agent.services.drivers.source_driver.SourceDriverService') as mock_service:
        mock_driver_class = MagicMock()
        mock_service.return_value._get_driver_by_type.return_value = mock_driver_class
        yield mock_service.return_value

@pytest.fixture
def mock_sync_config():
    return SyncConfig(source="test-source", pusher="test-pusher", disabled=False)

@pytest.fixture
def mock_source_config():
    return SourceConfig(driver="mock", uri="mock://test", credential=ApiKeyCredential(key="mock-key"), max_queue_size=100)

@pytest.fixture
def mock_pusher_config():
    return PusherConfig(driver="mock", endpoint="http://mock.com", credential=ApiKeyCredential(key="mock-key"), batch_size=10)

@pytest.fixture
def mock_pusher_schema():
    return {}

@pytest.fixture
def sync_instance(
    mock_sync_config,
    mock_source_config,
    mock_pusher_config,
    mock_bus_service,
    mock_pusher_driver_service,
    mock_source_driver_service,
    mock_pusher_schema
):
    instance = SyncInstance(
        id="test-sync",
        agent_id="test-agent",
        config=mock_sync_config,
        source_config=mock_source_config,
        pusher_config=mock_pusher_config,
        bus_service=mock_bus_service,
        pusher_driver_service=mock_pusher_driver_service,
        source_driver_service=mock_source_driver_service,
        pusher_schema=mock_pusher_schema
    )
    # Mock the actual driver instances created by SyncInstance
    instance.pusher_driver_instance = MagicMock()
    instance.pusher_driver_instance.push = AsyncMock()
    instance.pusher_driver_instance.create_session = AsyncMock()
    instance.source_driver_instance = MagicMock()
    return instance

@pytest.mark.asyncio
async def test_snapshot_stops_on_419_error(sync_instance):
    # Arrange
    # Mock the source_driver_instance to yield a single EventBase object
    mock_event = InsertEvent(event_schema="test", table="data", rows=[{"id": 1, "value": "test"}], fields=["id", "value"])
    # The iterator should yield individual EventBase objects
    sync_instance.source_driver_instance.get_snapshot_iterator.return_value = iter([mock_event])

    # Mock the pusher_driver_instance.push to raise a DriverError on the first call,
    # and then return a normal response for subsequent calls (though the second call shouldn't happen)
    sync_instance.pusher_driver_instance.push.side_effect = [
        DriverError("HTTP Error 419 while pushing to pusher. A newer sync session has been started. This snapshot task is now obsolete and should stop."),
        AsyncMock(return_value={}) # This second mock should not be reached if the break works
    ]

    # Mock create_session to return a session_id
    sync_instance.pusher_driver_instance.create_session.return_value = {"session_id": "mock-session-id"}
    sync_instance.session_id = "mock-session-id" # Manually set for _run_snapshot_sync context

    # Act
    # Start the snapshot task directly
    snapshot_task = asyncio.create_task(sync_instance._run_snapshot_sync())

    # Wait for the task to complete or be cancelled
    await asyncio.sleep(0.1) # Give it a moment to run and hit the error

    # Assert
    # The task should eventually stop (either by completing or being cancelled internally)
    # We expect it to stop gracefully due to the error handling
    assert snapshot_task.done()
    
    # Check that the push method was called
    sync_instance.pusher_driver_instance.push.assert_called_once()

    # The state should reflect that the snapshot is no longer running
    # The finally block in _run_snapshot_sync should clear the SNAPSHOT_SYNC flag
    assert SyncState.SNAPSHOT_SYNC not in sync_instance.state
    assert sync_instance.info == "快照同步任务已清理。" # Check the info message

    # Optionally, check the exception if the task finished with one (it should not re-raise DriverError)
    try:
        await snapshot_task
    except Exception as e:
        pytest.fail(f"Snapshot task raised an unexpected exception: {e}")
