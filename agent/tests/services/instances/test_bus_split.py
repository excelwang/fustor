import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import threading

from fustor_agent.services.instances.bus import EventBusService, EventBusInstanceRuntime
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.models.states import EventBusState
from fustor_core.event import InsertEvent

@pytest.fixture
def mock_source_driver_service():
    mock_service = MagicMock()
    mock_driver_instance = MagicMock()
    mock_driver_instance._stop_driver_event = threading.Event()
    mock_driver_instance.is_position_available.return_value = True
    # Dummy iterator
    mock_driver_instance.get_message_iterator.return_value = iter([])
    mock_driver_class = MagicMock(return_value=mock_driver_instance)
    mock_service._get_driver_by_type.return_value = mock_driver_class
    return mock_service

@pytest.fixture
def source_config():
    return SourceConfig(
        driver="mock_driver", 
        uri="mock_uri", 
        credential=PasswdCredential(user="u"), 
        max_queue_size=100
    )

@pytest.fixture
def event_bus_service(source_config, mock_source_driver_service):
    service = EventBusService(
        source_configs={"test_source": source_config},
        source_driver_service=mock_source_driver_service
    )
    service.sync_instance_service = AsyncMock()
    return service

@pytest.mark.asyncio
async def test_bus_split_logic(event_bus_service, source_config):
    """
    Tests that a bus split occurs when one consumer is much faster than another.
    """
    # 1. Create a bus and subscribe two consumers
    # S1 is the slow consumer, S2 is the fast one
    bus_runtime, _ = await event_bus_service.get_or_create_bus_for_subscriber(
        source_id="test_source", source_config=source_config, sync_id="S1", required_position=100, fields_mapping=[]
    )
    
    await event_bus_service.get_or_create_bus_for_subscriber(
        source_id="test_source", source_config=source_config, sync_id="S2", required_position=100, fields_mapping=[]
    )
    
    internal_bus = bus_runtime.internal_bus
    assert internal_bus.get_subscriber_count() == 2
    
    # 2. Fill the buffer to near capacity
    # capacity is 100.
    # We want S1 to be at index 100 (the very first event).
    # Then we put 96 events (100 to 195).
    # slowest_event_deque_position for S1=100 will be 0.
    # len(buffer) will be 96.
    # backlogged_events_count = 95 >= 95.
    
    # First, update S1 to index 100 (which will be the first event we put)
    await internal_bus.update_subscriber_position("S1", 100)
    
    events = [InsertEvent(event_schema="s", table="t", rows=[], fields=[], index=100 + i) for i in range(96)]
    for e in events:
        await internal_bus.put(e)
        
    assert len(internal_bus.buffer) == 96
    
    # 3. Fast consumer (S2) commits at the end
    # S2 is now at 195 (the index of the last event we put)
    
    # Act: Call commit_and_handle_split for S2
    # This should trigger split because S2 (195) is ahead of S1 (100)
    # slowest_event_deque_position = 0 (event index 100)
    # backlogged_events_count = (96 - 1) - 0 = 95
    
    await event_bus_service.commit_and_handle_split(
        bus_id=bus_runtime.id,
        sync_id="S2",
        num_events=96,
        last_consumed_position=195,
        fields_mapping=[]
    )
    
    # 4. Verify results
    # check if remap_sync_to_new_bus was called for S2
    event_bus_service.sync_instance_service.remap_sync_to_new_bus.assert_called_once()
    call_args = event_bus_service.sync_instance_service.remap_sync_to_new_bus.call_args
    sync_id, new_bus_runtime, lost = call_args[0]
    
    assert sync_id == "S2"
    # CRITICAL: Is it a NEW bus or the SAME bus?
    assert new_bus_runtime is not bus_runtime, "Should have created a NEW bus during split!"
