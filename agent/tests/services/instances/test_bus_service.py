import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import asyncio
import threading

from fustor_agent.services.instances.bus import EventBusService, EventBusInstanceRuntime
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.models.states import EventBusState
from fustor_core.exceptions import DriverError, TransientSourceBufferFullError
from fustor_core.event import InsertEvent

@pytest.fixture
def mock_source_driver_service():
    mock_service = MagicMock()
    mock_driver_instance = MagicMock()
    mock_driver_instance._stop_driver_event = threading.Event()
    # Mock the new interface that returns only an iterator
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
        max_queue_size=10
    )

@pytest.fixture
def event_bus_service(source_config, mock_source_driver_service):
    service = EventBusService(
        source_configs={"test_source": source_config},
        source_driver_service=mock_source_driver_service
    )
    # Mock the dependency that is set via set_dependencies
    service.sync_instance_service = AsyncMock()
    return service

@pytest.mark.asyncio
async def test_get_or_create_bus_reuses_existing(event_bus_service, source_config):
    """Tests that an existing, suitable bus is reused."""
    # First call creates the bus. The real producer loop will run but will finish
    # quickly because the mock driver's iterator is empty.
    bus1, lost1 = await event_bus_service.get_or_create_bus_for_subscriber(
        source_id="test_source", source_config=source_config, sync_id="sync1", required_position=0, fields_mapping=[]
    )

    # Mock the internal bus method for the check
    bus1.internal_bus.can_subscribe = MagicMock(return_value=True)

    # Second call should reuse the existing bus.
    bus2, lost2 = await event_bus_service.get_or_create_bus_for_subscriber(
        source_id="test_source", source_config=source_config, sync_id="sync2", required_position=0, fields_mapping=[]
    )

    assert bus2 is bus1
    assert not lost1
    assert not lost2

@pytest.mark.asyncio
async def test_bus_producer_loop_and_transient_error(source_config, mock_source_driver_service):
    """Unit tests the EventBusInstanceRuntime producer loop's error handling for transient sources."""
    source_config.max_queue_size = 1 # Set a small queue to trigger the error

    mock_driver_instance = MagicMock()
    mock_driver_instance._stop_driver_event = threading.Event()
    # The mock iterator that will produce more events than the queue can handle
    def mock_iterator(*args, **kwargs):
        yield InsertEvent(event_schema="s", table="t", rows=[{"id": 1}], fields=["id"], index=1)
        yield InsertEvent(event_schema="s", table="t", rows=[{"id": 2}], fields=["id"], index=2)
    
    # The mock driver must return only the iterator (new format)
    mock_driver_instance.get_message_iterator.return_value = mock_iterator()
    mock_source_driver_service._get_driver_by_type.return_value.return_value = mock_driver_instance

    bus_runtime = EventBusInstanceRuntime(
        source_id="test_source",
        source_config=source_config,
        source_signature=("mock_driver", "mock_uri", PasswdCredential(user="u")),
        source_driver_service=mock_source_driver_service,
        initial_start_position=1
    )

    # Run the producer loop, which should fail with TransientSourceBufferFullError
    await bus_runtime._produce_loop()

    assert bus_runtime.state == EventBusState.ERROR
    assert "event buffer is filled up" in bus_runtime.info
    assert len(bus_runtime.internal_bus.buffer) == 1
    assert bus_runtime.internal_bus.buffer[0].index == 1

@pytest.mark.asyncio
async def test_driver_error_propagates_to_bus_state(source_config, mock_source_driver_service):
    """
    Tests that a DriverError raised from the driver's iterator
    propagates to the EventBus, setting its state to ERROR.
    """
    # Arrange
    error_message = "Stopping driver to prevent data loss."
    error_to_raise = DriverError(error_message)

    # Configure the mock driver to raise an error when get_message_iterator is called
    mock_driver_instance = mock_source_driver_service._get_driver_by_type.return_value.return_value
    mock_driver_instance.get_message_iterator.side_effect = error_to_raise

    # Create the bus runtime instance directly to test the _produce_loop
    bus_runtime = EventBusInstanceRuntime(
        source_id="test_source",
        source_config=source_config,
        source_signature=("mock_driver", "mock_uri", PasswdCredential(user="u")),
        source_driver_service=mock_source_driver_service,
        initial_start_position=0
    )

    # Act
    # Run the producer loop. It should catch the DriverError and set the state.
    await bus_runtime._produce_loop()

    # Assert
    # 1. The bus state should be ERROR
    assert bus_runtime.state == EventBusState.ERROR
    
    # 2. The bus info should contain the error message
    assert "因驱动错误而停止" in bus_runtime.info # "Stopped due to driver error"
    assert error_message in bus_runtime.info

    # 3. The internal MemoryEventBus should also be marked as failed
    assert bus_runtime.internal_bus.failed is True
    assert error_message in bus_runtime.internal_bus.error_message
