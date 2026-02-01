"""Test cases for SyncInstance heartbeat functionality."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from fustor_agent.runtime.sync import SyncInstance
from fustor_core.models.config import SyncConfig, PusherConfig, SourceConfig, PasswdCredential, FieldMapping
from fustor_core.models.states import SyncState
from fustor_core.exceptions import DriverError


class TestSyncInstanceHeartbeat:
    """Test heartbeat functionality of SyncInstance."""

    @pytest.fixture
    def sync_config(self):
        """Create a mock SyncConfig."""
        return SyncConfig(
            id="test_sync",
            source="test_source",
            sender="test_pusher",
            fields_mapping=[
                FieldMapping(to="events.content", source=["mock_db.mock_table.id:0"])
            ],
            disabled=False
        )

    @pytest.fixture
    def source_config(self):
        """Create a mock SourceConfig."""
        return SourceConfig(
            id="test_source",
            driver="test_driver",
            credential=PasswdCredential(user="test", passwd="test"),
            uri="test://source",
            disabled=False
        )

    @pytest.fixture
    def pusher_config(self):
        """Create a mock PusherConfig."""
        return PusherConfig(
            id="test_pusher",
            driver="test_driver",
            credential=PasswdCredential(user="test", passwd="test"),
            endpoint="http://test.com",
            disabled=False,
            max_retries=2,  # For testing heartbeat error handling
            retry_delay_sec=1
        )

    @pytest.fixture
    def mock_services(self, mocker: MockerFixture):
        """Create mock services for SyncInstance using pytest-mock."""
        mock_bus_service = MagicMock()
        
        mock_bus_instance = MagicMock()
        mock_internal_bus = MagicMock()
        mock_internal_bus.get_events_for = AsyncMock(return_value=[])
        mock_bus_instance.internal_bus = mock_internal_bus
        
        mock_bus_service.get_or_create_bus_for_subscriber = AsyncMock(return_value=(mock_bus_instance, False))

        mock_pusher_driver_service = MagicMock()
        mock_source_driver_service = MagicMock()

        # Pusher driver mock - explicitly define async methods as AsyncMock
        # so they return coroutines that can be awaited.
        mock_pusher_driver_instance = MagicMock()
        mock_pusher_driver_instance.create_session = AsyncMock(return_value={})
        mock_pusher_driver_instance.heartbeat = AsyncMock(return_value={})
        mock_pusher_driver_instance.get_latest_committed_index = AsyncMock(return_value=0)
        mock_pusher_driver_instance.get_sentinel_tasks = AsyncMock(return_value={})
        mock_pusher_driver_instance.submit_sentinel_results = AsyncMock(return_value=True)
        mock_pusher_driver_instance.signal_audit_start = AsyncMock(return_value=True)
        mock_pusher_driver_instance.signal_audit_end = AsyncMock(return_value=True)
        mock_pusher_driver_instance.push = AsyncMock(return_value={})
        mock_pusher_driver_instance.close = AsyncMock(return_value=None)

        # Source driver mock - sync methods should be MagicMock
        mock_source_driver_instance = MagicMock()
        mock_source_driver_instance.get_snapshot_iterator = MagicMock()
        mock_source_driver_instance.get_message_iterator = MagicMock()
        mock_source_driver_instance.perform_sentinel_check = MagicMock()
        mock_source_driver_instance.close = AsyncMock(return_value=None)
        mock_source_driver_instance.is_transient = False
        mock_source_driver_instance.is_position_available.return_value = True

        # Make the service return the mock driver class
        mock_pusher_driver_class = MagicMock(return_value=mock_pusher_driver_instance)
        mock_source_driver_class = MagicMock(return_value=mock_source_driver_instance)

        mock_pusher_driver_service._get_driver_by_type.return_value = mock_pusher_driver_class
        mock_source_driver_service._get_driver_by_type.return_value = mock_source_driver_class

        return {
            'bus_service': mock_bus_service,
            'pusher_driver_service': mock_pusher_driver_service,
            'source_driver_service': mock_source_driver_service,
            'mock_pusher_driver_instance': mock_pusher_driver_instance,
            'mock_source_driver_instance': mock_source_driver_instance
        }

    @pytest.mark.asyncio
    async def test_heartbeat_basic_functionality(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test basic heartbeat functionality."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )
        instance.session_id = "test_session_123"
        
        expected_response = {
            "status": "ok",
            "message": "Session test_session_123 heartbeat updated successfully",
            "suggested_heartbeat_interval_seconds": 15,
            "session_timeout_seconds": 30
        }
        mock_services['mock_pusher_driver_instance'].heartbeat.return_value = expected_response

        result = await instance._send_heartbeat()

        mock_services['mock_pusher_driver_instance'].heartbeat.assert_called_once_with(
            session_id="test_session_123"
        )

        assert result is not None
        assert result["status"] == "ok"
        assert result["suggested_heartbeat_interval_seconds"] == 15
        assert result["session_timeout_seconds"] == 30

    @pytest.mark.asyncio
    async def test_heartbeat_with_no_session_id(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test heartbeat behavior when no session ID is available."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )

        await instance._send_heartbeat()

        mock_services['mock_pusher_driver_instance'].heartbeat.assert_called_once_with(
            session_id=None
        )

    @pytest.mark.asyncio
    async def test_heartbeat_failure_handling(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test heartbeat failure handling."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )
        instance.session_id = "test_session_123"
        
        # Use AsyncMock specifically with side_effect to ensure it behaves as a coro
        mock_services['mock_pusher_driver_instance'].heartbeat = AsyncMock(side_effect=Exception("Network error"))

        with pytest.raises(Exception, match="Network error"):
            await instance._send_heartbeat()

    @pytest.mark.asyncio
    async def test_start_stop_heartbeat_task(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test that heartbeat task is properly started and stopped."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )

        mock_services['mock_pusher_driver_instance'].create_session.return_value = {"session_id": "new_session_id"}
        mock_services['mock_pusher_driver_instance'].heartbeat.return_value = {
            "status": "ok",
            "suggested_heartbeat_interval_seconds": 10
        }
        mock_services['mock_pusher_driver_instance'].get_latest_committed_index.return_value = 0
        mock_services['mock_source_driver_instance'].get_snapshot_iterator.return_value = iter([])
        mock_services['mock_source_driver_instance'].get_message_iterator.return_value = (iter([]), False)

        await instance.start()
        await asyncio.sleep(0)  # Allow event loop to run and set session_id

        assert instance.session_id is not None
        assert instance._heartbeat_task is not None
        assert not instance._stop_heartbeat_event.is_set()

        await instance.stop()

        assert instance.state == SyncState.STOPPED

    @pytest.mark.asyncio
    async def test_heartbeat_loop_with_dynamic_interval(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test heartbeat loop behavior with dynamic interval adjustment."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )
        instance.session_id = "test_session_123"
        
        test_response = {
            "status": "ok",
            "suggested_heartbeat_interval_seconds": 20
        }
        mock_services['mock_pusher_driver_instance'].heartbeat.return_value = test_response

        result = await instance._send_heartbeat()
        
        assert result == test_response
        assert result["suggested_heartbeat_interval_seconds"] == 20

    @pytest.mark.asyncio
    async def test_heartbeat_loop_exit_on_stop_event(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test that heartbeat loop exits when stop event is set."""
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )
        instance.session_id = "test_session_123"
        
        mock_services['mock_pusher_driver_instance'].heartbeat.return_value = {
            "status": "ok",
            "suggested_heartbeat_interval_seconds": 10
        }

        instance._stop_heartbeat_event.set()

        await instance._run_heartbeat_loop()
        
        assert instance._stop_heartbeat_event.is_set()

    @pytest.mark.asyncio
    async def test_heartbeat_failure_sets_error_state_after_max_retries(
        self, 
        sync_config: SyncConfig, 
        source_config: SourceConfig, 
        pusher_config: PusherConfig, 
        mock_services: dict
    ):
        """Test that heartbeat failures after max_retries sets the sync instance to ERROR state."""
        async def mock_heartbeat_error(*args, **kwargs):
            raise DriverError("Heartbeat failed")
            
        mock_services['mock_pusher_driver_instance'].heartbeat.side_effect = mock_heartbeat_error
        mock_services['mock_pusher_driver_instance'].create_session.return_value = {"session_id": "new_session_id"}
        
        instance = SyncInstance(
            id="test_instance",
            agent_id="test_agent",
            config=sync_config,
            source_config=source_config,
            pusher_config=pusher_config,  # max_retries=2
            bus_service=mock_services['bus_service'],
            pusher_driver_service=mock_services['pusher_driver_service'],
            source_driver_service=mock_services['source_driver_service'],
            pusher_schema={"properties": {"events.content": {"type": "object"}}}
        )
        
        async def hanging_message_sync(*args, **kwargs):
            while not instance._stop_heartbeat_event.is_set():
                await asyncio.sleep(0.1)
        
        with patch.object(instance, '_run_message_sync', side_effect=hanging_message_sync):
            await instance.start()
            
            await asyncio.sleep(pusher_config.max_retries * pusher_config.retry_delay_sec + 1)
            
            assert instance.state == SyncState.ERROR
            
            await instance.stop()