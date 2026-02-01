import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_agent.runtime.sync import SyncInstance
from fustor_core.models.config import SyncConfig, SourceConfig, SenderConfig, PasswdCredential, FieldMapping
from fustor_core.models.states import SyncState

class TestSyncInstanceAuditSentinel:
    """Detailed unit tests for SyncInstance's Trigger Audit and Sentinel actions."""

    @pytest.fixture
    def sync_config(self):
        return SyncConfig(
            id="test_sync",
            source="test_source",
            sender="test_pusher",
            fields_mapping=[FieldMapping(to="events.content", source=["mock_db.mock_table.id:0"])],
            disabled=False
        )

    @pytest.fixture
    def source_config(self):
        return SourceConfig(
            id="test_id",
            driver="fs",
            credential=PasswdCredential(user="test"),
            uri="/tmp/test",
            disabled=False
        )

    @pytest.fixture
    def sender_config(self):
        return SenderConfig(
            id="pusher_id",
            driver="fusion",
            credential=PasswdCredential(user="test"),
            uri="http://fusion",
            disabled=False
        )

    @pytest.fixture
    def instance(self, sync_config, source_config, sender_config):
        bus_service = MagicMock()
        sender_driver_service = MagicMock()
        source_driver_service = MagicMock()
        
        # Mock drivers
        # perform_sentinel_check is usually called via asyncio.to_thread, so it should be a regular mock if used that way,
        # but in sync.py it's called with await asyncio.to_thread(...)
        source_driver_mock = MagicMock()
        source_driver_mock.get_audit_iterator.return_value = iter([])
        source_driver_mock.perform_sentinel_check.return_value = {}
        
        pusher_driver_mock = MagicMock()
        pusher_driver_mock.signal_audit_start = AsyncMock(return_value=True)
        pusher_driver_mock.signal_audit_end = AsyncMock(return_value=True)
        pusher_driver_mock.get_sentinel_tasks = AsyncMock(return_value={})
        pusher_driver_mock.submit_sentinel_results = AsyncMock(return_value=True)

        sender_driver_service._get_driver_by_type.return_value = MagicMock(return_value=pusher_driver_mock)
        source_driver_service._get_driver_by_type.return_value = MagicMock(return_value=source_driver_mock)

        inst = SyncInstance(
            id="test_instance",
            agent_id="agent_1",
            config=sync_config,
            source_config=source_config,
            sender_config=sender_config,
            bus_service=bus_service,
            sender_driver_service=sender_driver_service,
            source_driver_service=source_driver_service,
            sender_schema={"type": "object"}
        )
        
        inst.current_role = 'leader'
        # References for verification
        inst._pusher_mock = pusher_driver_mock
        inst._source_mock = source_driver_mock
        return inst

    @pytest.mark.asyncio
    async def test_trigger_audit_state_transitions(self, instance):
        """Test that triggering audit correctly sets and clears the AUDIT_SYNC state."""
        # Initial state
        assert not (instance.state & SyncState.AUDIT_SYNC)
        
        # We need to mock _run_audit_sync to control its execution
        with patch.object(instance, '_run_audit_sync', wraps=instance._run_audit_sync) as mock_audit:
            await instance.trigger_audit()
            mock_audit.assert_called_once()
            
        # State should be back to normal after await
        assert not (instance.state & SyncState.AUDIT_SYNC)

    @pytest.mark.asyncio
    async def test_trigger_audit_prevent_concurrent(self, instance):
        """Test that audit cannot be triggered if already running."""
        instance.state |= SyncState.AUDIT_SYNC
        
        with patch('fustor_agent.runtime.sync.logger') as mock_logger:
            await instance.trigger_audit()
            mock_logger.warning.assert_any_call("Audit for test_instance already running.")
            assert instance._pusher_mock.signal_audit_start.call_count == 0

    @pytest.mark.asyncio
    async def test_trigger_sentinel_state_transitions(self, instance):
        """Test that triggering sentinel correctly sets and clears the SENTINEL_SWEEP state."""
        assert not (instance.state & SyncState.SENTINEL_SWEEP)
        
        await instance.trigger_sentinel()
        
        # In trigger_sentinel, it calls _run_sentinel_check, which sets/unsets flag
        assert instance._pusher_mock.get_sentinel_tasks.called
        assert not (instance.state & SyncState.SENTINEL_SWEEP)

    @pytest.mark.asyncio
    async def test_trigger_sentinel_prevent_concurrent(self, instance):
        """Test that sentinel cannot be triggered if already running."""
        instance.state |= SyncState.SENTINEL_SWEEP
        
        with patch('fustor_agent.runtime.sync.logger') as mock_logger:
            await instance.trigger_sentinel()
            mock_logger.warning.assert_any_call("Sentinel check for test_instance already running.")
            assert instance._pusher_mock.get_sentinel_tasks.call_count == 0

    @pytest.mark.asyncio
    async def test_trigger_actions_require_leader(self, instance):
        """Test that audit and sentinel triggers require the leader role."""
        instance.current_role = 'follower'
        
        with patch('fustor_agent.runtime.sync.logger') as mock_logger:
            await instance.trigger_audit()
            mock_logger.warning.assert_any_call("Cannot trigger audit for test_instance: not a leader.")
            
            await instance.trigger_sentinel()
            mock_logger.warning.assert_any_call("Cannot trigger sentinel for test_instance: not a leader.")
