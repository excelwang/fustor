import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_agent.runtime.sync import SyncInstance
from fustor_core.models.config import SyncConfig, SourceConfig, SenderConfig, PasswdCredential
from fustor_core.models.states import SyncState

class TestSyncLeaderSequence:
    """Tests for the strict Leader duties sequence (Snapshot -> Audit -> Sentinel)."""

    @pytest.fixture
    def sync_instance(self):
        sync_config = SyncConfig(id="t1", source="s1", sender="p1", disabled=False)
        source_config = SourceConfig(id="s1", driver="fs", credential=PasswdCredential(user="u"), uri="/t", disabled=False)
        sender_config = SenderConfig(id="p1", driver="fusion", credential=PasswdCredential(user="u"), uri="h", disabled=False)
        
        bus_service = MagicMock()
        pusher_driver_svc = MagicMock()
        source_driver_svc = MagicMock()

        pusher_driver_svc._get_driver_by_type.return_value = MagicMock()
        source_driver_svc._get_driver_by_type.return_value = MagicMock()

        inst = SyncInstance(
            id="test_seq",
            agent_id="agent_1",
            config=sync_config,
            source_config=source_config,
            sender_config=sender_config,
            bus_service=bus_service,
            sender_driver_service=pusher_driver_svc,
            source_driver_service=source_driver_svc,
            sender_schema={}
        )
        inst.state = SyncState.MESSAGE_SYNC # Ensure not STOPPED
        inst.current_role = 'leader'
        # Mock internal methods to avoid side effects
        inst._run_snapshot_sync = AsyncMock(return_value=True)
        inst._run_audit_loop = AsyncMock()
        inst._run_sentinel_loop = AsyncMock()
        inst.trigger_audit = AsyncMock()
        return inst

    @pytest.mark.asyncio
    async def test_leader_sequence_success_path(self, sync_instance):
        """Test that Audit and Sentinel start only after successful Snapshot."""
        await sync_instance._run_leader_sequence()

        sync_instance._run_snapshot_sync.assert_awaited_once()
        sync_instance._run_audit_loop.assert_called_once()
        sync_instance.trigger_audit.assert_called_once()
        sync_instance._run_sentinel_loop.assert_called_once()

    @pytest.mark.asyncio
    async def test_leader_sequence_snapshot_failure(self, sync_instance):
        """Test that Audit and Sentinel do NOT start if Snapshot fails."""
        sync_instance._run_snapshot_sync.return_value = False

        await sync_instance._run_leader_sequence()

        sync_instance._run_snapshot_sync.assert_awaited_once()
        sync_instance._run_audit_loop.assert_not_called()
        sync_instance.trigger_audit.assert_not_called()
        sync_instance._run_sentinel_loop.assert_not_called()

    @pytest.mark.asyncio
    async def test_leader_sequence_aborts_on_role_change(self, sync_instance):
        """Test that Audit doesn't start if role changes during Snapshot."""
        # Simulate role change while snapshot is running
        async def mock_snapshot_with_role_change():
            sync_instance.current_role = 'follower'
            return True
        
        sync_instance._run_snapshot_sync = AsyncMock(side_effect=mock_snapshot_with_role_change)

        await sync_instance._run_leader_sequence()

        sync_instance._run_audit_loop.assert_not_called()
        sync_instance._run_sentinel_loop.assert_not_called()

    @pytest.mark.asyncio
    async def test_leader_sequence_aborts_on_stop(self, sync_instance):
        """Test that Audit doesn't start if instance is stopped during Snapshot."""
        async def mock_snapshot_with_stop():
            sync_instance.state = SyncState.STOPPED
            return True
        
        sync_instance._run_snapshot_sync = AsyncMock(side_effect=mock_snapshot_with_stop)

        await sync_instance._run_leader_sequence()

        sync_instance._run_audit_loop.assert_not_called()
        sync_instance._run_sentinel_loop.assert_not_called()
