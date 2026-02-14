# agent/tests/runtime/test_agent_pipe_lifecycle.py
"""
Lifecycle and role management tests for AgentPipe.
These tests verify the state transitions and sequence of the pipe.
"""
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe

@pytest.mark.timeout(5)
class TestAgentPipeLifecycle:
    """Test AgentPipe lifecycle and role transitions."""

    @pytest.fixture
    def agent_pipe(self, mock_source, mock_sender, pipe_config):
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = MagicMock()
        # Prevent busy loop in message sync task
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])
        
        return AgentPipe(
            pipe_id="test-pipe",
            config=pipe_config,
            source_handler=mock_source,
            sender_handler=mock_sender,
            event_bus=mock_bus
        )

    @pytest.mark.asyncio
    async def test_pipe_start_and_wait_for_role(self, agent_pipe, mock_sender):
        """Test starting the pipe and waiting in follower mode."""
        await agent_pipe.start()
        
        # Initially follower, should be in PAUSED/standby mode
        await asyncio.sleep(0.2)
        assert "Follower mode" in agent_pipe.info
        assert PipeState.PAUSED in agent_pipe.state
        
        await agent_pipe.stop()
        assert mock_sender.session_closed

    @pytest.mark.asyncio
    async def test_pipe_leader_transition(self, agent_pipe, mock_sender, mock_source):
        """Test transition from follower to leader."""
        # Start as follower
        await agent_pipe.start()
        
        # Transisition to leader - update mock state for next heartbeat
        mock_sender.role = "leader"
        
        # Poll until leader sequence completes (snapshot done + message sync started).
        # We check `is_realtime_ready` which is set inside bus_message_sync
        # and is more stable than the PipeState flag.
        reached_target = False
        for _ in range(80):  # Up to 4 seconds
            if (agent_pipe.current_role == "leader" 
                and agent_pipe.is_realtime_ready
                and mock_source.snapshot_calls > 0):
                reached_target = True
                break
            await asyncio.sleep(0.05)
        
        try:
            assert agent_pipe.current_role == "leader"
            assert mock_source.snapshot_calls > 0
            assert len(mock_sender.batches) >= 2
            assert reached_target, (
                f"Pipe did not reach expected state. "
                f"role={agent_pipe.current_role}, realtime_ready={agent_pipe.is_realtime_ready}, "
                f"snapshot_calls={mock_source.snapshot_calls}, state={agent_pipe.state}"
            )
        finally:
            await agent_pipe.stop()

    @pytest.mark.asyncio
    async def test_manual_triggers(self, agent_pipe, mock_sender, mock_source):
        """Test manual audit and sentinel triggers."""
        agent_pipe.current_role = "leader"
        agent_pipe.session_id = "test-session"
        
        # We need to set state to include RUNNING for triggers to work
        agent_pipe.state = PipeState.RUNNING
        
        await agent_pipe.trigger_audit()
        await asyncio.sleep(0.2)
        
        assert mock_source.audit_calls > 0
        
        # Sentinel check
        await agent_pipe.trigger_sentinel()
        # verify no crash at least
        
        await agent_pipe.stop()

    @pytest.mark.asyncio
    async def test_message_to_audit_transition(self, agent_pipe, mock_sender, mock_source):
        """Pipe should transition to AUDIT_PHASE when audit is triggered."""
        agent_pipe.current_role = "leader"
        agent_pipe.session_id = "test-session"
        agent_pipe._set_state(PipeState.RUNNING | PipeState.MESSAGE_SYNC)
        
        # Mock audit to take some time
        async def slow_audit():
            await asyncio.sleep(0.5)
            yield {"id": 1}
        mock_source.get_audit_iterator = MagicMock(return_value=slow_audit())
        mock_sender.send_batch = AsyncMock(return_value=(True, {}))
        
        # Trigger audit
        await agent_pipe.trigger_audit()
        
        # It runs in background, so wait a bit
        await asyncio.sleep(0.1)
        
        # While audit is running, state should include AUDIT_PHASE
        assert PipeState.AUDIT_PHASE in agent_pipe.state
        assert PipeState.MESSAGE_SYNC in agent_pipe.state
        
        # After audit, it should return to MESSAGE_SYNC (implicit, but check no crash)
        await agent_pipe.stop()

    @pytest.mark.asyncio
    async def test_stop_during_snapshot(self, agent_pipe, mock_sender, mock_source):

        """Test stopping the pipe while it is in snapshot sync phase."""
        # Slow down snapshot iterator to simulate work
        async def slow_iter():
            for i in range(10):
                yield {"index": i}
                await asyncio.sleep(0.1)
                
        with patch.object(mock_source, 'get_snapshot_iterator', return_value=slow_iter()):
            mock_sender.role = "leader"
            await agent_pipe.start()
            
            # Wait for snapshot task to start (poll)
            found_snapshot = False
            for _ in range(20):
                if agent_pipe.state & PipeState.SNAPSHOT_SYNC:
                    found_snapshot = True
                    break
                await asyncio.sleep(0.1)
                
            assert found_snapshot, "Pipe did not enter SNAPSHOT_SYNC state"
            
            await agent_pipe.stop()
            assert agent_pipe.state == PipeState.STOPPED
            assert mock_sender.session_closed
