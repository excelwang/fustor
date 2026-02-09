# agent/tests/runtime/test_agent_pipe_role_switch.py
"""
Tests for AgentPipe role transitions (Leader <-> Follower).
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_agent.runtime.agent_pipe import AgentPipe

@pytest.mark.timeout(10)
class TestAgentRoleSwitch:
    
    @pytest.mark.asyncio
    async def test_leader_to_follower_cancels_tasks(self, mock_source, mock_sender, pipe_config):
        """When role changes from leader to follower, leader-specific tasks should be cancelled."""
        mock_sender.role = "leader"
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])

        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        # Mock leader tasks
        snapshot_started = asyncio.Event()
        async def mock_snapshot():
            snapshot_started.set()
            pipe._set_state(pipe.state | PipeState.SNAPSHOT_SYNC)
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                pipe._set_state(pipe.state & ~PipeState.SNAPSHOT_SYNC)
                raise
        pipe._run_snapshot_sync = mock_snapshot
        
        await pipe.start()
        await snapshot_started.wait()
        
        # Verify it's in SNAPSHOT_SYNC
        assert pipe.state & PipeState.SNAPSHOT_SYNC
        assert pipe._snapshot_task is not None
        
        # Change role via heartbeat
        mock_sender.role = "follower"
        
        # Wait for heartbeat and role change task to complete, and control loop to set PAUSED
        # Limit wait to 5 seconds to avoid infinite hangs
        for _ in range(500):
            if pipe.current_role == "follower" and (pipe.state & PipeState.PAUSED):
                break
            await asyncio.sleep(0.01)
        
        # Assertions
        assert pipe.current_role == "follower", f"Expected follower role, got {pipe.current_role}"
        # Snapshot task should have been cancelled
        assert pipe._snapshot_task is None or pipe._snapshot_task.done()
        # State should reflect follower standby (PAUSED)
        assert pipe.state & PipeState.PAUSED
        # Snapshot flag should be gone
        assert not (pipe.state & PipeState.SNAPSHOT_SYNC)
        
        await pipe.stop()

    @pytest.mark.asyncio
    async def test_follower_to_leader_starts_sync(self, mock_source, mock_sender, pipe_config):
        """When role changes from follower to leader, leader sequence should start."""
        mock_sender.role = "follower"
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])

        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        await pipe.start()
        
        # Wait for it to start as follower
        for _ in range(100):
            if pipe.current_role == "follower" and (pipe.state & PipeState.PAUSED):
                break
            await asyncio.sleep(0.01)
        
        # Verify initial follower state
        assert pipe.current_role == "follower"
        assert pipe.state & PipeState.PAUSED
        
        # Switch to leader
        mock_sender.role = "leader"
        
        # Wait for control loop to detect change and run snapshot
        for _ in range(100):
            if mock_source.snapshot_calls >= 1:
                break
            await asyncio.sleep(0.01)
            
        # Assertions
        assert pipe.current_role == "leader"
        # Snapshot should have been called
        assert mock_source.snapshot_calls >= 1
        
        await pipe.stop()
