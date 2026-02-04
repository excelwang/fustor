# agent/tests/runtime/test_agent_pipeline_role_switch.py
"""
Tests for AgentPipeline role transitions (Leader <-> Follower).
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_core.pipeline import PipelineState
from fustor_agent.runtime.agent_pipeline import AgentPipeline
from fustor_agent.runtime.agent_pipeline import AgentPipeline

@pytest.mark.timeout(10)
class TestAgentRoleSwitch:
    
    @pytest.mark.asyncio
    async def test_leader_to_follower_cancels_tasks(self, mock_source, mock_sender, pipeline_config):
        """When role changes from leader to follower, leader-specific tasks should be cancelled."""
        mock_sender.role = "leader"
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])

        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        # Mock leader tasks
        snapshot_started = asyncio.Event()
        async def mock_snapshot():
            snapshot_started.set()
            pipeline._set_state(pipeline.state | PipelineState.SNAPSHOT_SYNC)
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                pipeline._set_state(pipeline.state & ~PipelineState.SNAPSHOT_SYNC)
                raise
        pipeline._run_snapshot_sync = mock_snapshot
        
        await pipeline.start()
        await snapshot_started.wait()
        
        # Verify it's in SNAPSHOT_SYNC
        assert pipeline.state & PipelineState.SNAPSHOT_SYNC
        assert pipeline._snapshot_task is not None
        
        # Change role via heartbeat
        mock_sender.role = "follower"
        
        # Wait for heartbeat and role change task to complete, and control loop to set PAUSED
        # Limit wait to 5 seconds to avoid infinite hangs
        for _ in range(500):
            if pipeline.current_role == "follower" and (pipeline.state & PipelineState.PAUSED):
                break
            await asyncio.sleep(0.01)
        
        # Assertions
        assert pipeline.current_role == "follower", f"Expected follower role, got {pipeline.current_role}"
        # Snapshot task should have been cancelled
        assert pipeline._snapshot_task is None or pipeline._snapshot_task.done()
        # State should reflect follower standby (PAUSED)
        assert pipeline.state & PipelineState.PAUSED
        # Snapshot flag should be gone
        assert not (pipeline.state & PipelineState.SNAPSHOT_SYNC)
        
        await pipeline.stop()

    @pytest.mark.asyncio
    async def test_follower_to_leader_starts_sync(self, mock_source, mock_sender, pipeline_config):
        """When role changes from follower to leader, leader sequence should start."""
        mock_sender.role = "follower"
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])

        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        await pipeline.start()
        
        # Wait for it to start as follower
        for _ in range(100):
            if pipeline.current_role == "follower" and (pipeline.state & PipelineState.PAUSED):
                break
            await asyncio.sleep(0.01)
        
        # Verify initial follower state
        assert pipeline.current_role == "follower"
        assert pipeline.state & PipelineState.PAUSED
        
        # Switch to leader
        mock_sender.role = "leader"
        
        # Wait for control loop to detect change and run snapshot
        for _ in range(100):
            if mock_source.snapshot_calls >= 1:
                break
            await asyncio.sleep(0.01)
            
        # Assertions
        assert pipeline.current_role == "leader"
        # Snapshot should have been called
        assert mock_source.snapshot_calls >= 1
        
        await pipeline.stop()
