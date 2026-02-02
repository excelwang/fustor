# agent/tests/runtime/test_agent_pipeline_role_switch.py
"""
Tests for AgentPipeline role transitions (Leader <-> Follower).
"""
import pytest
import asyncio
from fustor_core.pipeline import PipelineState
from fustor_agent.runtime.agent_pipeline import AgentPipeline

class TestAgentRoleSwitch:
    
    @pytest.mark.asyncio
    async def test_leader_to_follower_cancels_tasks(self, mock_source, mock_sender, pipeline_config):
        """When role changes from leader to follower, active sync tasks should be cancelled."""
        mock_sender.role = "leader"
        
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        
        # Start message sync but make it block
        phase_started = asyncio.Event()
        async def mock_msg_sync_phase():
            phase_started.set()
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                # This is what we expect
                raise
                
        pipeline._run_message_sync = mock_msg_sync_phase
        
        await pipeline.start()
        await phase_started.wait()
        
        # Verify it's in MESSAGE_SYNC
        assert pipeline.state & PipelineState.MESSAGE_SYNC
        assert pipeline._message_sync_task is not None
        assert not pipeline._message_sync_task.done()
        
        # Change role via heartbeat
        mock_sender.role = "follower"
        
        # Wait for heartbeat to process
        await asyncio.sleep(0.05)
        
        # Assertions
        assert pipeline.current_role == "follower"
        # Task should have been cancelled
        assert pipeline._message_sync_task is None or pipeline._message_sync_task.cancelled() or pipeline._message_sync_task.done()
        # State should reflect follower standby (PAUSED)
        assert pipeline.state & PipelineState.PAUSED
        
        await pipeline.stop()

    @pytest.mark.asyncio
    async def test_follower_to_leader_starts_sync(self, mock_source, mock_sender, pipeline_config):
        """When role changes from follower to leader, pipeline sequence should start."""
        mock_sender.role = "follower"
        
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        
        await pipeline.start()
        
        # Wait for loop to start in follower mode
        await asyncio.sleep(0.05)
        assert pipeline.current_role == "follower"
        assert pipeline.state & PipelineState.PAUSED
        
        # Switch to leader
        mock_sender.role = "leader"
        
        # Mock _run_message_sync to block so we can catch the state
        phase_started = asyncio.Event()
        async def mock_msg_sync_phase():
            phase_started.set()
            while True:
                await asyncio.sleep(0.1)
        pipeline._run_message_sync = mock_msg_sync_phase
        
        # Wait for control loop to detect change and finish snapshot
        await phase_started.wait()
        
        # Assertions
        assert pipeline.current_role == "leader"
        # Snapshot should have been called
        assert mock_source.snapshot_calls >= 1
        # Now it should be in MESSAGE_SYNC
        assert pipeline.state & PipelineState.MESSAGE_SYNC
        
        await pipeline.stop()
