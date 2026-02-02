# agent/tests/runtime/test_agent_pipeline_lifecycle.py
"""
Lifecycle and role management tests for AgentPipeline.
These tests verify the state transitions and sequence of the pipeline.
"""
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_core.pipeline import PipelineState
from fustor_agent.runtime.agent_pipeline import AgentPipeline

class TestAgentPipelineLifecycle:
    """Test AgentPipeline lifecycle and role transitions."""

    @pytest.fixture
    def agent_pipeline(self, mock_source, mock_sender, pipeline_config):
        return AgentPipeline(
            pipeline_id="test-sync",
            task_id="agent:test-sync",
            config=pipeline_config,
            source_handler=mock_source,
            sender_handler=mock_sender
        )

    @pytest.mark.asyncio
    async def test_pipeline_start_and_wait_for_role(self, agent_pipeline, mock_sender):
        """Test starting the pipeline and waiting in follower mode."""
        await agent_pipeline.start()
        
        # Initially follower, should be in PAUSED/standby mode
        await asyncio.sleep(0.2)
        assert "Follower mode" in agent_pipeline.info
        assert PipelineState.PAUSED in agent_pipeline.state
        
        await agent_pipeline.stop()
        assert mock_sender.session_closed

    @pytest.mark.asyncio
    async def test_pipeline_leader_transition(self, agent_pipeline, mock_sender, mock_source):
        """Test transition from follower to leader."""
        # Start as follower
        await agent_pipeline.start()
        
        # Transition to leader
        mock_sender.role = "leader"
        # Wait for control loop to catch the change
        await asyncio.sleep(0.2)
        
        # Should be running sequence
        assert agent_pipeline.current_role == "leader"
        
        # Wait for snapshot to finish
        await asyncio.sleep(0.5)
        
        assert mock_source.snapshot_calls > 0
        assert len(mock_sender.batches) >= 2
        assert PipelineState.MESSAGE_PHASE in agent_pipeline.state
        
        await agent_pipeline.stop()

    @pytest.mark.asyncio
    async def test_manual_triggers(self, agent_pipeline, mock_sender, mock_source):
        """Test manual audit and sentinel triggers."""
        agent_pipeline.current_role = "leader"
        agent_pipeline.session_id = "test-session"
        
        # We need to set state to include RUNNING for triggers to work
        agent_pipeline.state = PipelineState.RUNNING
        
        await agent_pipeline.trigger_audit()
        await asyncio.sleep(0.2)
        
        assert mock_source.audit_calls > 0
        
        # Sentinel check
        await agent_pipeline.trigger_sentinel()
        # verify no crash at least
        
        await agent_pipeline.stop()

    @pytest.mark.asyncio
    async def test_message_to_audit_transition(self, agent_pipeline, mock_sender, mock_source):
        """Pipeline should transition to AUDIT_PHASE when audit is triggered."""
        agent_pipeline.current_role = "leader"
        agent_pipeline.session_id = "test-session"
        agent_pipeline._set_state(PipelineState.RUNNING | PipelineState.MESSAGE_PHASE)
        
        # Mock audit to take some time
        async def slow_audit():
            await asyncio.sleep(0.5)
            yield {"id": 1}
        mock_source.get_audit_iterator = MagicMock(return_value=slow_audit())
        mock_sender.send_batch = AsyncMock(return_value=(True, {}))
        
        # Trigger audit
        await agent_pipeline.trigger_audit()
        
        # It runs in background, so wait a bit
        await asyncio.sleep(0.1)
        
        # While audit is running, state should include AUDIT_PHASE
        assert PipelineState.AUDIT_PHASE in agent_pipeline.state
        assert PipelineState.MESSAGE_PHASE in agent_pipeline.state
        
        # After audit, it should return to MESSAGE_PHASE (implicit, but check no crash)
        await agent_pipeline.stop()

    @pytest.mark.asyncio
    async def test_stop_during_snapshot(self, agent_pipeline, mock_sender, mock_source):

        """Test stopping the pipeline while it is in snapshot phase."""
        # Slow down snapshot iterator to simulate work
        async def slow_iter():
            for i in range(10):
                yield {"index": i}
                await asyncio.sleep(0.1)
                
        with patch.object(mock_source, 'get_snapshot_iterator', return_value=slow_iter()):
            mock_sender.role = "leader"
            await agent_pipeline.start()
            
            await asyncio.sleep(0.2)
            assert PipelineState.SNAPSHOT_PHASE in agent_pipeline.state
            
            await agent_pipeline.stop()
            assert agent_pipeline.state == PipelineState.STOPPED
            assert mock_sender.session_closed
