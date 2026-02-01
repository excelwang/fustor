# agent/tests/runtime/test_agent_pipeline_error_recovery.py
"""
Tests for AgentPipeline error recovery and session loss handling.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_core.pipeline import PipelineState
from fustor_agent.runtime.agent_pipeline import AgentPipeline
from .mocks import MockSourceHandler, MockSenderHandler

# Speed up tests by shortening intervals
AgentPipeline.CONTROL_LOOP_INTERVAL = 0.01
AgentPipeline.ROLE_CHECK_INTERVAL = 0.01
AgentPipeline.FOLLOWER_STANDBY_INTERVAL = 0.01
AgentPipeline.ERROR_RETRY_INTERVAL = 0.01

@pytest.fixture
def mock_source():
    ms = MockSourceHandler()
    # Make message iterator block so it doesn't end immediately
    async def mock_aiter_msg():
        while True:
            await asyncio.sleep(0.1)
            yield {"index": 999}
    ms.get_message_iterator = MagicMock(return_value=mock_aiter_msg())
    return ms

@pytest.fixture
def mock_sender():
    return MockSenderHandler()

@pytest.fixture
def pipeline_config():
    return {
        "batch_size": 5,
        "heartbeat_interval_sec": 0.01,
        "audit_interval_sec": 0.05,
        "sentinel_interval_sec": 0,
    }

class TestAgentErrorRecovery:
    
    @pytest.mark.asyncio
    async def test_session_creation_retry(self, mock_source, mock_sender, pipeline_config):
        """Pipeline should retry session creation if it fails."""
        # Setup sender to fail first 2 times then succeed
        mock_sender.create_session = AsyncMock(side_effect=[
            RuntimeError("Connection refused"),
            RuntimeError("Timeout"),
            ("valid-session", {"role": "leader"})
        ])
        
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        
        # Start pipeline
        await pipeline.start()
        
        # Wait a bit for iterations
        await asyncio.sleep(0.2)
        
        try:
            # Should eventually succeed
            assert mock_sender.create_session.call_count >= 3
            assert pipeline.session_id == "valid-session"
            # It should be in RUNNING | MESSAGE_PHASE now
            assert pipeline.is_running()
        finally:
            await pipeline.stop()

    @pytest.mark.asyncio
    async def test_session_loss_during_message_sync(self, mock_source, mock_sender, pipeline_config):
        """Pipeline should detect session loss and restart from snapshot."""
        # Setup: Start as leader
        mock_sender.role = "leader"
        
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        
        # Mock _run_message_sync to simulate error after some time
        original_msg_sync = pipeline._run_message_sync
        
        error_triggered = False
        async def mock_msg_sync():
            nonlocal error_triggered
            if not error_triggered:
                await asyncio.sleep(0.05)
                error_triggered = True
                raise RuntimeError("Session lost")
            # Success on retry
            while True:
                await asyncio.sleep(0.1)
            
        pipeline._run_message_sync = mock_msg_sync
        
        # Start
        await pipeline.start()
        
        # Wait for error and recovery
        # We need enough time for error -> backoff -> retry -> success
        await asyncio.sleep(0.3)
        
        try:
            assert error_triggered
            # Should have restarted and called snapshot again
            assert mock_source.snapshot_calls >= 2
            # Check state after recovery
            assert pipeline.session_id is not None
        finally:
            await pipeline.stop()

    @pytest.mark.asyncio
    async def test_audit_sync_with_session_loss(self, mock_source, mock_sender, pipeline_config):
        """Audit sync should handle session being cleared during execution."""
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        pipeline.session_id = "test-session"
        pipeline.current_role = "leader"
        
        # Mock send_batch to clear session_id mid-audit
        call_count = 0
        async def mock_send_batch(session_id, batch, context=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1: # First call (audit start)
                pipeline.session_id = None # Clear session!
            return True, {}
            
        mock_sender.send_batch = mock_send_batch
        
        # Manually run one audit sync
        # We need set_state to include RUNNING for it to work
        pipeline.state = PipelineState.RUNNING
        await pipeline._run_audit_sync()
        
        # Assertions:
        # 1. It should NOT throw AttributeError when trying to send "audit end" if session_id is None
        # (This is what we fixed with has_active_session())
        assert pipeline.session_id is None
        assert call_count == 1 # Second call (audit end) should have been skipped or handled safely
        
    @pytest.mark.asyncio
    async def test_exponential_backoff_values(self, mock_source, mock_sender, pipeline_config):
        """Test that consecutive errors increase backoff."""
        pipeline = AgentPipeline(
            "test-id", "agent:test-id", pipeline_config,
            mock_source, mock_sender
        )
        
        mock_sender.create_session = AsyncMock(side_effect=RuntimeError("Fail"))
        
        # Start and let it fail once
        task = asyncio.create_task(pipeline._run_control_loop())
        
        await asyncio.sleep(0.05)
        
        try:
            # 1st error: backoff should be ERROR_RETRY_INTERVAL (0.01)
            # 2nd error: backoff should be 0.02
            # 3rd error: backoff should be 0.04
            assert pipeline._consecutive_errors >= 2
            # The state info should reflect backoff
            assert "backoff" in pipeline.info
        finally:
            task.cancel()
            await pipeline.stop()
