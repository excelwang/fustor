# agent/tests/runtime/test_agent_pipe_error_recovery.py
"""
Tests for AgentPipe error recovery and session loss handling.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe
from .mocks import MockSourceHandler, MockSenderHandler

# Using fixtures and fast intervals from conftest.py


class TestAgentErrorRecovery:
    
    @pytest.mark.asyncio
    async def test_session_creation_retry(self, mock_source, mock_sender, pipe_config):
        """Pipe should retry session creation if it fails."""
        # Setup sender to fail first 2 times then succeed
        call_count = 0
        async def mock_create_session(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1: raise RuntimeError("Connection refused")
            if call_count == 2: raise RuntimeError("Timeout")
            return "valid-session", {"role": "leader"}
            
        mock_sender.create_session = AsyncMock(side_effect=mock_create_session)
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        # Ensure it doesn't fail on length check or iteration
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        # Start pipe
        await pipe.start()
        
        # Wait for retries to happen - need time for backoff (0.1s, 0.2s etc)
        await asyncio.sleep(1.0)
        
        try:
            # Should eventually succeed
            assert mock_sender.create_session.call_count >= 3
            assert pipe.session_id == "valid-session"
            # It should be in RUNNING | MESSAGE_SYNC now
            assert pipe.is_running()
        finally:
            await pipe.stop()

    @pytest.mark.asyncio
    async def test_session_loss_during_message_sync(self, mock_source, mock_sender, pipe_config):
        """Pipe should detect session loss and restart from snapshot."""
        # Setup: Start as leader with a bus so message sync can work
        mock_sender.role = "leader"
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        # Mock _run_message_sync to simulate error after some time
        original_msg_sync_phase = pipe._run_message_sync
        
        error_triggered = False
        async def mock_msg_sync_phase():
            nonlocal error_triggered
            pipe._set_state(pipe.state | PipeState.MESSAGE_SYNC)
            if not error_triggered:
                await asyncio.sleep(0.05)
                error_triggered = True
                await pipe._handle_fatal_error(RuntimeError("Session lost"))
                return
            # Success on retry
            while True:
                await asyncio.sleep(0.1)
            
        pipe._run_message_sync = mock_msg_sync_phase
        
        # Start
        await pipe.start()
        
        # Wait for error and recovery
        # Need time for: snapshot (~0.1s) + thread join (0.5s) + message sync entry + error + backoff + retry
        await asyncio.sleep(1.5)
        
        try:
            assert error_triggered
            # Should have restarted and called snapshot again
            assert mock_source.snapshot_calls >= 2
            # Check state after recovery
            assert pipe.session_id is not None
        finally:
            await pipe.stop()

    @pytest.mark.asyncio
    async def test_audit_sync_with_session_loss(self, mock_source, mock_sender, pipe_config):
        """Audit phase should handle session being cleared during execution."""
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender
        )
        pipe.session_id = "test-session"
        pipe.current_role = "leader"
        
        # Mock send_batch to clear session_id mid-audit
        call_count = 0
        async def mock_send_batch(session_id, batch, context=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1: # First call (audit start)
                pipe.session_id = None # Clear session!
            return True, {}
            
        mock_sender.send_batch = mock_send_batch
        
        # Manually run one audit phase
        # We need set_state to include RUNNING for it to work
        pipe.state = PipeState.RUNNING
        await pipe._run_audit_sync()
        
        # Assertions:
        # 1. It should NOT throw AttributeError when trying to send "audit end" if session_id is None
        # (This is what we fixed with has_active_session())
        assert pipe.session_id is None
        assert call_count == 1 # Second call (audit end) should have been skipped or handled safely
        
    @pytest.mark.asyncio
    async def test_initialization_error_sets_error_state(self, mock_source, mock_sender, pipe_config):
        """Pipe should go to ERROR state if handler initialization fails."""
        mock_source.initialize = AsyncMock(side_effect=RuntimeError("Init failed"))
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender
        )
        
        await pipe.start()
        
        assert pipe.state == PipeState.ERROR
        assert "Initialization failed" in pipe.info

    @pytest.mark.asyncio
    async def test_session_obsolete_clears_session_immediately(
        self, mock_source, mock_sender, pipe_config
    ):
        """Pipe should clear session and reconnect immediately on SessionObsoletedError."""
        from fustor_core.exceptions import SessionObsoletedError
        
        mock_sender.role = "leader"
        # Disable background tasks to make test deterministic
        pipe_config_no_bg = pipe_config.copy()
        pipe_config_no_bg["audit_interval_sec"] = 0
        pipe_config_no_bg["sentinel_interval_sec"] = 0
        
        mock_bus = MagicMock()
        mock_bus.id = "mock-bus"
        mock_bus.internal_bus = AsyncMock()
        mock_bus.internal_bus.get_events_for = AsyncMock(return_value=[])
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config_no_bg,
            mock_source, mock_sender, event_bus=mock_bus
        )
        
        # Setup: Success first time, then 419 error, then success again
        # Note: send_batch is called multiple times during leader sequence
        create_call_count = 0
        async def mock_create_session(*args, **kwargs):
            nonlocal create_call_count
            create_call_count += 1
            return f"sess-{create_call_count}", {"role": "leader"}
            
        mock_sender.create_session = AsyncMock(side_effect=mock_create_session)
        
        batch_call_count = 0
        async def mock_send_batch(*args, **kwargs):
            nonlocal batch_call_count
            batch_call_count += 1
            if batch_call_count == 2: # Snapshot readiness succeeded, now fail message sync
                raise SessionObsoletedError("Session dead")
            return True, {"success": True}
            
        mock_sender.send_batch = mock_send_batch
        
        # Configure bus to yield one event then nothing
        mock_event = MagicMock()
        mock_event.index = 100
        # Use a list that won't run out too quickly to avoid StopIteration recovery loops
        mock_bus.internal_bus.get_events_for = AsyncMock(side_effect=[[mock_event]] + [[]] * 100)
        
        # Wait for recovery
        await pipe.start()
        
        # Give it time to hit the error and recover
        # Since we use 'continue' and no backoff for 419, it should be fast
        success = False
        for _ in range(40): # Up to 2 seconds
            if mock_sender.create_session.call_count >= 2:
                success = True
                break
            await asyncio.sleep(0.05)
        
        try:
            # Should have called create_session at least twice (initial + recovery)
            assert success, f"Expected 2+ calls, got {mock_sender.create_session.call_count}"
            # Should be back with an active session
            assert pipe.has_active_session()
            assert mock_sender.create_session.call_count >= 2
        finally:
            await pipe.stop()

    @pytest.mark.asyncio
    async def test_exponential_backoff_values(self, mock_source, mock_sender, pipe_config):

        """Test that consecutive errors increase backoff."""
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender
        )
        
        mock_sender.create_session = AsyncMock(side_effect=RuntimeError("Fail"))
        
        # Start and let it fail
        task = asyncio.create_task(pipe._run_control_loop())
        
        # Wait for a couple of retries (interval is 0.1s + backoff)
        await asyncio.sleep(0.5)
        
        try:
            # 1st error: backoff should be ERROR_RETRY_INTERVAL (0.01)
            # 2nd error: backoff should be 0.02
            # 3rd error: backoff should be 0.04
            assert pipe._consecutive_errors >= 2
            # The state info should reflect backoff
            assert "backoff" in pipe.info
        finally:
            task.cancel()
            await pipe.stop()
