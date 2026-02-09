# agent/tests/runtime/test_agent_pipe_bus.py
"""
Tests for AgentPipe integration with EventBus.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe

class TestAgentPipeBus:

    @pytest.mark.asyncio
    async def test_run_bus_message_sync_success(self, mock_source, mock_sender, pipe_config):
        """Pipe should read from bus and commit successfully."""
        # Setup mock bus
        mock_bus = MagicMock()
        mock_bus.id = "bus-123"
        mock_bus.internal_bus = AsyncMock()
        
        # Mock events from bus
        mock_event = MagicMock()
        mock_event.index = 100
        mock_bus.internal_bus.get_events_for.side_effect = [
            [mock_event], # First call returns one event
            [],           # Second call returns nothing (continue)
            asyncio.CancelledError() # Stop the loop
        ]
        
        # Override send_batch with AsyncMock for assertions
        mock_sender.send_batch = AsyncMock(return_value=(True, {"count": 1}))
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        pipe.session_id = "test-session"
        pipe.state = PipeState.RUNNING
        
        # Run bus message sync phase
        try:
            await pipe._run_bus_message_sync()
        except asyncio.CancelledError:
            pass
            
        # Assertions
        # 1. Should have called get_events_for
        assert mock_bus.internal_bus.get_events_for.called
        # 2. Should have sent to Fusion
        mock_sender.send_batch.assert_called_with(
            "test-session", [mock_event], {"phase": "realtime"}
        )
        # 3. Should have committed to bus
        mock_bus.internal_bus.commit.assert_called_with("agent:test-id", 1, 100)
        # 4. Statistics should be updated
        assert pipe.statistics["events_pushed"] == 1

    @pytest.mark.asyncio
    async def test_run_bus_message_sync_retry_on_failure(self, mock_source, mock_sender, pipe_config):
        """Pipe should retry if sending bus events fails."""
        mock_bus = MagicMock()
        mock_bus.internal_bus = AsyncMock()
        
        mock_event = MagicMock()
        mock_event.index = 100
        mock_bus.internal_bus.get_events_for.side_effect = [
            [mock_event], # First attempt
            [mock_event], # Second attempt
            asyncio.CancelledError()
        ]
        
        # Override send_batch with AsyncMock
        mock_sender.send_batch = AsyncMock(side_effect = [
            (False, {"error": "Failed"}),
            (True, {"success": True})
        ])
        
        pipe = AgentPipe(
            "test-id", "agent:test-id", pipe_config,
            mock_source, mock_sender, event_bus=mock_bus
        )
        pipe.session_id = "test-session"
        pipe.state = PipeState.RUNNING
        
        # Run
        try:
            await pipe._run_bus_message_sync()
        except asyncio.CancelledError:
            pass
            
        # Should have called send_batch twice
        assert mock_sender.send_batch.call_count == 2
        # Should have committed only ONCE (after second success)
        mock_bus.internal_bus.commit.assert_called_with("agent:test-id", 1, 100)
        assert mock_bus.internal_bus.commit.call_count == 1
