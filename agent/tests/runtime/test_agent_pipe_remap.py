# agent/tests/runtime/test_agent_pipe_remap.py
"""
Tests for AgentPipe.remap_to_new_bus() method.

This tests the hot-migration of a pipe to a new EventBus instance
when bus splitting occurs.
"""
import pytest
from unittest.mock import MagicMock
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe

@pytest.fixture
def mock_bus():
    """Create a mock EventBusInstanceRuntime."""
    bus = MagicMock()
    bus.id = "bus-12345"
    bus.internal_bus = MagicMock()
    return bus

@pytest.fixture
def new_mock_bus():
    """Create a second mock EventBusInstanceRuntime for remap target."""
    bus = MagicMock()
    bus.id = "bus-67890"
    bus.internal_bus = MagicMock()
    return bus

@pytest.fixture
def agent_pipe(mock_source, mock_sender, pipe_config, mock_bus):
    return AgentPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        source_handler=mock_source,
        sender_handler=mock_sender,
        event_bus=mock_bus
    )

class TestRemapToNewBus:
    """Tests for remap_to_new_bus method."""

    @pytest.mark.asyncio
    async def test_remap_without_position_loss(
        self, agent_pipe, new_mock_bus, mock_bus
    ):
        """remap_to_new_bus should replace bus reference when no position lost."""
        # Pre-condition
        assert agent_pipe.bus.id == mock_bus.id
        
        # Action
        await agent_pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=False)
        
        # Assert
        assert agent_pipe.bus.id == new_mock_bus.id
        assert agent_pipe.bus != mock_bus

    @pytest.mark.asyncio
    async def test_remap_with_position_loss_cancels_message_sync(
        self, agent_pipe, new_mock_bus
    ):
        """remap_to_new_bus should cancel message sync phase when position is lost."""
        # Setup: create a mock message sync phase task
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mock_task.cancel = MagicMock()
        agent_pipe._message_sync_task = mock_task
        
        # Action
        await agent_pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=True)
        
        # Assert
        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_remap_with_position_loss_sets_reconnecting_state(
        self, agent_pipe, new_mock_bus
    ):
        """remap_to_new_bus should set RECONNECTING state on position loss."""
        # Action
        await agent_pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=True)
        
        # Assert
        assert agent_pipe.state & PipeState.RECONNECTING
        assert "re-sync" in agent_pipe.info.lower()

    @pytest.mark.asyncio
    async def test_remap_without_position_loss_preserves_state(
        self, agent_pipe, new_mock_bus
    ):
        """remap_to_new_bus should not change state when no position lost."""
        # Setup
        agent_pipe._set_state(PipeState.RUNNING | PipeState.MESSAGE_SYNC)
        original_state = agent_pipe.state
        
        # Action
        await agent_pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=False)
        
        # Assert - state should be unchanged
        assert agent_pipe.state == original_state

    @pytest.mark.asyncio
    async def test_remap_from_none_bus(self, mock_source, mock_sender, pipe_config, new_mock_bus):
        """remap_to_new_bus should work when initial bus is None."""
        # Create pipe without bus
        pipe = AgentPipe(
            pipe_id="test-pipe-no-bus",
            config=pipe_config,
            source_handler=mock_source,
            sender_handler=mock_sender,
            event_bus=None  # No initial bus
        )
        
        # Action - should not raise
        await pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=False)
        
        # Assert
        assert pipe.bus.id == new_mock_bus.id

    @pytest.mark.asyncio
    async def test_remap_skips_cancel_when_task_already_done(
        self, agent_pipe, new_mock_bus
    ):
        """remap_to_new_bus should not call cancel on completed task."""
        # Setup: create a mock message sync phase task that is already done
        mock_task = MagicMock()
        mock_task.done.return_value = True  # Already done
        mock_task.cancel = MagicMock()
        agent_pipe._message_sync_task = mock_task
        
        # Action
        await agent_pipe.remap_to_new_bus(new_mock_bus, needed_position_lost=True)
        
        # Assert - cancel should NOT be called
        mock_task.cancel.assert_not_called()
