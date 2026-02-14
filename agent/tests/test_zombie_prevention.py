import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.runtime.agent_pipe import AgentPipe

# Mock AgentPipe for testing methods without full init
class MockAgentPipe(AgentPipe):
    def __init__(self):
        self.id = "test-pipe"
        self.task_zombie_timeout = 0.1
        self._task_last_active = {}
        # We need to mock methods called by _check_task_liveness if any logging/etc
        self.statistics = {}
        
@pytest.mark.asyncio
async def test_check_task_liveness_zombie():
    # Setup
    with patch("fustor_agent.runtime.agent_pipe.logger"):
        pipe = MockAgentPipe()
        
        # Use MagicMock for Task object, not AsyncMock, because done() is sync
        mock_task = MagicMock()
        mock_task.done.return_value = False
        
        with patch("asyncio.get_event_loop") as mock_loop_getter:
            mock_loop = MagicMock()
            mock_loop_getter.return_value = mock_loop
            
            # 1. Update activity at T=100
            current_time = 100.0
            mock_loop.time.return_value = current_time
            pipe._task_last_active["test_task"] = current_time
            
            # 2. Advance time beyond timeout (T=100.5 > 100 + 0.1)
            mock_loop.time.return_value = current_time + 0.5
            
            # 3. Check
            is_alive = await pipe._check_task_liveness(mock_task, "test_task")
            
            assert is_alive is False
            mock_task.cancel.assert_called_once()

@pytest.mark.asyncio
async def test_check_task_liveness_active():
    with patch("fustor_agent.runtime.agent_pipe.logger"):
        pipe = MockAgentPipe()
        
        mock_task = MagicMock()
        mock_task.done.return_value = False
        
        with patch("asyncio.get_event_loop") as mock_loop_getter:
            mock_loop = MagicMock()
            mock_loop_getter.return_value = mock_loop
            
            current_time = 100.0
            mock_loop.time.return_value = current_time
            pipe._task_last_active["test_task"] = current_time
            
            # Advance time slightly (T=100.05 < 100 + 0.1)
            mock_loop.time.return_value = current_time + 0.05
            
            is_alive = await pipe._check_task_liveness(mock_task, "test_task")
            
            assert is_alive is True
            assert not mock_task.cancel.called
