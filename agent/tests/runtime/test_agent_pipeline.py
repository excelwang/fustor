# agent/tests/runtime/test_agent_pipe.py
"""
Tests for AgentPipe - Basic unit tests only.

Note: Full lifecycle tests are skipped because they require async task coordination.
Those tests should be done as integration tests with proper timeouts.
"""
import pytest
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe

@pytest.fixture
def agent_pipe(mock_source, mock_sender, pipe_config):
    return AgentPipe(
        pipe_id="test-pipe",
        task_id="agent1:test-pipe",
        config=pipe_config,
        source_handler=mock_source,
        sender_handler=mock_sender
    )

class TestAgentPipeInit:
    """Test AgentPipe initialization."""
    
    def test_initial_state(self, agent_pipe):
        """Pipe should start in STOPPED state."""
        assert agent_pipe.state == PipeState.STOPPED
        assert agent_pipe.session_id is None
        assert agent_pipe.current_role is None
    
    def test_config_parsing(self, agent_pipe, pipe_config):
        """Configuration should be parsed correctly."""
        assert agent_pipe.batch_size == pipe_config["batch_size"]
        assert agent_pipe.heartbeat_interval_sec == pipe_config["heartbeat_interval_sec"]
    
    def test_dto(self, agent_pipe):
        """get_dto should return pipe info."""
        dto = agent_pipe.get_dto()
        assert dto.id == "test-pipe"
        assert dto.task_id == "agent1:test-pipe"
        assert "STOPPED" in str(dto.state)

        assert dto.statistics is not None
    
    def test_is_running_when_stopped(self, agent_pipe):
        """is_running should return False when stopped."""
        assert not agent_pipe.is_running()
    
    def test_is_outdated_when_fresh(self, agent_pipe):
        """is_outdated should return False on fresh pipe."""
        assert not agent_pipe.is_outdated()


class TestAgentPipeStateManagement:
    """Test pipestate transitions."""
    
    def test_set_state(self, agent_pipe):
        """_set_state should update state and info."""
        agent_pipe._set_state(PipeState.RUNNING, "Test info")
        
        assert agent_pipe.state == PipeState.RUNNING
        assert agent_pipe.info == "Test info"
    
    def test_composite_state(self, agent_pipe):
        """Pipe should support composite states."""
        agent_pipe._set_state(
            PipeState.RUNNING | PipeState.SNAPSHOT_SYNC
        )
        
        assert agent_pipe.is_running()
        assert PipeState.SNAPSHOT_SYNC in agent_pipe.state
    
    def test_str_representation(self, agent_pipe):
        """__str__ should return readable format."""
        s = str(agent_pipe)
        assert "test-pipe" in s
        assert "STOPPED" in s
