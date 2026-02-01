# agent/tests/runtime/test_agent_pipeline.py
"""
Tests for AgentPipeline - Basic unit tests only.

Note: Full lifecycle tests are skipped because they require async task coordination.
Those tests should be done as integration tests with proper timeouts.
"""
import pytest
from typing import Iterator, List, Any, Dict, Tuple

from fustor_core.pipeline import PipelineState
from fustor_core.pipeline.handler import SourceHandler
from fustor_core.pipeline.sender import SenderHandler
from fustor_agent.runtime.agent_pipeline import AgentPipeline


from .mocks import MockSourceHandler, MockSenderHandler


@pytest.fixture
def mock_source():
    return MockSourceHandler()


@pytest.fixture
def mock_sender():
    return MockSenderHandler()


@pytest.fixture
def pipeline_config():
    return {
        "batch_size": 5,
        "heartbeat_interval_sec": 1,
        "audit_interval_sec": 0,
        "sentinel_interval_sec": 0,
    }


@pytest.fixture
def agent_pipeline(mock_source, mock_sender, pipeline_config):
    return AgentPipeline(
        pipeline_id="test-sync",
        task_id="agent1:test-sync",
        config=pipeline_config,
        source_handler=mock_source,
        sender_handler=mock_sender
    )


class TestAgentPipelineInit:
    """Test AgentPipeline initialization."""
    
    def test_initial_state(self, agent_pipeline):
        """Pipeline should start in STOPPED state."""
        assert agent_pipeline.state == PipelineState.STOPPED
        assert agent_pipeline.session_id is None
        assert agent_pipeline.current_role is None
    
    def test_config_parsing(self, agent_pipeline, pipeline_config):
        """Configuration should be parsed correctly."""
        assert agent_pipeline.batch_size == pipeline_config["batch_size"]
        assert agent_pipeline.heartbeat_interval_sec == pipeline_config["heartbeat_interval_sec"]
    
    def test_dto(self, agent_pipeline):
        """get_dto should return pipeline info."""
        dto = agent_pipeline.get_dto()
        assert dto.id == "test-sync"
        assert dto.task_id == "agent1:test-sync"
        assert "STOPPED" in str(dto.state)

        assert dto.statistics is not None
    
    def test_is_running_when_stopped(self, agent_pipeline):
        """is_running should return False when stopped."""
        assert not agent_pipeline.is_running()
    
    def test_is_outdated_when_fresh(self, agent_pipeline):
        """is_outdated should return False on fresh pipeline."""
        assert not agent_pipeline.is_outdated()


class TestAgentPipelineStateManagement:
    """Test pipeline state transitions."""
    
    def test_set_state(self, agent_pipeline):
        """_set_state should update state and info."""
        agent_pipeline._set_state(PipelineState.RUNNING, "Test info")
        
        assert agent_pipeline.state == PipelineState.RUNNING
        assert agent_pipeline.info == "Test info"
    
    def test_composite_state(self, agent_pipeline):
        """Pipeline should support composite states."""
        agent_pipeline._set_state(
            PipelineState.RUNNING | PipelineState.SNAPSHOT_PHASE
        )
        
        assert agent_pipeline.is_running()
        assert PipelineState.SNAPSHOT_PHASE in agent_pipeline.state
    
    def test_str_representation(self, agent_pipeline):
        """__str__ should return readable format."""
        s = str(agent_pipeline)
        assert "test-sync" in s
        assert "STOPPED" in s
