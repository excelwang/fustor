# agent/tests/runtime/test_agent_pipeline_lifecycle.py
"""
Lifecycle and role management tests for AgentPipeline.
These tests verify the state transitions and sequence of the pipeline.
"""
import asyncio
import pytest
from unittest.mock import MagicMock, patch
from typing import Iterator, List, Any, Dict, Tuple

from fustor_core.pipeline import PipelineState
from fustor_core.pipeline.handler import SourceHandler
from fustor_core.pipeline.sender import SenderHandler
from fustor_agent.runtime.agent_pipeline import AgentPipeline


class MockSourceHandler(SourceHandler):
    def __init__(self, events: List[Any] = None):
        super().__init__("mock-source", {})
        self.events = events or [{"index": i} for i in range(10)]
        self.snapshot_calls = 0
        self.message_calls = 0
        self.audit_calls = 0
    
    def get_snapshot_iterator(self, **kwargs) -> Iterator[Any]:
        self.snapshot_calls += 1
        return iter(self.events)
    
    async def get_message_iterator(self, start_position: int = -1, **kwargs):
        self.message_calls += 1
        try:
            while True:
                await asyncio.sleep(0.1)
                # yield nothing but stay alive
                if False: yield {} 
        except asyncio.CancelledError:
            pass
    
    def get_audit_iterator(self, **kwargs) -> Iterator[Any]:

        self.audit_calls += 1
        return iter([])


class MockSenderHandler(SenderHandler):
    def __init__(self):
        super().__init__("mock-sender", {})
        self.session_id = "test-session"
        self.role = "follower"
        self.batches = []
        self.heartbeat_calls = 0
        self.session_closed = False
    
    async def create_session(self, task_id, source_type, **kwargs):
        return self.session_id, {"role": self.role}
    
    async def send_heartbeat(self, session_id):
        self.heartbeat_calls += 1
        return {"role": self.role}
    
    async def send_batch(self, session_id, batch, context=None):
        self.batches.append((batch, context))
        return True, {}
    
    async def close_session(self, session_id):
        self.session_closed = True
        return True


@pytest.fixture
def mock_source():
    return MockSourceHandler()


@pytest.fixture
def mock_sender():
    return MockSenderHandler()


@pytest.fixture
def agent_pipeline(mock_source, mock_sender):
    return AgentPipeline(
        pipeline_id="test-sync",
        task_id="agent:test-sync",
        config={
            "batch_size": 5,
            "heartbeat_interval_sec": 0.1,
            "audit_interval_sec": 0,
            "sentinel_interval_sec": 0,
        },
        source_handler=mock_source,
        sender_handler=mock_sender
    )


@pytest.mark.asyncio
async def test_pipeline_start_and_wait_for_role(agent_pipeline, mock_sender):
    """Test starting the pipeline and waiting in follower mode."""
    await agent_pipeline.start()
    
    # Initially follower, should be in PAUSED/standby mode
    await asyncio.sleep(0.2)
    assert "Follower mode" in agent_pipeline.info
    assert PipelineState.PAUSED in agent_pipeline.state
    
    await agent_pipeline.stop()
    assert mock_sender.session_closed


@pytest.mark.asyncio
async def test_pipeline_leader_transition(agent_pipeline, mock_sender, mock_source):
    """Test transition from follower to leader."""
    # Start as follower
    await agent_pipeline.start()
    
    # Transition to leader
    mock_sender.role = "leader"
    # Wait for heartbeat to catch the change
    await asyncio.sleep(0.2)
    
    # Should be running sequence
    assert agent_pipeline.current_role == "leader"
    
    # Wait for snapshot to finish (mock_source has 10 items, batch size 5 = 2 batches)
    await asyncio.sleep(0.5)
    
    assert mock_source.snapshot_calls > 0
    assert len(mock_sender.batches) >= 2
    assert PipelineState.MESSAGE_PHASE in agent_pipeline.state
    
    await agent_pipeline.stop()


@pytest.mark.asyncio
async def test_manual_triggers(agent_pipeline, mock_sender, mock_source):
    """Test manual audit and sentinel triggers."""
    agent_pipeline.current_role = "leader"
    agent_pipeline.session_id = "test-session"
    
    await agent_pipeline.trigger_audit()
    await asyncio.sleep(0.1)
    
    assert mock_source.audit_calls > 0
    
    # Sentinel check (placeholder in implementation for now)
    await agent_pipeline.trigger_sentinel()
    # verify no crash at least
    
    await agent_pipeline.stop()


@pytest.mark.asyncio
async def test_stop_during_snapshot(agent_pipeline, mock_sender, mock_source):
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
