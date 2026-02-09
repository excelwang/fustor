
import pytest
import asyncio
from unittest.mock import MagicMock
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_agent.runtime.pipe.phases import run_audit_sync
from .mocks import MockSourceHandler, MockSenderHandler

from fustor_core.pipe import PipeState

class TestAgentPipeAudit:
    
    @pytest.fixture
    def mock_source_audit(self):
        ms = MockSourceHandler()
        # Mock audit iterator to return specific tuple sequence
        ms.get_audit_iterator = MagicMock(return_value=iter([
            (None, {"/path/silent": 100.0}),
            ({"event_type": "update", "path": "/path/active"}, {"/path/active": 200.0})
        ]))
        return ms

    @pytest.fixture
    def agent_pipe(self, mock_source_audit, mock_sender, pipe_config):
        pipe = AgentPipe(
            pipe_id="test-audit-pipe",
            task_id="agent:test-audit",
            config=pipe_config,
            source_handler=mock_source_audit,
            sender_handler=mock_sender
        )
        return pipe

    @pytest.mark.asyncio
    async def test_audit_sync_updates_context(self, agent_pipe):
        """
        Verify that run_audit_sync updates pipe.audit_context from source iterator items,
        properly handling both Silent (None) entries and Active entries.
        """
        # Set state to RUNNING so loop continues
        agent_pipe._set_state(PipeState.RUNNING, "Starting test")
        agent_pipe.session_id = "sess-1"
        
        # Run audit sync directly
        await run_audit_sync(agent_pipe)
        
        # Verify context updates
        assert "/path/silent" in agent_pipe.audit_context
        assert agent_pipe.audit_context["/path/silent"] == 100.0
        
        assert "/path/active" in agent_pipe.audit_context
        assert agent_pipe.audit_context["/path/active"] == 200.0
        
        # Verify sender called for active event but NOT for silent one (None)
        # We expect 1 batch of 1 event
        assert len(agent_pipe.sender_handler.batches_sent) == 1 + 1 # 1 data batch + 1 final signal batch
        rows = agent_pipe.sender_handler.batches_sent[0]
        assert len(rows) == 1
        assert rows[0]["event_type"] == "update"
