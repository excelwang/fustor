
import pytest
import asyncio
from unittest.mock import MagicMock
from fustor_agent.runtime.agent_pipeline import AgentPipeline
from fustor_agent.runtime.pipeline.phases import run_audit_sync
from .mocks import MockSourceHandler, MockSenderHandler

from fustor_core.pipeline import PipelineState

class TestAgentPipelineAudit:
    
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
    def agent_pipeline(self, mock_source_audit, mock_sender, pipeline_config):
        pipeline = AgentPipeline(
            pipeline_id="test-audit-pipeline",
            task_id="agent:test-audit",
            config=pipeline_config,
            source_handler=mock_source_audit,
            sender_handler=mock_sender
        )
        return pipeline

    @pytest.mark.asyncio
    async def test_audit_sync_updates_context(self, agent_pipeline):
        """
        Verify that run_audit_sync updates pipeline.audit_context from source iterator items,
        properly handling both Silent (None) entries and Active entries.
        """
        # Set state to RUNNING so loop continues
        agent_pipeline._set_state(PipelineState.RUNNING, "Starting test")
        agent_pipeline.session_id = "sess-1"
        
        # Run audit phase directly
        await run_audit_sync(agent_pipeline)
        
        # Verify context updates
        assert "/path/silent" in agent_pipeline.audit_context
        assert agent_pipeline.audit_context["/path/silent"] == 100.0
        
        assert "/path/active" in agent_pipeline.audit_context
        assert agent_pipeline.audit_context["/path/active"] == 200.0
        
        # Verify sender called for active event but NOT for silent one (None)
        # We expect 1 batch of 1 event
        assert len(agent_pipeline.sender_handler.batches_sent) == 1 + 1 # 1 data batch + 1 final signal batch
        rows = agent_pipeline.sender_handler.batches_sent[0]
        assert len(rows) == 1
        assert rows[0]["event_type"] == "update"
