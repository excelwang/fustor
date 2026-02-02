
import pytest
import asyncio
from unittest.mock import AsyncMock
from fustor_agent.runtime.agent_pipeline import AgentPipeline
from .mocks import MockSourceHandler, MockSenderHandler

class TestAgentPipelineResume:
    
    @pytest.fixture
    def mock_sender_with_index(self):
        sender = MockSenderHandler()
        sender.get_latest_committed_index = AsyncMock(return_value=12345)
        return sender

    @pytest.fixture
    def agent_pipeline(self, mock_source, mock_sender_with_index, pipeline_config):
        return AgentPipeline(
            pipeline_id="test-resume-pipeline",
            task_id="agent:test-resume",
            config=pipeline_config,
            source_handler=mock_source,
            sender_handler=mock_sender_with_index
        )

    @pytest.mark.asyncio
    async def test_resume_from_committed_index(self, agent_pipeline, mock_sender_with_index):
        """
        Verify that the pipeline initializes its statistics with the 
        committed index fetched from Fusion on startup.
        """
        # Start pipeline
        await agent_pipeline.start()
        
        # Wait for session creation (control loop iteration)
        await asyncio.sleep(0.1)
        
        # Verify call was made
        mock_sender_with_index.get_latest_committed_index.assert_called_once()
        
        # Verify stats updated
        assert agent_pipeline.statistics["last_pushed_event_id"] == 12345
        
        # Cleanup
        await agent_pipeline.stop()

    @pytest.mark.asyncio
    async def test_resume_does_not_overwrite_newer_local_stats(self, agent_pipeline, mock_sender_with_index):
        """
        Verify that if local stats are newer (e.g. from previous run in same process), 
        we don't overwrite with older server index.
        """
        # Simulate local progress
        agent_pipeline.statistics["last_pushed_event_id"] = 99999
        mock_sender_with_index.get_latest_committed_index.return_value = 100
        
        await agent_pipeline.start()
        await asyncio.sleep(0.1)
        
        # Should persist local value
        assert agent_pipeline.statistics["last_pushed_event_id"] == 99999
        
        await agent_pipeline.stop()
