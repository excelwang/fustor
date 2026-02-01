# fusion/tests/runtime/test_fusion_pipeline.py
"""
Tests for FusionPipeline.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict, List, Optional

from fustor_core.pipeline import PipelineState
from fustor_core.pipeline.handler import ViewHandler
from fustor_fusion.runtime import FusionPipeline


class MockViewHandler(ViewHandler):
    """Mock view handler for testing."""
    
    schema_name = "mock"
    
    def __init__(self, handler_id: str = "mock-view"):
        super().__init__(handler_id, {})
        self.events_processed: List[Any] = []
        self.session_starts = 0
        self.session_closes = 0
        self.initialized = False
        self._closed = False
    
    async def initialize(self) -> None:
        self.initialized = True
    
    async def close(self) -> None:
        self._closed = True
    
    async def process_event(self, event: Any) -> None:
        self.events_processed.append(event)
    
    async def on_session_start(self) -> None:
        self.session_starts += 1
    
    async def on_session_close(self) -> None:
        self.session_closes += 1
    
    def get_data_view(self, **kwargs) -> Dict[str, Any]:
        return {"events_count": len(self.events_processed)}
    
    def get_stats(self) -> Dict[str, Any]:
        return {"processed": len(self.events_processed)}


@pytest.fixture
def mock_view_handler():
    return MockViewHandler()


@pytest.fixture
def pipeline_config():
    return {
        "datastore_id": 1,
        "allow_concurrent_push": True,
        "queue_batch_size": 100,
    }


@pytest.fixture
def fusion_pipeline(mock_view_handler, pipeline_config):
    return FusionPipeline(
        pipeline_id="test-pipeline",
        config=pipeline_config,
        view_handlers=[mock_view_handler]
    )


class TestFusionPipelineInit:
    """Test FusionPipeline initialization."""
    
    def test_initial_state(self, fusion_pipeline):
        """Pipeline should start in STOPPED state."""
        assert fusion_pipeline.state == PipelineState.STOPPED
        assert fusion_pipeline.datastore_id == "1"
    
    def test_view_handlers_registered(self, fusion_pipeline, mock_view_handler):
        """View handlers should be registered."""
        assert "mock-view" in fusion_pipeline.get_available_views()
        assert fusion_pipeline.get_view_handler("mock-view") is mock_view_handler
    
    def test_config_parsing(self, fusion_pipeline, pipeline_config):
        """Configuration should be parsed correctly."""
        assert fusion_pipeline.allow_concurrent_push == pipeline_config["allow_concurrent_push"]
        assert fusion_pipeline.queue_batch_size == pipeline_config["queue_batch_size"]
    
    @pytest.mark.asyncio
    async def test_dto(self, fusion_pipeline):
        """get_dto should return pipeline info."""
        dto = await fusion_pipeline.get_dto()
        assert dto["id"] == "test-pipeline"
        assert dto["datastore_id"] == "1"
        assert "mock-view" in dto["view_handlers"]
        assert "statistics" in dto


class TestFusionPipelineLifecycle:
    """Test FusionPipeline start/stop lifecycle."""
    
    @pytest.mark.asyncio
    async def test_start(self, fusion_pipeline, mock_view_handler):
        """start() should initialize handlers and start processing."""
        await fusion_pipeline.start()
        
        assert fusion_pipeline.is_running()
        assert mock_view_handler.initialized
        
        await fusion_pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_stop(self, fusion_pipeline, mock_view_handler):
        """stop() should close handlers."""
        await fusion_pipeline.start()
        await fusion_pipeline.stop()
        
        assert not fusion_pipeline.is_running()
        assert mock_view_handler._closed


class TestFusionPipelineSession:
    """Test session management."""
    
    @pytest.mark.asyncio
    async def test_session_created_first_is_leader(self, fusion_pipeline, mock_view_handler):
        """First session should become leader."""
        await fusion_pipeline.start()
        
        await fusion_pipeline.on_session_created("sess-1", task_id="agent:sync")
        
        assert await fusion_pipeline.get_session_role("sess-1") == "leader"
        assert mock_view_handler.session_starts == 1
        
        await fusion_pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_session_created_second_is_follower(self, fusion_pipeline):
        """Second session should be follower."""
        await fusion_pipeline.start()
        
        await fusion_pipeline.on_session_created("sess-1", task_id="agent1:sync")
        await fusion_pipeline.on_session_created("sess-2", task_id="agent2:sync")
        
        assert await fusion_pipeline.get_session_role("sess-1") == "leader"
        assert await fusion_pipeline.get_session_role("sess-2") == "follower"
        
        await fusion_pipeline.stop()
    
    @pytest.mark.asyncio
    async def test_leader_election_on_close(self, fusion_pipeline):
        """New leader should be elected when leader leaves."""
        await fusion_pipeline.start()
        
        await fusion_pipeline.on_session_created("sess-1", task_id="agent1:sync")
        await fusion_pipeline.on_session_created("sess-2", task_id="agent2:sync")
        
        await fusion_pipeline.on_session_closed("sess-1")
        
        assert await fusion_pipeline.get_session_role("sess-2") == "leader"
        
        await fusion_pipeline.stop()


class TestFusionPipelineDto:
    """Test DTO and stats."""
    
    @pytest.mark.asyncio
    async def test_aggregated_stats(self, fusion_pipeline, mock_view_handler):
        """Aggregated stats should include view stats."""
        await fusion_pipeline.start()
        
        stats = fusion_pipeline.get_aggregated_stats()
        
        assert "pipeline" in stats
        assert "views" in stats
        assert "mock-view" in stats["views"]
        
        await fusion_pipeline.stop()
    
    def test_str_representation(self, fusion_pipeline):
        """__str__ should return readable format."""
        s = str(fusion_pipeline)
        assert "test-pipeline" in s
        assert "STOPPED" in s


class TestFusionPipelineViewHandler:
    """Test view handler operations."""
    
    def test_register_handler(self, fusion_pipeline):
        """should be able to register additional handlers."""
        new_handler = MockViewHandler("another-view")
        fusion_pipeline.register_view_handler(new_handler)
        
        assert "another-view" in fusion_pipeline.get_available_views()
    
    @pytest.mark.asyncio
    async def test_get_view(self, fusion_pipeline, mock_view_handler):
        """get_view should return handler's data view."""
        await fusion_pipeline.start()
        
        view = fusion_pipeline.get_view("mock-view")
        assert view == {"events_count": 0}
        
        await fusion_pipeline.stop()
    
    def test_get_view_nonexistent(self, fusion_pipeline):
        """get_view should return None for nonexistent handler."""
        view = fusion_pipeline.get_view("nonexistent")
        assert view is None
