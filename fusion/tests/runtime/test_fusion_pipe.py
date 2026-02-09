# fusion/tests/runtime/test_fusion_pipe.py
"""
Tests for FusionPipe.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict, List, Optional

from fustor_core.pipe import PipeState
from fustor_core.pipe.handler import ViewHandler
from fustor_fusion.runtime import FusionPipe


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
def pipe_config():
    return {
        "view_id": "1",
        "allow_concurrent_push": True,
        "queue_batch_size": 100,
    }


@pytest.fixture
def fusion_pipe(mock_view_handler, pipe_config):
    return FusionPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        view_handlers=[mock_view_handler]
    )


class TestFusionPipeInit:
    """Test FusionPipe initialization."""
    
    def test_initial_state(self, fusion_pipe):
        """Pipe should start in STOPPED state."""
        assert fusion_pipe.state == PipeState.STOPPED
        assert fusion_pipe.view_id == "1"
    
    def test_view_handlers_registered(self, fusion_pipe, mock_view_handler):
        """View handlers should be registered."""
        assert "mock-view" in fusion_pipe.get_available_views()
        assert fusion_pipe.get_view_handler("mock-view") is mock_view_handler
    
    def test_config_parsing(self, fusion_pipe, pipe_config):
        """Configuration should be parsed correctly."""
        assert fusion_pipe.allow_concurrent_push == pipe_config["allow_concurrent_push"]
        assert fusion_pipe.queue_batch_size == pipe_config["queue_batch_size"]
    
    @pytest.mark.asyncio
    async def test_dto(self, fusion_pipe):
        """get_dto should return pipe info."""
        dto = await fusion_pipe.get_dto()
        assert dto["id"] == "test-pipe"
        assert dto["view_id"] == "1"
        assert "mock-view" in dto["view_handlers"]
        assert "statistics" in dto


class TestFusionPipeLifecycle:
    """Test FusionPipe start/stop lifecycle."""
    
    @pytest.mark.asyncio
    async def test_start(self, fusion_pipe, mock_view_handler):
        """start() should initialize handlers and start processing."""
        await fusion_pipe.start()
        
        assert fusion_pipe.is_running()
        assert mock_view_handler.initialized
        
        await fusion_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_stop(self, fusion_pipe, mock_view_handler):
        """stop() should close handlers."""
        await fusion_pipe.start()
        await fusion_pipe.stop()
        
        assert not fusion_pipe.is_running()
        assert mock_view_handler._closed


class TestFusionPipeSession:
    """Test session management."""
    
    @pytest.mark.asyncio
    async def test_session_created_first_is_leader(self, fusion_pipe, mock_view_handler):
        """First session should become leader."""
        from fustor_fusion.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fusion_pipe)
        
        await fusion_pipe.start()
        
        # Use bridge to create session (handles election and backing store)
        await bridge.create_session(task_id="agent:pipe", session_id="sess-1")
        
        assert await fusion_pipe.get_session_role("sess-1") == "leader"
        assert mock_view_handler.session_starts == 1
        
        await fusion_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_session_created_second_is_follower(self, fusion_pipe):
        """Second session should be follower."""
        from fustor_fusion.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fusion_pipe)
        
        await fusion_pipe.start()
        
        await bridge.create_session(task_id="agent1:pipe", session_id="sess-1")
        await bridge.create_session(task_id="agent2:pipe", session_id="sess-2")
        
        assert await fusion_pipe.get_session_role("sess-1") == "leader"
        assert await fusion_pipe.get_session_role("sess-2") == "follower"
        
        await fusion_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_leader_election_on_close(self, fusion_pipe):
        """New leader should be elected when leader leaves."""
        from fustor_fusion.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fusion_pipe)
        
        await fusion_pipe.start()
        
        await bridge.create_session(task_id="agent1:pipe", session_id="sess-1")
        await bridge.create_session(task_id="agent2:pipe", session_id="sess-2")
        
        # Close via bridge (which calls pipe.on_session_closed)
        await bridge.close_session("sess-1")
        
        assert await fusion_pipe.get_session_role("sess-2") == "leader"
        
        await fusion_pipe.stop()


class TestFusionPipeDto:
    """Test DTO and stats."""
    
    @pytest.mark.asyncio
    async def test_aggregated_stats(self, fusion_pipe, mock_view_handler):
        """Aggregated stats should include view stats."""
        await fusion_pipe.start()
        
        stats = await fusion_pipe.get_aggregated_stats()
        
        assert "pipe" in stats
        assert "views" in stats
        assert "mock-view" in stats["views"]
        
        await fusion_pipe.stop()
    
    def test_str_representation(self, fusion_pipe):
        """__str__ should return readable format."""
        s = str(fusion_pipe)
        assert "test-pipe" in s
        assert "STOPPED" in s


class TestFusionPipeViewHandler:
    """Test view handler operations."""
    
    def test_register_handler(self, fusion_pipe):
        """should be able to register additional handlers."""
        new_handler = MockViewHandler("another-view")
        fusion_pipe.register_view_handler(new_handler)
        
        assert "another-view" in fusion_pipe.get_available_views()
    
    @pytest.mark.asyncio
    async def test_get_view(self, fusion_pipe, mock_view_handler):
        """get_view should return handler's data view."""
        await fusion_pipe.start()
        
        view = fusion_pipe.get_view("mock-view")
        assert view == {"events_count": 0}
        
        await fusion_pipe.stop()
    
    def test_get_view_nonexistent(self, fusion_pipe):
        """get_view should return None for nonexistent handler."""
        view = fusion_pipe.get_view("nonexistent")
        assert view is None
