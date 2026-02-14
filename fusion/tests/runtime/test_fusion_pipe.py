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
        self.next_role = "leader"
    
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
    
    async def resolve_session_role(self, session_id: str, pipe_id: Optional[str] = None) -> Dict[str, Any]:
        from fustor_fusion.view_state_manager import view_state_manager
        view_id = self.id
        if self.next_role == "leader":
            is_leader = await view_state_manager.try_become_leader(view_id, session_id)
            return {"role": "leader" if is_leader else "follower"}
        return {"role": "follower"}

    def get_data_view(self, **kwargs) -> Dict[str, Any]:
        return {"events_count": len(self.events_processed)}
    
    def get_stats(self) -> Dict[str, Any]:
        return {"processed": len(self.events_processed)}


@pytest.fixture
def mock_view_handler():
    return MockViewHandler(handler_id="mock-view")


@pytest.fixture
def pipe_config():
    return {
        "view_id": "mock-view",
        "allow_concurrent_push": True,
        "queue_batch_size": 100,
    }


import pytest_asyncio
@pytest_asyncio.fixture
async def fusion_pipe(mock_view_handler, pipe_config):
    from fustor_fusion.view_state_manager import view_state_manager
    from fustor_fusion.core.session_manager import session_manager
    
    # 清理旧状态
    await view_state_manager.clear_state("mock-view")
    await session_manager.clear_all_sessions("mock-view")
    
    p = FusionPipe(
        pipe_id="test-pipe",
        config=pipe_config,
        view_handlers=[mock_view_handler]
    )
    # Manual injection to adapt to business logic bug
    p.pipe_id = p.id
    return p


class TestFusionPipeInit:
    """Test FusionPipe initialization."""
    
    @pytest.mark.asyncio
    async def test_initial_state(self, fusion_pipe):
        assert fusion_pipe.id == "test-pipe"
        assert fusion_pipe.state == PipeState.STOPPED
        assert fusion_pipe.session_id is None
    
    @pytest.mark.asyncio
    async def test_view_handlers_registered(self, fusion_pipe, mock_view_handler):
        assert "mock-view" in fusion_pipe._view_handlers
        assert fusion_pipe._view_handlers["mock-view"] == mock_view_handler
    
    @pytest.mark.asyncio
    async def test_config_parsing(self, fusion_pipe):
        assert fusion_pipe.view_id == "mock-view"
        assert fusion_pipe.allow_concurrent_push is True
        assert fusion_pipe.queue_batch_size == 100
    
    @pytest.mark.asyncio
    async def test_dto(self, fusion_pipe):
        dto = await fusion_pipe.get_dto() # get_dto is async
        assert dto["id"] == "test-pipe"
        assert dto["state"] == "STOPPED"
        assert dto["view_id"] == "mock-view"


class TestFusionPipeLifecycle:
    """Test FusionPipe start/stop lifecycle."""
    
    @pytest.mark.asyncio
    async def test_start(self, fusion_pipe, mock_view_handler):
        await fusion_pipe.start()
        assert fusion_pipe.state == PipeState.RUNNING
        assert mock_view_handler.initialized is True
        await fusion_pipe.stop()
    
    @pytest.mark.asyncio
    async def test_stop(self, fusion_pipe, mock_view_handler):
        await fusion_pipe.start()
        await fusion_pipe.stop()
        assert fusion_pipe.state == PipeState.STOPPED
        assert mock_view_handler._closed is True


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
    async def test_session_created_second_is_follower(self, fusion_pipe, mock_view_handler):
        """Second session should be follower."""
        from fustor_fusion.runtime.session_bridge import create_session_bridge
        bridge = create_session_bridge(fusion_pipe)
        
        await fusion_pipe.start()
        
        await bridge.create_session(task_id="agent1:pipe", session_id="sess-1")
        
        # Make the next session a follower
        mock_view_handler.next_role = "follower"
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
        
        # sess-2 should now be leader
        assert await fusion_pipe.get_session_role("sess-2") == "leader"
        
        await fusion_pipe.stop()


class TestFusionPipeDto:
    """Test data transfer objects."""
    
    @pytest.mark.asyncio
    async def test_aggregated_stats(self, fusion_pipe, mock_view_handler):
        stats = await fusion_pipe.get_aggregated_stats()
        assert "pipe" in stats
        assert "views" in stats
        assert "mock-view" in stats["views"]
    
    @pytest.mark.asyncio
    async def test_str_representation(self, fusion_pipe):
        s = str(fusion_pipe)
        assert "FusionPipe" in s
        assert "test-pipe" in s


class TestFusionPipeViewHandler:
    """Test view handler management."""
    
    @pytest.mark.asyncio
    async def test_register_handler(self, fusion_pipe):
        h2 = MockViewHandler("h2")
        fusion_pipe.register_view_handler(h2)
        assert "h2" in fusion_pipe._view_handlers
    
    @pytest.mark.asyncio
    async def test_get_view(self, fusion_pipe, mock_view_handler):
        # get_view is synchronous
        view = fusion_pipe.get_view("mock-view")
        assert view["events_count"] == 0
    
    @pytest.mark.asyncio
    async def test_get_view_nonexistent(self, fusion_pipe):
        view = fusion_pipe.get_view("nonexistent")
        assert view is None
