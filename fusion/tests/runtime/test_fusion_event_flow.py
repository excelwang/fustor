import pytest
import pytest_asyncio
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.runtime.fusion_pipe import FusionPipe
from fustor_core.pipe.handler import ViewHandler

class TestFusionEventFlow:
    """
    Test suite for FusionPipe event flow and signal handling.
    """

    @pytest_asyncio.fixture
    async def pipe(self):
        config = {"view_ids": ["view-main"]}
        pipe = FusionPipe("pipe-test", config)
        pipe._set_state = MagicMock() # Mock state transitions
        pipe._handlers_ready.set()    # Avoid initialization timeout
        
        # Setup handlers
        self.handler1 = AsyncMock(spec=ViewHandler)
        self.handler1.id = "h1"
        self.handler1.schema_name = "test-schema"
        
        # Mock view_id property on handler (for fan-out test)
        self.handler1.view_id = "view-other" 
        
        pipe.register_view_handler(self.handler1)
        
        # Mock running state
        with patch.object(pipe, 'is_running', return_value=True):
            yield pipe
            
    @pytest.mark.asyncio
    async def test_snapshot_end_leader(self, pipe):
        """Verify snapshot end signal from leader marks view complete."""
        session_id = "sess-leader"
        view_id = "view-main"
        
        mock_vsm = AsyncMock()
        mock_vsm.is_leader.return_value = True
        
        with patch.dict('sys.modules', {
            'fustor_fusion.view_state_manager': MagicMock(view_state_manager=mock_vsm)
        }):
            # Process Snapshot End
            await pipe.process_events([], session_id, source_type="snapshot", is_end=True)
            
            # Verify Main View Complete
            mock_vsm.set_snapshot_complete.assert_any_await(view_id, session_id)
            
            # Verify Fan-out to handler's view
            mock_vsm.set_snapshot_complete.assert_any_await("view-other", session_id)

    @pytest.mark.asyncio
    async def test_snapshot_end_follower(self, pipe):
        """Verify snapshot end signal from follower is IGNORED."""
        session_id = "sess-follower"
        
        mock_vsm = AsyncMock()
        mock_vsm.is_leader.return_value = False
        
        with patch.dict('sys.modules', {
            'fustor_fusion.view_state_manager': MagicMock(view_state_manager=mock_vsm)
        }):
            # Process Snapshot End
            await pipe.process_events([], session_id, source_type="snapshot", is_end=True)
            
            # Verify NO calls to set_snapshot_complete
            mock_vsm.set_snapshot_complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_audit_end_propagation(self, pipe):
        """Verify audit end waits for drain and triggers handler hook."""
        session_id = "sess-audit"
        
        # Mock wait_for_drain to ensure it's called
        pipe.wait_for_drain = AsyncMock()
        
        # Process Audit End
        await pipe.process_events([], session_id, source_type="audit", is_end=True)
        
        # Verify Drain wait
        pipe.wait_for_drain.assert_awaited()
        
        # Verify Handler Hook
        self.handler1.handle_audit_end.assert_awaited()
