import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from fustor_fusion.runtime.pipe_manager import PipeManager
from fustor_fusion.runtime.fusion_pipe import FusionPipe
from fustor_core.event import EventBase

@pytest.fixture
def mock_receivers_config():
    with patch("fustor_fusion.runtime.pipe_manager.fusion_config") as mock:
        mock.get_all_receivers.return_value = {}
        yield mock


        
@pytest.fixture
def pipe_manager():
    return PipeManager()

class TestPipeManager:
    @pytest.mark.asyncio
    async def test_initialization_empty(self, pipe_manager, mock_receivers_config):
        mock_receivers_config.get_default_pipes.return_value = {}
        await pipe_manager.initialize_pipes()
        assert pipe_manager._pipes == {}
        assert pipe_manager._receivers == {}

    @pytest.mark.asyncio
    async def test_initialization_with_http_receiver(self, pipe_manager, mock_receivers_config):
        # Mock receiver config
        receiver_cfg = MagicMock()
        receiver_cfg.driver = "http"
        receiver_cfg.bind_host = "127.0.0.1"
        receiver_cfg.port = 8101
        receiver_cfg.disabled = False
        receiver_cfg.session_timeout_seconds = 30
        receiver_cfg.api_keys = []
        
        
        mock_receivers_config.get_default_pipes.return_value = {} # No pipes, just receiver? 
        # Actually initialize_pipes only loads receivers IF they are used by a pipe.
        # So this test needs a pipe to trigger receiver loading.
        # Or we test internal methods? No, test public API.
        # If no pipes, no receivers are loaded in V2 logic (lazy loading per pipe).
        # So enable a dummy pipe to load the receiver.
        
        pipe_cfg = MagicMock()
        pipe_cfg.enabled = True
        pipe_cfg.disabled = False
        pipe_cfg.receiver = "http-main"
        pipe_cfg.views = []
        
        rec_cfg = receiver_cfg
        
        mock_receivers_config.get_all_pipes.return_value = {"pipe-1": pipe_cfg}
        mock_receivers_config.resolve_pipe_refs.return_value = {
            "pipe": pipe_cfg,
            "receiver": rec_cfg,
            "views": {}
        }

        mock_receivers_config.get_receiver.return_value = rec_cfg
        
        # Create a dummy class to satisfy isinstance check
        class DummyHTTPReceiver:
            def __init__(self, *args, **kwargs): 
                pass
            def register_api_key(self, *args): pass
            async def start(self): pass
            def register_callbacks(self, **kwargs): pass
            async def stop(self): pass

        with patch("fustor_fusion.runtime.pipe_manager.HTTPReceiver", new=DummyHTTPReceiver):
            await pipe_manager.initialize_pipes()
            
            assert pipe_manager.get_receiver("http-main") is not None
            assert isinstance(pipe_manager.get_receiver("http-main"), DummyHTTPReceiver)

    @pytest.mark.asyncio
    async def test_initialization_with_pipe(self, pipe_manager, mock_receivers_config):
        # Mock pipe config
        pipe_cfg = MagicMock()
        pipe_cfg.enabled = True
        pipe_cfg.disabled = False
        pipe_cfg.views = ["view1"]
        pipe_cfg.receiver = "http-main"
        
        rec_cfg = MagicMock()
        rec_cfg.driver = "http"
        rec_cfg.port = 8102
        rec_cfg.disabled = False
        rec_cfg.api_keys = []
        rec_cfg.session_timeout_seconds = 30
        
        mock_receivers_config.get_all_pipes.return_value = {"pipe-1": pipe_cfg}
        view_cfg = MagicMock()
        view_cfg.disabled = False
        mock_receivers_config.resolve_pipe_refs.return_value = {
            "pipe": pipe_cfg,
            "receiver": rec_cfg,
            "views": {"view1": view_cfg} # Need a view
        }
        
        # Mock dependencies
        with patch("fustor_fusion.runtime.pipe_manager.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm, \
             patch("fustor_fusion.runtime.pipe_manager.create_view_handler_from_manager") as mock_create_handler, \
             patch("fustor_fusion.runtime.pipe_manager.create_session_bridge") as mock_bridge:
                 
             mock_vm = MagicMock()
             mock_get_vm.return_value = mock_vm
             
             await pipe_manager.initialize_pipes()
             
             assert "pipe-1" in pipe_manager._pipes
             mock_get_vm.assert_called_with('view1')

    @pytest.mark.asyncio
    async def test_callbacks(self, pipe_manager):
        # Manually inject a mock pipe and bridge
        mock_pipe = AsyncMock(spec=FusionPipe)
        mock_pipe.get_session_role.return_value = "leader"
        mock_pipe.get_session_info.return_value = {"id": "sess-1"}
        mock_pipe.process_events.return_value = {"success": True}
        
        mock_bridge = AsyncMock()
        mock_bridge.create_session.return_value = {"role": "leader"}
        mock_bridge.keep_alive.return_value = {"status": "ok", "role": "leader"}
        
        pipe_manager._pipes["pipe-1"] = mock_pipe
        pipe_manager._bridges["pipe-1"] = mock_bridge
        
        # Test session created
        session_info = await pipe_manager._on_session_created(
            "sess-1", "task-1", "pipe-1", {"client_ip": "1.2.3.4"}, 60
        )
        assert session_info.session_id == "sess-1"
        assert session_info.role == "leader"
        # Bridge should be called, not pipe directly (pipe called via bridge)
        mock_bridge.create_session.assert_called_once()
        
        # Test event received
        events = []
        result = await pipe_manager._on_event_received(
            "sess-1", events, "message", False
        )
        assert result is True
        mock_pipe.process_events.assert_called_once()

