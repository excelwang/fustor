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
        
        pipe_cfg = MagicMock()
        pipe_cfg.enabled = True
        pipe_cfg.disabled = False
        pipe_cfg.receiver = "http-main"
        pipe_cfg.views = []
        
        rec_cfg = receiver_cfg
        
        mock_receivers_config.get_all_pipes.return_value = {"pipe-1": pipe_cfg}
        mock_receivers_config.get_enabled_pipes.return_value = {"pipe-1": pipe_cfg}
        
        mock_receivers_config.resolve_pipe_refs.return_value = {
            "pipe": pipe_cfg,
            "receiver": rec_cfg,
            "views": {}
        }

        mock_receivers_config.get_receiver.return_value = rec_cfg
        
        # Create a dummy class to satisfy isinstance check
        class DummyHTTPReceiver:
            def __init__(self, *args, **kwargs): 
                self.id = "recv_http_8101"
            def register_api_key(self, *args): pass
            async def start(self): pass
            def register_callbacks(self, **kwargs): pass
            async def stop(self): pass
            def mount_router(self, router): pass

        # Patch ReceiverRegistry.create instead of HTTPReceiver directly
        with patch("fustor_core.transport.ReceiverRegistry.create", return_value=DummyHTTPReceiver()) as mock_create:
            await pipe_manager.initialize_pipes()
            
            # Verify receiver is created and accessible
            # Note: get_receiver takes receiver ID or signature? 
            # In code: self._receivers[r_sig] = receiver where r_sig = (driver, port)
            # PipeManager doesn't expose get_receiver by name publicly in previous view, 
            # but let's assume valid access or check internal state
            r_sig = ("http", 8101)
            assert r_sig in pipe_manager._receivers
            assert isinstance(pipe_manager._receivers[r_sig], DummyHTTPReceiver)

    @pytest.mark.asyncio
    async def test_initialization_with_pipe(self, pipe_manager, mock_receivers_config):
        # Mock pipe config
        pipe_cfg = MagicMock()
        pipe_cfg.enabled = True
        pipe_cfg.disabled = False
        pipe_cfg.views = ["view1"]
        pipe_cfg.receiver = "http-main"
        pipe_cfg.allow_concurrent_push = True
        pipe_cfg.session_timeout_seconds = 30
        
        rec_cfg = MagicMock()
        rec_cfg.driver = "http"
        rec_cfg.port = 8102
        rec_cfg.disabled = False
        rec_cfg.api_keys = []
        rec_cfg.session_timeout_seconds = 30
        
        mock_receivers_config.get_all_pipes.return_value = {"pipe-1": pipe_cfg}
        mock_receivers_config.get_enabled_pipes.return_value = {"pipe-1": pipe_cfg}
        
        view_cfg = MagicMock()
        view_cfg.disabled = False
        mock_receivers_config.resolve_pipe_refs.return_value = {
            "pipe": pipe_cfg,
            "receiver": rec_cfg,
            "views": {"view1": view_cfg}
        }
        
        # Mock dependencies - note patching where they are DEFINED or imported strictly
        with patch("fustor_fusion.runtime.pipe_manager.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm, \
             patch("fustor_fusion.runtime.view_handler_adapter.create_view_handler_from_manager") as mock_create_handler, \
             patch("fustor_fusion.runtime.session_bridge.create_session_bridge") as mock_bridge, \
             patch("fustor_core.transport.ReceiverRegistry.create") as mock_recv_create:
                 
             mock_vm = MagicMock()
             mock_get_vm.return_value = mock_vm
             
             await pipe_manager.initialize_pipes()
             
             assert "pipe-1" in pipe_manager._pipes
             mock_get_vm.assert_called_with('view1')

    @pytest.mark.asyncio
    async def test_callbacks(self, pipe_manager):
        # Manually inject a mock pipe and bridge
        mock_pipe = AsyncMock(spec=FusionPipe)
        mock_pipe.id = "pipe-1"
        mock_pipe.view_id = "view-1"
        mock_pipe.get_session_role.return_value = "leader"
        mock_pipe.get_session_info.return_value = {"id": "sess-1"}
        mock_pipe.process_events.return_value = {"success": True}
        
        mock_bridge = AsyncMock()
        mock_bridge.create_session.return_value = {"role": "leader"}
        mock_bridge.keep_alive.return_value = {"status": "ok", "role": "leader"}
        
        pipe_manager._pipes["pipe-1"] = mock_pipe
        # Manually injecting bridge since we check it
        pipe_manager._bridges["pipe-1"] = mock_bridge
        pipe_manager._session_to_pipe["sess-1"] = "pipe-1"
        
        # Test session created
        session_info = await pipe_manager._on_session_created(
            "sess-1", "task-1", "pipe-1", {"client_ip": "1.2.3.4"}, 60
        )
        # Note: on_session_created returns what create_session returns (from bridge) or pipe?
        # Let's check implementation. It calls _bridges[...].create_session
        assert session_info.role == "leader"
        
        # Test event received
        events = []
        result = await pipe_manager._on_event_received(
            "sess-1", events, "message", False
        )
        assert result is True
        mock_pipe.process_events.assert_called_once()
