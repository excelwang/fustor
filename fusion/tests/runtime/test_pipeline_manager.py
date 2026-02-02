import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from fustor_fusion.runtime.pipeline_manager import PipelineManager
from fustor_fusion.runtime.fusion_pipeline import FusionPipeline
from fustor_core.event import EventBase

@pytest.fixture
def mock_receivers_config():
    with patch("fustor_fusion.runtime.pipeline_manager.receivers_config") as mock:
        mock.reload.return_value = {}
        yield mock

@pytest.fixture
def mock_pipelines_config():
    with patch("fustor_fusion.runtime.pipeline_manager.fusion_pipelines_config") as mock:
        mock.reload.return_value = {}
        yield mock
        
@pytest.fixture
def pipeline_manager():
    return PipelineManager()

class TestPipelineManager:
    @pytest.mark.asyncio
    async def test_initialization_empty(self, pipeline_manager, mock_receivers_config, mock_pipelines_config):
        pipeline_manager.load_receivers()
        await pipeline_manager.initialize_pipelines()
        assert pipeline_manager._pipelines == {}
        assert pipeline_manager._receivers == {}

    @pytest.mark.asyncio
    async def test_initialization_with_http_receiver(self, pipeline_manager, mock_receivers_config, mock_pipelines_config):
        # Mock receiver config
        receiver_cfg = MagicMock()
        receiver_cfg.driver = "http"
        receiver_cfg.bind_host = "127.0.0.1"
        receiver_cfg.port = 8101
        receiver_cfg.session_timeout_seconds = 30
        receiver_cfg.api_keys = []
        
        
        mock_receivers_config.reload.return_value = {"http-main": receiver_cfg}
        
        # Create a dummy class to satisfy isinstance check
        class DummyHTTPReceiver:
            def __init__(self, *args, **kwargs): 
                pass
            def register_api_key(self, *args): pass
            async def start(self): pass
            def register_callbacks(self, **kwargs): pass

        with patch("fustor_fusion.runtime.pipeline_manager.HTTPReceiver", new=DummyHTTPReceiver):
            pipeline_manager.load_receivers()
            
            assert "http-main" in pipeline_manager._receivers
            assert isinstance(pipeline_manager._receivers["http-main"], DummyHTTPReceiver)

    @pytest.mark.asyncio
    async def test_initialization_with_pipeline(self, pipeline_manager, mock_receivers_config, mock_pipelines_config):
        # Mock pipeline config
        pipe_cfg = MagicMock()
        pipe_cfg.enabled = True
        pipe_cfg.views = ["view1"]
        pipe_cfg.extra = {"view_id": "1"}
        pipe_cfg.allow_concurrent_push = True
        pipe_cfg.session_timeout_seconds = 30
        
        mock_pipelines_config.reload.return_value = {"pipe-1": pipe_cfg}
        
        # Mock dependencies
        with patch("fustor_fusion.runtime.pipeline_manager.get_cached_view_manager", new_callable=AsyncMock) as mock_get_vm, \
             patch("fustor_fusion.runtime.pipeline_manager.create_view_handler_from_manager") as mock_create_handler, \
             patch("fustor_fusion.runtime.pipeline_manager.create_session_bridge") as mock_bridge:
                 
             mock_vm = MagicMock()
             mock_get_vm.return_value = mock_vm
             
             await pipeline_manager.initialize_pipelines()
             
             assert "pipe-1" in pipeline_manager._pipelines
             mock_get_vm.assert_called_with('1')

    @pytest.mark.asyncio
    async def test_callbacks(self, pipeline_manager):
        # Manually inject a mock pipeline and bridge
        mock_pipeline = AsyncMock(spec=FusionPipeline)
        mock_pipeline.get_session_role.return_value = "leader"
        mock_pipeline.get_session_info.return_value = {"id": "sess-1"}
        mock_pipeline.process_events.return_value = {"success": True}
        
        mock_bridge = AsyncMock()
        mock_bridge.create_session.return_value = {"role": "leader"}
        mock_bridge.keep_alive.return_value = {"status": "ok", "role": "leader"}
        
        pipeline_manager._pipelines["pipe-1"] = mock_pipeline
        pipeline_manager._bridges["pipe-1"] = mock_bridge
        
        # Test session created
        session_info = await pipeline_manager._on_session_created(
            "sess-1", "task-1", "pipe-1", {"client_ip": "1.2.3.4"}
        )
        assert session_info.session_id == "sess-1"
        assert session_info.role == "leader"
        # Bridge should be called, not pipeline directly (pipeline called via bridge)
        mock_bridge.create_session.assert_called_once()
        
        # Test event received
        events = []
        result = await pipeline_manager._on_event_received(
            "sess-1", events, "message", False
        )
        assert result is True
        mock_pipeline.process_events.assert_called_once()

