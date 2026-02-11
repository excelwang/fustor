import asyncio
import pytest
from pathlib import Path
import time
import logging

from fustor_agent.runtime import AgentPipe
from fustor_agent.services.drivers.source_driver import SourceDriverService
from fustor_agent.services.drivers.sender_driver import SenderDriverService
from fustor_agent.services.instances.bus import EventBusService
from fustor_agent.services.instances.pipe import PipeInstanceService
from fustor_agent.services.configs.pipe import PipeConfigService
from fustor_agent.services.configs.source import SourceConfigService
from fustor_agent.services.configs.sender import SenderConfigService
from fustor_core.models.config import PipeConfig, SourceConfig, SenderConfig, PasswdCredential, FieldMapping, AppConfig

@pytest.fixture
def integration_configs(tmp_path: Path):
    source_config = SourceConfig(
        driver="fs", 
        uri=str(tmp_path),
        credential=PasswdCredential(user="test"),
        driver_params={"hot_data_cooloff_seconds": 0},
        disabled=False
    )
    sender_config = SenderConfig(
        driver="echo", 
        uri="http://localhost:8080", 
        credential=PasswdCredential(user="test"),
        batch_size=10,
        disabled=False
    )
    pipe_config = PipeConfig(
        source="test_source", 
        sender="test_sender",
        disabled=False,
        fields_mapping=[

            FieldMapping(to="target.file_path", source=["fs.files.file_path:0"]),
            FieldMapping(to="target.size", source=["fs.files.size:0"])
        ]
    )
    return pipe_config, source_config, sender_config

class MockSenderDriver:
    """Mock for EchoDriver to avoid real network if any, though Echo is usually local."""
    def __init__(self, **kwargs):
        self.id = "mock"
        self.config = {}
    async def connect(self): pass
    async def close(self): pass
    async def create_session(self, task_id): return {"session_id": "s1", "role": "leader"}
    async def heartbeat(self): return {"role": "leader"}
    async def send_events(self, events, source_type="message", is_end=False, metadata=None): return {"success": True}
    async def close_session(self): pass

@pytest.mark.asyncio
async def test_pipe_instance_service_integration(integration_configs, tmp_path: Path, caplog):
    """Integration test for PipeInstanceService using AgentPipe."""
    pipe_config, source_config, sender_config = integration_configs
    
    # Setup AppConfig
    app_config = AppConfig()
    app_config.add_source("test_source", source_config)
    app_config.add_sender("test_sender", sender_config)
    app_config.add_pipe("test_pipe", pipe_config)

    from unittest.mock import patch
    from fustor_agent.config.unified import AgentPipeConfig

    with patch("fustor_agent.services.configs.pipe.agent_config") as mock_agent_config:
        agent_pipe_config = AgentPipeConfig(
            source="test_source",
            sender="test_sender",
            disabled=False,
            fields_mapping=[
                {"to": "target.file_path", "source": ["fs.files.file_path:0"]},
                {"to": "target.size", "source": ["fs.files.size:0"]}
            ]
        )
        mock_agent_config.get_all_pipes.return_value = {"test_pipe": agent_pipe_config}
        mock_agent_config.get_pipe.return_value = agent_pipe_config
        
        # Initialize Services
        source_cfg_svc = SourceConfigService(app_config)
        sender_cfg_svc = SenderConfigService(app_config)
        pipe_cfg_svc = PipeConfigService(app_config, source_cfg_svc, sender_cfg_svc)
        
        source_dr_svc = SourceDriverService()
        sender_dr_svc = SenderDriverService()
        bus_svc = EventBusService(source_configs={"test_source": source_config}, source_driver_service=source_dr_svc)
        
        service = PipeInstanceService(
            pipe_config_service=pipe_cfg_svc,
            source_config_service=source_cfg_svc,
            sender_config_service=sender_cfg_svc,
            bus_service=bus_svc,
            sender_driver_service=sender_dr_svc,
            source_driver_service=source_dr_svc,
            agent_id="test-agent"
        )

        # Prepare some data
        test_file = tmp_path / "test1.txt"
        test_file.write_text("hello")

        # Act
        # Act
        with caplog.at_level(logging.INFO):
            try:
                await service.start_one("test_pipe")
                
                # Give some time for AgentPipe to run its sequence
                await asyncio.sleep(1)
                
                # Verify instance in pool
                instance = service.get_instance("test_pipe")
                assert instance is not None
                assert isinstance(instance, AgentPipe)
                
                # Check logs for pipe activity
                # We assert "start initiated successfully" instead of "Using AgentPipe"
                assert "Pipe instance 'test_pipe' start initiated successfully" in caplog.text
                assert "Snapshot sync phase complete" in caplog.text or "Starting message sync phase" in caplog.text

            finally:
                await service.stop_one("test_pipe")
                # Ensure bus service cleanup too
                await service.stop_all()
            
            assert service.get_instance("test_pipe") is None

