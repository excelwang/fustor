import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from fustor_agent.services.instances.pipe import PipeInstanceService
from fustor_agent.services.configs.pipe import PipeConfigService
from fustor_agent.services.configs.source import SourceConfigService
from fustor_agent.services.configs.sender import SenderConfigService
from fustor_agent.services.instances.bus import EventBusService
from fustor_agent.services.drivers.source_driver import SourceDriverService
from fustor_agent.services.drivers.sender_driver import SenderDriverService
from fustor_core.models.config import AppConfig, PipeConfig, SourceConfig, SenderConfig, PasswdCredential
from fustor_agent.config.unified import AgentPipeConfig

@pytest.fixture
def mock_services():
    app_config = AppConfig()
    source_cfg = SourceConfig(driver="fs", uri="/tmp", credential=PasswdCredential(user="u"))
    sender_cfg = SenderConfig(driver="echo", uri="http://loc", credential=PasswdCredential(user="u"))
    pipe_cfg = PipeConfig(source="s1", sender="se1", fields_mapping=[])
    
    app_config.add_source("s1", source_cfg)
    app_config.add_sender("se1", sender_cfg)
    app_config.add_pipe("p1", pipe_cfg)

    source_cfg_svc = SourceConfigService(app_config)
    sender_cfg_svc = SenderConfigService(app_config)
    pipe_cfg_svc = PipeConfigService(app_config, source_cfg_svc, sender_cfg_svc)
    
    source_dr_svc = SourceDriverService()
    sender_dr_svc = SenderDriverService()
    bus_svc = EventBusService(source_configs={"s1": source_cfg}, source_driver_service=source_dr_svc)
    
    service = PipeInstanceService(
        pipe_config_service=pipe_cfg_svc,
        source_config_service=source_cfg_svc,
        sender_config_service=sender_cfg_svc,
        bus_service=bus_svc,
        sender_driver_service=sender_dr_svc,
        source_driver_service=source_dr_svc,
        agent_id="test-agent"
    )
    return service, pipe_cfg_svc

@pytest.mark.asyncio
async def test_pipe_service_restart_outdated_pipes(mock_services):
    """Verify that restart_outdated_pipes stops existing and starts new instance."""
    service, pipe_cfg_svc = mock_services
    
    with patch("fustor_agent.services.configs.pipe.agent_config") as mock_agent_config:
        agent_pipe_config = AgentPipeConfig(source="s1", sender="se1")
        # We need to ensure the service's get_config returns our mocked YAML-like object
        # OR we mock the underlying config loader it uses.
        # PipeConfigService.get_config calls agent_config.get_pipe(id)
        mock_agent_config.get_pipe.return_value = agent_pipe_config
        
        with patch("fustor_agent.services.instances.pipe.AgentPipe") as MockPipe:
            # 1. Start initially
            await service.start_one("p1")
            assert "p1" in service.pool
            first_instance = service.pool["p1"]
            
            # 2. Mark as outdated
            await service.mark_dependent_pipes_outdated("source", "s1", "config changed")
            
            # 3. Restart outdated
            count = await service.restart_outdated_pipes()
            assert count == 1
            
            # Verify first was stopped
            first_instance.stop.assert_called_once()
            
            # Verify "p1" is still in pool (new one started)
            assert "p1" in service.pool
            assert service.pool["p1"] is not first_instance
            assert MockPipe.call_count == 2

@pytest.mark.asyncio
async def test_pipe_service_stop_all_cleans_up(mock_services):
    """Stop all should close bus service and stop all pipes."""
    service, _ = mock_services
    service.bus_service = MagicMock(spec=EventBusService)
    
    with patch("fustor_agent.services.configs.pipe.agent_config") as mock_agent_config:
        agent_pipe_config = AgentPipeConfig(source="s1", sender="se1")
        mock_agent_config.get_pipe.return_value = agent_pipe_config

        with patch("fustor_agent.services.instances.pipe.AgentPipe") as MockPipe:
            await service.start_one("p1")
            await service.stop_all()
            
            service.pool["p1"].stop.assert_called_once()
            service.bus_service.release_all_unused_buses.assert_called_once()
            assert len(service.pool) == 0
