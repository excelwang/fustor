import asyncio
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

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
    
    # Create a mock for EventBusInstanceRuntime
    mock_bus_instance_runtime = MagicMock()
    mock_bus_instance_runtime.id = "mock-bus-id"

    bus_svc = EventBusService(source_configs={"s1": source_cfg}, source_driver_service=source_dr_svc)
    bus_svc.get_or_create_bus_for_subscriber = AsyncMock(return_value=(mock_bus_instance_runtime, False))
    
    # Patch discovery before creating services
    with patch.object(SourceDriverService, "_discover_installed_drivers", return_value={}), \
         patch.object(SenderDriverService, "_discover_installed_drivers", return_value={}):
        service = PipeInstanceService(
            pipe_config_service=pipe_cfg_svc,
            source_config_service=source_cfg_svc,
            sender_config_service=sender_cfg_svc,
            bus_service=bus_svc,
            sender_driver_service=sender_dr_svc,
            source_driver_service=source_dr_svc,
        )
        service.source_driver_service._get_driver_by_type = MagicMock(return_value=MagicMock())
        service.sender_driver_service._get_driver_by_type = MagicMock(return_value=MagicMock())
    return service, pipe_cfg_svc

@pytest.mark.asyncio
async def test_pipe_service_restart_outdated_pipes(mock_services):
    """Verify that restart_outdated_pipes stops existing and starts new instance."""
    service, pipe_cfg_svc = mock_services
    
    mock_pipe_instance_1 = AsyncMock()
    mock_pipe_instance_1.id = "p1"
    mock_pipe_instance_1.task_id = "test-agent:p1"
    mock_pipe_instance_1.state = MagicMock()
    mock_pipe_instance_1.info = ""
    mock_pipe_instance_1.bus = MagicMock(id="mock-bus-id-1")


    mock_pipe_instance_2 = AsyncMock()
    mock_pipe_instance_2.id = "p1"
    mock_pipe_instance_2.task_id = "test-agent:p1"
    mock_pipe_instance_2.state = MagicMock()
    mock_pipe_instance_2.info = ""
    mock_pipe_instance_2.bus = MagicMock(id="mock-bus-id-2")


    with patch("fustor_agent.services.configs.pipe.agent_config") as mock_agent_config, \
         patch("fustor_agent.services.instances.pipe.AgentPipe", side_effect=[mock_pipe_instance_1, mock_pipe_instance_2]) as MockAgentPipeClass:
        
        agent_pipe_config = AgentPipeConfig(source="s1", sender="se1")
        mock_agent_config.get_pipe.return_value = agent_pipe_config
        
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
        assert MockAgentPipeClass.call_count == 2

@pytest.mark.asyncio
async def test_pipe_service_stop_all_cleans_up(mock_services):
    """Stop all should close bus service and stop all pipes."""
    service, _ = mock_services
    service.bus_service = MagicMock(spec=EventBusService)
    
    mock_pipe_instance = AsyncMock() # Single mock instance
    mock_pipe_instance.id = "p1"
    mock_pipe_instance.bus = MagicMock(id="mock-bus-id-1") # Ensure bus attribute exists

    with patch("fustor_agent.services.configs.pipe.agent_config") as mock_agent_config, \
         patch("fustor_agent.services.instances.pipe.AgentPipe", return_value=mock_pipe_instance) as MockAgentPipeClass:
        
        agent_pipe_config = AgentPipeConfig(source="s1", sender="se1")
        mock_agent_config.get_pipe.return_value = agent_pipe_config

        # Mock bus_service.get_or_create_bus_for_subscriber as it is called in start_one
        service.bus_service.get_or_create_bus_for_subscriber = AsyncMock(return_value=(MagicMock(id="mock-bus-for-stop-all"), False))
        service.bus_service.release_all_unused_buses = AsyncMock() # Ensure this is awaitable

        await service.start_one("p1")
        
        # Capture the instance before stop_all removes it from the pool
        captured_instance = service.pool["p1"]

        await service.stop_all()
            
        captured_instance.stop.assert_called_once()
        service.bus_service.release_all_unused_buses.assert_called_once()
        assert len(service.pool) == 0
