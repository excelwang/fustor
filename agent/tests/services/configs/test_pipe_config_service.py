import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.pipe import PipeConfigService
from fustor_core.models.config import AppConfig, PipeConfig, PasswdCredential

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_pipes.return_value = {}
    return app_config

@pytest.fixture
def mock_source_config_service():
    return MagicMock()

@pytest.fixture
def mock_sender_config_service():
    return MagicMock()

@pytest.fixture
def pipe_config_service(mock_app_config, mock_source_config_service, mock_sender_config_service):
    service = PipeConfigService(
        mock_app_config, 
        mock_source_config_service, 
        mock_sender_config_service
    )
    service.pipe_instance_service = MagicMock() # Mock the injected dependency
    return service

@pytest.fixture
def sample_pipe_config():
    return PipeConfig(source="source1", sender="sender1", disabled=False)

class TestPipeConfigService:
    def test_set_dependencies(self, pipe_config_service):
        mock_pipe_service = MagicMock()
        pipe_config_service.set_dependencies(mock_pipe_service)
        assert pipe_config_service.pipe_instance_service == mock_pipe_service

    # Inherits most of its functionality from BaseConfigService.
    # Additional tests can be added here if PipeConfigService introduces
    # unique logic beyond what BaseConfigService handles.
