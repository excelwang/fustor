import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.pipeline import PipelineConfigService
from fustor_core.models.config import AppConfig, PipelineConfig, PasswdCredential

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_pipelines.return_value = {}
    return app_config

@pytest.fixture
def mock_source_config_service():
    return MagicMock()

@pytest.fixture
def mock_sender_config_service():
    return MagicMock()

@pytest.fixture
def pipeline_config_service(mock_app_config, mock_source_config_service, mock_sender_config_service):
    service = PipelineConfigService(
        mock_app_config, 
        mock_source_config_service, 
        mock_sender_config_service
    )
    service.pipeline_instance_service = MagicMock() # Mock the injected dependency
    return service

@pytest.fixture
def sample_pipeline_config():
    return PipelineConfig(source="source1", sender="sender1", disabled=False)

class TestPipelineConfigService:
    def test_set_dependencies(self, pipeline_config_service):
        mock_pipeline_service = MagicMock()
        pipeline_config_service.set_dependencies(mock_pipeline_service)
        assert pipeline_config_service.pipeline_instance_service == mock_pipeline_service

    # Inherits most of its functionality from BaseConfigService.
    # Additional tests can be added here if PipelineConfigService introduces
    # unique logic beyond what BaseConfigService handles.
