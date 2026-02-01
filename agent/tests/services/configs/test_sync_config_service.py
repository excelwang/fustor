import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.sync import SyncConfigService
from fustor_core.models.config import AppConfig, SyncConfig, PasswdCredential

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_syncs.return_value = {}
    return app_config

@pytest.fixture
def mock_source_config_service():
    return MagicMock()

@pytest.fixture
def mock_sender_config_service():
    return MagicMock()

@pytest.fixture
def sync_config_service(mock_app_config, mock_source_config_service, mock_sender_config_service):
    service = SyncConfigService(
        mock_app_config, 
        mock_source_config_service, 
        mock_sender_config_service
    )
    service.sync_instance_service = MagicMock() # Mock the injected dependency
    return service

@pytest.fixture
def sample_sync_config():
    return SyncConfig(source="source1", sender="pusher1", disabled=False)

class TestSyncConfigService:
    def test_set_dependencies(self, sync_config_service):
        mock_sync_service = MagicMock()
        sync_config_service.set_dependencies(mock_sync_service)
        assert sync_config_service.sync_instance_service == mock_sync_service

    # Inherits most of its functionality from BaseConfigService.
    # Additional tests can be added here if SyncConfigService introduces
    # unique logic beyond what BaseConfigService handles.
