import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.sender import SenderConfigService
from fustor_core.models.config import AppConfig, PusherConfig, SyncConfig, PasswdCredential

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_pushers.return_value = {}
    app_config.get_syncs.return_value = {}
    return app_config

@pytest.fixture
def pusher_config_service(mock_app_config):
    service = SenderConfigService(mock_app_config)
    service.sync_instance_service = MagicMock() # Mock the injected dependency
    return service

@pytest.fixture
def sample_pusher_config():
    return PusherConfig(driver="http", endpoint="http://localhost", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_sync_config(sample_pusher_config):
    return SyncConfig(source="source1", sender="pusher1", disabled=False)

class TestPusherConfigService:
    def test_set_dependencies(self, pusher_config_service):
        mock_sync_service = MagicMock()
        pusher_config_service.set_dependencies(mock_sync_service)
        assert pusher_config_service.sync_instance_service == mock_sync_service

    @pytest.mark.asyncio
    @patch('fustor_agent.services.common.config_lock')
    async def test_cleanup_obsolete_configs(self, mock_config_lock, pusher_config_service, mock_app_config):
        # Setup mock app_config state
        initial_pushers = {
            "rec1": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=True), # Obsolete
            "rec2": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=False), # Not obsolete (enabled)
            "rec3": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=True), # Obsolete
            "rec4": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=False), # Not obsolete (enabled)
        }
        mock_app_config.get_pushers.return_value = initial_pushers
        mock_app_config.get_syncs.return_value = {
            "sync1": SyncConfig(source="s1", sender="rec2", disabled=False),
            "sync2": SyncConfig(source="s1", sender="rec4", disabled=False),
        }

        # Mock the async context manager for config_lock
        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        deleted_ids = await pusher_config_service.cleanup_obsolete_configs()

        assert sorted(deleted_ids) == sorted(["rec1", "rec3"])
        assert "rec1" not in mock_app_config.get_pushers.return_value
        assert "rec3" not in mock_app_config.get_pushers.return_value
        assert "rec3" not in mock_app_config.get_pushers.return_value

    @pytest.mark.asyncio
    @patch('fustor_agent.services.common.config_lock')
    async def test_cleanup_obsolete_configs_no_obsolete(self, mock_config_lock, pusher_config_service, mock_app_config):
        initial_pushers = {
            "rec1": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=False),
            "rec2": PusherConfig(driver="d", endpoint="e", credential=PasswdCredential(user="u"), disabled=True), # Used by sync
        }
        mock_app_config.get_pushers.return_value = initial_pushers
        mock_app_config.get_syncs.return_value = {
            "sync1": SyncConfig(source="s1", sender="rec2", disabled=False),
        }

        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        deleted_ids = await pusher_config_service.cleanup_obsolete_configs()

        assert deleted_ids == []
