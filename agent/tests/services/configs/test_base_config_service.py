import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.base import BaseConfigService
from fustor_core.models.config import AppConfig, SourceConfig, SenderConfig, PipeConfig, PasswdCredential
from fustor_core.exceptions import ConfigError, NotFoundError, ConflictError
from fustor_core.models.states import PipeState

# Define a simple mock config class for testing BaseConfigService
class MockConfig(SourceConfig):
    pass

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_sources.return_value = {}
    app_config.get_senders.return_value = {}
    app_config.get_pipes.return_value = {}
    app_config.add_source = MagicMock()
    app_config.add_sender = MagicMock()
    app_config.add_pipe = MagicMock()
    app_config.delete_source = MagicMock()
    app_config.delete_sender = MagicMock()
    app_config.delete_pipe = MagicMock()
    return app_config

@pytest.fixture
def mock_pipe_instance_service():
    service = MagicMock()
    service.mark_dependent_pipes_outdated = AsyncMock()
    service.stop_dependent_pipes = AsyncMock()
    service.stop_one = AsyncMock()
    service.get_instance = MagicMock()
    return service

@pytest.fixture
def base_config_service(mock_app_config, mock_pipe_instance_service):
    return BaseConfigService(mock_app_config, mock_pipe_instance_service, "source")

@pytest.fixture
def sample_source_config():
    return SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_sender_config():
    return SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_pipe_config():
    return PipeConfig(source="s1", sender="r1", disabled=False)

class TestBaseConfigService:
    @pytest.mark.asyncio
    async def test_add_config(self, base_config_service, mock_app_config, sample_source_config):
        mock_app_config.get_sources.return_value = {}
        mock_app_config.add_source.return_value = sample_source_config

        result = await base_config_service.add_config("test_source", sample_source_config)

        mock_app_config.add_source.assert_called_once_with("test_source", sample_source_config)
        assert result == sample_source_config

    @pytest.mark.asyncio
    async def test_update_config_enable_disable(self, base_config_service, mock_app_config, mock_pipe_instance_service, sample_source_config):
        # Setup initial state
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}

        # Test disabling
        updated_config = await base_config_service.update_config("test_source", {"disabled": True})
        assert updated_config.disabled is True
        mock_pipe_instance_service.mark_dependent_pipes_outdated.assert_called_once_with(
            "source", "test_source", "Dependency Source 'test_source' configuration was disabled.", {"disabled": True}
        )
        # Reset mocks
        mock_pipe_instance_service.mark_dependent_pipes_outdated.reset_mock()

        # Test enabling
        updated_config = await base_config_service.update_config("test_source", {"disabled": False})
        assert updated_config.disabled is False
        mock_pipe_instance_service.mark_dependent_pipes_outdated.assert_called_once_with(
            "source", "test_source", "Dependency Source 'test_source' configuration was enabled.", {"disabled": False}
        )

    @pytest.mark.asyncio
    async def test_update_config_non_disabled_field(self, base_config_service, mock_app_config, mock_pipe_instance_service, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}

        updated_config = await base_config_service.update_config("test_source", {"max_retries": 20})
        assert updated_config.max_retries == 20
        mock_pipe_instance_service.mark_dependent_pipes_outdated.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_config_source_sender(self, base_config_service, mock_app_config, mock_pipe_instance_service, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}
        mock_app_config.delete_source.return_value = sample_source_config

        result = await base_config_service.delete_config("test_source")

        mock_app_config.delete_source.assert_called_once_with("test_source")
        assert result == sample_source_config

    @pytest.mark.asyncio
    async def test_delete_config_pipe(self, mock_app_config, mock_pipe_instance_service, sample_pipe_config):
        pipe_service = BaseConfigService(mock_app_config, mock_pipe_instance_service, "pipe")
        mock_app_config.get_pipes.return_value = {"test_pipe": sample_pipe_config}
        mock_app_config.delete_pipe.return_value = sample_pipe_config

        result = await pipe_service.delete_config("test_pipe")

        mock_pipe_instance_service.stop_one.assert_called_once_with("test_pipe")
        mock_app_config.delete_pipe.assert_called_once_with("test_pipe")
        assert result == sample_pipe_config

    @pytest.mark.asyncio
    async def test_disable_config(self, base_config_service, mock_app_config, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}
        with patch.object(base_config_service, 'update_config', new=AsyncMock()) as mock_update:
            await base_config_service.disable("test_source")
            mock_update.assert_called_once_with("test_source", {'disabled': True})

    @pytest.mark.asyncio
    async def test_enable_config(self, base_config_service, mock_app_config, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}
        with patch.object(base_config_service, 'update_config', new=AsyncMock()) as mock_update:
            await base_config_service.enable("test_source")
            mock_update.assert_called_once_with("test_source", {'disabled': False})

    @pytest.mark.asyncio
    async def test_delete_config_with_dependency(self, base_config_service, mock_app_config, sample_source_config, sample_pipe_config):
        mock_app_config.get_sources.return_value = {"s1": sample_source_config}
        mock_app_config.get_pipes.return_value = {"pipe1": sample_pipe_config}

        with pytest.raises(ConflictError) as excinfo:
            await base_config_service.delete_config("s1")

        assert "used by the following pipes: pipe1" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_update_config_pipe_state_change(self, mock_app_config, mock_pipe_instance_service, sample_pipe_config):
        pipe_service = BaseConfigService(mock_app_config, mock_pipe_instance_service, "pipe")
        mock_app_config.get_pipes.return_value = {"test_pipe": sample_pipe_config}

        mock_instance = MagicMock()
        # --- REFACTORED: Use a valid v2 state ---
        mock_instance.state = PipeState.MESSAGE_SYNC
        mock_instance._set_state = MagicMock()
        mock_pipe_instance_service.get_instance.return_value = mock_instance

        await pipe_service.update_config("test_pipe", {"disabled": True})
        mock_instance._set_state.assert_called_once_with(PipeState.RUNNING_CONF_OUTDATE, "Dependency Pipe 'test_pipe' configuration was disabled.")

        mock_instance.state = PipeState.STOPPED
        mock_instance._set_state.reset_mock()
        await pipe_service.update_config("test_pipe", {"disabled": False})
        mock_instance._set_state.assert_not_called() # Should not set state if already stopped