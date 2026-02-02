import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.services.configs.base import BaseConfigService
from fustor_core.models.config import AppConfig, SourceConfig, SenderConfig, PipelineConfig, PasswdCredential
from fustor_core.exceptions import ConfigError, NotFoundError, ConflictError
from fustor_core.models.states import PipelineState

# Define a simple mock config class for testing BaseConfigService
class MockConfig(SourceConfig):
    pass

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_sources.return_value = {}
    app_config.get_senders.return_value = {}
    app_config.get_pipelines.return_value = {}
    app_config.add_source = MagicMock()
    app_config.add_sender = MagicMock()
    app_config.add_pipeline = MagicMock()
    app_config.delete_source = MagicMock()
    app_config.delete_sender = MagicMock()
    app_config.delete_pipeline = MagicMock()
    return app_config

@pytest.fixture
def mock_pipeline_instance_service():
    service = MagicMock()
    service.mark_dependent_pipelines_outdated = AsyncMock()
    service.stop_dependent_pipelines = AsyncMock()
    service.stop_one = AsyncMock()
    service.get_instance = MagicMock()
    return service

@pytest.fixture
def base_config_service(mock_app_config, mock_pipeline_instance_service):
    return BaseConfigService(mock_app_config, mock_pipeline_instance_service, "source")

@pytest.fixture
def sample_source_config():
    return SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_sender_config():
    return SenderConfig(driver="http", uri="http://localhost", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_pipeline_config():
    return PipelineConfig(source="s1", sender="r1", disabled=False)

class TestBaseConfigService:
    @pytest.mark.asyncio
    async def test_add_config(self, base_config_service, mock_app_config, sample_source_config):
        mock_app_config.get_sources.return_value = {}
        mock_app_config.add_source.return_value = sample_source_config

        result = await base_config_service.add_config("test_source", sample_source_config)

        mock_app_config.add_source.assert_called_once_with("test_source", sample_source_config)
        assert result == sample_source_config

    @pytest.mark.asyncio
    async def test_update_config_enable_disable(self, base_config_service, mock_app_config, mock_pipeline_instance_service, sample_source_config):
        # Setup initial state
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}

        # Test disabling
        updated_config = await base_config_service.update_config("test_source", {"disabled": True})
        assert updated_config.disabled is True
        mock_pipeline_instance_service.mark_dependent_pipelines_outdated.assert_called_once_with(
            "source", "test_source", "Dependency Source 'test_source' configuration was disabled.", {"disabled": True}
        )
        # Reset mocks
        mock_pipeline_instance_service.mark_dependent_pipelines_outdated.reset_mock()

        # Test enabling
        updated_config = await base_config_service.update_config("test_source", {"disabled": False})
        assert updated_config.disabled is False
        mock_pipeline_instance_service.mark_dependent_pipelines_outdated.assert_called_once_with(
            "source", "test_source", "Dependency Source 'test_source' configuration was enabled.", {"disabled": False}
        )

    @pytest.mark.asyncio
    async def test_update_config_non_disabled_field(self, base_config_service, mock_app_config, mock_pipeline_instance_service, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}

        updated_config = await base_config_service.update_config("test_source", {"max_retries": 20})
        assert updated_config.max_retries == 20
        mock_pipeline_instance_service.mark_dependent_pipelines_outdated.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_config_source_sender(self, base_config_service, mock_app_config, mock_pipeline_instance_service, sample_source_config):
        mock_app_config.get_sources.return_value = {"test_source": sample_source_config}
        mock_app_config.delete_source.return_value = sample_source_config

        result = await base_config_service.delete_config("test_source")

        mock_app_config.delete_source.assert_called_once_with("test_source")
        assert result == sample_source_config

    @pytest.mark.asyncio
    async def test_delete_config_pipeline(self, mock_app_config, mock_pipeline_instance_service, sample_pipeline_config):
        pipeline_service = BaseConfigService(mock_app_config, mock_pipeline_instance_service, "pipeline")
        mock_app_config.get_pipelines.return_value = {"test_pipeline": sample_pipeline_config}
        mock_app_config.delete_pipeline.return_value = sample_pipeline_config

        result = await pipeline_service.delete_config("test_pipeline")

        mock_pipeline_instance_service.stop_one.assert_called_once_with("test_pipeline")
        mock_app_config.delete_pipeline.assert_called_once_with("test_pipeline")
        assert result == sample_pipeline_config

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
    async def test_delete_config_with_dependency(self, base_config_service, mock_app_config, sample_source_config, sample_pipeline_config):
        mock_app_config.get_sources.return_value = {"s1": sample_source_config}
        mock_app_config.get_pipelines.return_value = {"pipeline1": sample_pipeline_config}

        with pytest.raises(ConflictError) as excinfo:
            await base_config_service.delete_config("s1")

        assert "used by the following pipeline tasks: pipeline1" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_update_config_pipeline_state_change(self, mock_app_config, mock_pipeline_instance_service, sample_pipeline_config):
        pipeline_service = BaseConfigService(mock_app_config, mock_pipeline_instance_service, "pipeline")
        mock_app_config.get_pipelines.return_value = {"test_pipeline": sample_pipeline_config}

        mock_instance = MagicMock()
        # --- REFACTORED: Use a valid v2 state ---
        mock_instance.state = PipelineState.MESSAGE_PHASE
        mock_instance._set_state = MagicMock()
        mock_pipeline_instance_service.get_instance.return_value = mock_instance

        await pipeline_service.update_config("test_pipeline", {"disabled": True})
        mock_instance._set_state.assert_called_once_with(PipelineState.RUNNING_CONF_OUTDATE, "Dependency Pipeline 'test_pipeline' configuration was disabled.")

        mock_instance.state = PipelineState.STOPPED
        mock_instance._set_state.reset_mock()
        await pipeline_service.update_config("test_pipeline", {"disabled": False})
        mock_instance._set_state.assert_not_called() # Should not set state if already stopped