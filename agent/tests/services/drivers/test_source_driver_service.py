import pytest
from unittest.mock import MagicMock, AsyncMock
from fustor_agent.services.drivers.source_driver import SourceDriverService
from fustor_core.exceptions import DriverError, ConfigError

@pytest.fixture
def source_driver_service():
    return SourceDriverService()

@pytest.fixture
def mock_source_module():
    mock = MagicMock()
    mock.get_wizard_steps = MagicMock(return_value={"steps": []})
    mock.get_available_fields = AsyncMock(return_value={"field1": "type1"})
    mock.test_connection = AsyncMock(return_value=(True, "Connected"))
    mock.check_params = AsyncMock(return_value=(True, "Params OK"))
    mock.create_agent_user = AsyncMock(return_value=(True, "User created"))
    mock.check_privileges = AsyncMock(return_value=(True, "Privileges OK"))
    
    return mock

class TestSourceDriverService:
    def test_get_driver_by_type_success(self, source_driver_service, mock_source_module, mocker):
        mocker.patch.dict(source_driver_service._discovered_drivers, {"mock_source": mock_source_module})
        driver = source_driver_service._get_driver_by_type("mock_source")
        assert driver == mock_source_module

    def test_get_driver_by_type_invalid_type(self, source_driver_service):
        with pytest.raises(ConfigError, match="Driver type cannot be empty"):
            source_driver_service._get_driver_by_type(None)

    def test_get_driver_by_type_import_error(self, source_driver_service):
        with pytest.raises(DriverError, match="Source driver 'non_existent' not found."):
            source_driver_service._get_driver_by_type("non_existent")

    @pytest.mark.asyncio
    async def test_get_available_fields_success(self, source_driver_service, mock_source_module, mocker):
        mocker.patch.dict(source_driver_service._discovered_drivers, {"mock_source": mock_source_module})
        fields = await source_driver_service.get_available_fields("mock_source")
        assert fields == {"field1": "type1"}
        mock_source_module.get_available_fields.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_success(self, source_driver_service, mock_source_module, mocker):
        mocker.patch.dict(source_driver_service._discovered_drivers, {"mock_source": mock_source_module})
        status, msg = await source_driver_service.test_connection("mock_source")
        assert status is True
        assert msg == "Connected"
        mock_source_module.test_connection.assert_called_once()

    
