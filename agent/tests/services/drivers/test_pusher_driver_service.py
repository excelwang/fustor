import pytest
from unittest.mock import MagicMock, AsyncMock
from fustor_agent.services.drivers.pusher_driver import PusherDriverService
from fustor_core.exceptions import DriverError, ConfigError

@pytest.fixture
def pusher_driver_service(mocker):
    """Fixture for a PusherDriverService with patched discovery."""
    mocker.patch.object(PusherDriverService, '_discover_installed_drivers', return_value={})
    yield PusherDriverService()

@pytest.fixture
def mock_pusher_class():
    """Mocks a driver class that conforms to the PusherDriver ABC."""
    mock = MagicMock()
    # Mock class methods
    mock.get_wizard_steps = AsyncMock(return_value={"steps": [{"id": "s1"}]})
    mock.get_needed_fields = AsyncMock(return_value={"properties": {"field1": {"type": "string"}}})
    mock.test_connection = AsyncMock(return_value=(True, "Connected"))
    mock.check_privileges = AsyncMock(return_value=(True, "Privileges OK"))
    return mock

class TestPusherDriverService:

    def test_get_driver_by_type_success(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that a discovered driver class can be retrieved."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        driver_class = pusher_driver_service._get_driver_by_type("mock_pusher")
        assert driver_class == mock_pusher_class

    def test_get_driver_by_type_not_found(self, pusher_driver_service):
        """Tests that a DriverError is raised for a non-existent driver."""
        with pytest.raises(DriverError, match="Pusher driver 'non_existent' not found."):
            pusher_driver_service._get_driver_by_type("non_existent")

    @pytest.mark.asyncio
    async def test_get_wizard_definition_by_type(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that the service correctly proxies the call to get_wizard_steps."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        
        wizard = await pusher_driver_service.get_wizard_definition_by_type("mock_pusher")
        
        assert wizard == {"steps": [{"id": "s1"}]}
        mock_pusher_class.get_wizard_steps.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_test_connection(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that the service correctly proxies the call to test_connection."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        payload = {"endpoint": "http://test.com"}

        status, msg = await pusher_driver_service.test_connection("mock_pusher", **payload)

        assert status is True
        assert msg == "Connected"
        mock_pusher_class.test_connection.assert_awaited_once_with(**payload)

    @pytest.mark.asyncio
    async def test_check_privileges(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that the service correctly proxies the call to check_privileges."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        payload = {"endpoint": "http://test.com", "credential": {"user": "u"}}

        status, msg = await pusher_driver_service.check_privileges("mock_pusher", **payload)

        assert status is True
        assert msg == "Privileges OK"
        mock_pusher_class.check_privileges.assert_awaited_once_with(**payload)

    @pytest.mark.asyncio
    async def test_get_needed_fields(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that the service correctly proxies the call to get_needed_fields."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        payload = {"endpoint": "http://test.com"}

        fields = await pusher_driver_service.get_needed_fields("mock_pusher", **payload)

        assert fields == {"properties": {"field1": {"type": "string"}}}
        mock_pusher_class.get_needed_fields.assert_awaited_once_with(**payload)

    @pytest.mark.asyncio
    async def test_get_needed_fields_exception(self, pusher_driver_service, mock_pusher_class, mocker):
        """Tests that exceptions from the driver are caught and re-raised correctly."""
        mocker.patch.dict(pusher_driver_service._discovered_drivers, {"mock_pusher": mock_pusher_class})
        mock_pusher_class.get_needed_fields.side_effect = Exception("Test Exception")
        payload = {"endpoint": "http://test.com"}

        with pytest.raises(RuntimeError, match="Could not retrieve needed fields"):
            await pusher_driver_service.get_needed_fields("mock_pusher", **payload)