import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.view_manager.manager import ViewManager, reset_views, get_cached_view_manager

@pytest.fixture
def mock_driver():
    driver = AsyncMock()
    driver.target_schema = "fs"
    driver.get_data_view.return_value = {"tree": "root"}
    return driver

@pytest.mark.asyncio
async def test_view_manager_lifecycle(mock_driver):
    config_mock = MagicMock()
    config_mock.driver = "fs"
    config_mock.driver_params = {"p1": 1}
    
    with patch("fustor_fusion.view_manager.manager._load_view_drivers", return_value={"fs": MagicMock(return_value=mock_driver)}):
        with patch("fustor_fusion.view_manager.manager.fusion_config.get_view", return_value=config_mock):
            vm = ViewManager("view1")
            await vm.initialize_driver_instances()
            
            assert "view1" in vm.driver_instances
            mock_driver.initialize.assert_called_once()
            
            # ViewManager has no close() method, skip testing it.

@pytest.mark.asyncio
async def test_reset_views_integration():
    mock_vm = AsyncMock()
    with patch("fustor_fusion.view_manager.manager.runtime_objects.view_managers", {"group1": mock_vm}):
        await reset_views("group1")
        mock_vm.reset.assert_called_once()

@pytest.mark.asyncio
async def test_view_manager_process_event(mock_driver):
    vm = ViewManager("g1")
    vm.driver_instances["v1"] = mock_driver
    
    event = MagicMock()
    event.event_schema = "fs"
    
    await vm.process_event(event)
    mock_driver.process_event.assert_called_once_with(event)

@pytest.mark.asyncio
async def test_view_manager_get_data_view(mock_driver):
    vm = ViewManager("g1")
    vm.driver_instances["v1"] = mock_driver
    
    # Test explicit driver_id
    res = await vm.get_data_view(driver_id="v1")
    assert res == {"tree": "root"}
    
    # Test nonexistent driver
    res2 = await vm.get_data_view(driver_id="v2")
    assert res2 is None
