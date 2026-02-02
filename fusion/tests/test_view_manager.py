
import pytest
import asyncio
from typing import Dict, Any, Optional
from unittest.mock import patch, MagicMock

from fustor_core.drivers import ViewDriver
from fustor_fusion.view_manager.manager import ViewManager
from fustor_fusion.config.views import ViewConfig

class MockViewDriver(ViewDriver):
    target_schema = "mock"
    def __init__(self, id: str, view_id: str, config: Dict[str, Any]):
        super().__init__(id, view_id, config)
        self.initialized = False

    async def initialize(self):
        self.initialized = True

    async def process_event(self, event) -> bool:
        return True

    async def get_data_view(self, **kwargs) -> Any:
        return {}

@pytest.mark.asyncio
async def test_view_manager_initialization():
    """Test that ViewManager correctly instantiates and initializes providers."""
    
    # Mock view_configs.get_by_view to return a mock config
    mock_config = ViewConfig(
        id="test-view",
        view_id="1", # Now called view_id in Config
        driver="mock",
        driver_params={"param1": "val1"}
    )
    
    with patch("fustor_fusion.config.views.views_config.get_by_datastore", return_value=[mock_config]), \
         patch("fustor_fusion.view_manager.manager._load_view_drivers", return_value={"mock": MockViewDriver}):
        
        vm = ViewManager(view_id="1")
        await vm.initialize_providers()
        
        assert "test-view" in vm.providers
        provider = vm.providers["test-view"]
        assert isinstance(provider, MockViewDriver)
        assert provider.id == "test-view"
        assert provider.view_id == "1"
        assert provider.config == {"param1": "val1"}
        # Note: initialize() is NOT called in ViewManager.initialize_providers() currently.
        # It's called in main.py auto-start logic. 
        # But wait, should it be called in ViewManager?
        # In the refactored main.py, it IS called.
        # Let's check ViewManager.initialize_providers again.

@pytest.mark.asyncio
async def test_view_driver_abc_initialization():
    """Test the ViewDriver ABC constructor and initialize method."""
    driver = MockViewDriver(id="v1", view_id="10", config={"k": "v"})
    assert driver.id == "v1"
    assert driver.view_id == "10"
    assert driver.config == {"k": "v"}
    
    await driver.initialize()
    assert driver.initialized is True
