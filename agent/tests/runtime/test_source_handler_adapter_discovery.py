import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fustor_agent.runtime.source_handler_adapter import SourceHandlerAdapter
from fustor_core.drivers import SourceDriver

class MockDriver:
    def __init__(self, id="test-driver"):
        self.id = id
        self.initialize = AsyncMock()
        self.get_available_fields = AsyncMock(return_value={"schema": "ok"})
        self.require_schema_discovery = True

@pytest.mark.asyncio
async def test_source_handler_adapter_discovery_enabled():
    """Verify that get_available_fields is called when require_schema_discovery is True."""
    driver = MockDriver()
    handler = SourceHandlerAdapter(driver)
    
    await handler.initialize()
    
    assert driver.initialize.called
    assert driver.get_available_fields.called

@pytest.mark.asyncio
async def test_source_handler_adapter_discovery_disabled():
    """Verify that get_available_fields is NOT called when require_schema_discovery is False."""
    driver = MockDriver()
    driver.require_schema_discovery = False
    handler = SourceHandlerAdapter(driver)
    
    await handler.initialize()
    
    assert driver.initialize.called
    assert not driver.get_available_fields.called

@pytest.mark.asyncio
async def test_source_handler_adapter_discovery_failure():
    """Verify that initialization fails if discovery fails."""
    driver = MockDriver()
    driver.get_available_fields.side_effect = Exception("Discovery Error")
    handler = SourceHandlerAdapter(driver)
    
    with pytest.raises(RuntimeError) as excinfo:
        await handler.initialize()
    
    assert "Schema discovery failed" in str(excinfo.value)
