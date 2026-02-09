"""
Test FSViewDriver reset logic.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_view_fs.driver import FSViewDriver

@pytest.mark.asyncio
async def test_reset_calls_state_reset():
    """Test that reset() correctly resets the FSState."""
    driver = FSViewDriver(id="test_view", view_id="1", config={})
    
    # Mock the state object
    driver.state = MagicMock()
    driver.state.reset = MagicMock()
    
    # Needs to mock _global_exclusive_lock or _global_semaphore if they are complex,
    # but they are usually based on asyncio.Lock/Semaphore.
    # FSViewBase (parent) initializes them.
    
    await driver.reset()
    
    driver.state.reset.assert_called_once()
