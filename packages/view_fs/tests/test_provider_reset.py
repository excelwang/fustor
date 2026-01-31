"""
Test FSViewProvider reset logic.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_view_fs.provider import FSViewProvider

@pytest.mark.asyncio
async def test_reset_calls_state_reset():
    """Test that reset() correctly resets the FSState."""
    provider = FSViewProvider(datastore_id="1", view_id="test_view", config={})
    
    # Mock the state object
    provider.state = MagicMock()
    provider.state.reset = MagicMock()
    
    # Needs to mock _global_exclusive_lock or _global_semaphore if they are complex,
    # but they are usually based on asyncio.Lock/Semaphore.
    # FSViewBase (parent) initializes them.
    
    await provider.reset()
    
    provider.state.reset.assert_called_once()
    
