
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException, status

from fustor_fusion.api.views import FallbackDriverWrapper
from fustor_fusion import runtime_objects

@pytest.mark.asyncio
async def test_wrapper_centralized_readiness_logic():
    """
    Test that FallbackDriverWrapper correctly implements the centralized readiness check.
    It should check VSM state and trigger fallback if not ready, WITHOUT relying on router dependency.
    """
    mock_driver = MagicMock()
    # Mock get_data_view to simulate "Success" if called (which it shouldn't be if not ready)
    async def real_get_data_view(*args, **kwargs):
        print("Driver called!")
        return {"status": "ok"}
    mock_driver.get_data_view = real_get_data_view
    
    wrapper = FallbackDriverWrapper(mock_driver, "view-1")
    
    # Use standard patch to intercept the import inside the method
    # "fustor_fusion.view_state_manager.view_state_manager" refers to the 'view_state_manager' instance 
    # inside the 'fustor_fusion.view_state_manager' module.
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        # Check snapshot logic
        mock_vsm.is_snapshot_complete = AsyncMock(return_value=False)
        mock_vsm.get_state = AsyncMock(return_value=MagicMock(authoritative_session_id="sess-1"))
        
        # Scenario 1: Fallback ENABLED -> Should Trigger Fallback
        mock_fallback = AsyncMock(return_value={"fallback": "data"})
        
        # Dynamically attach fallback handler to runtime_objects
        with patch.object(runtime_objects, "on_command_fallback", mock_fallback, create=True):
            result = await wrapper.get_data_view(path="/")
            
            # Verify fallback was called
            mock_fallback.assert_called_once()
            # Verify result matches fallback data
            assert result == {"fallback": "data"}
            
        # Scenario 2: Fallback DISABLED -> Should Raise 503
        with patch.object(runtime_objects, "on_command_fallback", None, create=True):
            with pytest.raises(HTTPException) as exc:
                await wrapper.get_data_view(path="/")
            assert exc.value.status_code == 503
            # Matches the code in views.py
            assert "performing initial synchronization" in str(exc.value.detail) or "View Not Ready" in str(exc.value.detail)
