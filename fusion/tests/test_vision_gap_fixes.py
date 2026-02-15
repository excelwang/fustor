
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException, status

# Mock before importing views to avoid import errors if dependencies missing
# Because views.py imports from runtime_objects, config, etc.
# We need to ensure these are importable or mocked.
# Assuming standard environment setup is fine for imports.

from fustor_fusion.api.views import make_readiness_checker, FallbackDriverWrapper
from fustor_fusion import runtime_objects

# Mock view_state_manager globally because it's imported inside functions
mock_vsm_module = MagicMock()
mock_vsm = AsyncMock()
mock_vsm.get_state.return_value = MagicMock(authoritative_session_id="sess-1")
mock_vsm.is_snapshot_complete.return_value = True
mock_vsm_module.view_state_manager = mock_vsm

@pytest.fixture
def mock_view_manager():
    with patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get:
        manager = MagicMock()
        manager.driver_instances = {"view-1": MagicMock()}
        mock_get.return_value = manager
        yield mock_get

@pytest.mark.asyncio
async def test_readiness_checker_bypass_if_fallback_enabled(mock_view_manager):
    """
    Test that readiness checker bypasses 503 if fallback is enabled.
    Simulates: Not ready state (snapshot incomplete), but has fallback.
    Expected: No exception raised.
    """
    # Setup state: Snapshot INCOMPLETE
    with patch.dict("sys.modules", {"fustor_fusion.view_state_manager": mock_vsm_module}):
        mock_vsm.is_snapshot_complete.return_value = False
        
        # Scenario 1: Fallback ENABLED -> Should Pass (Bypass 503)
        with patch.object(runtime_objects, "on_command_fallback", AsyncMock()):
            checker = make_readiness_checker("view-1")
            # Should not raise exception
            await checker(authorized_identity="view-1")

        # Scenario 2: Fallback DISABLED -> Should Raise 503
        with patch.object(runtime_objects, "on_command_fallback", None):
            checker = make_readiness_checker("view-1")
            with pytest.raises(HTTPException) as exc:
                await checker(authorized_identity="view-1")
            assert exc.value.status_code == 503

@pytest.mark.asyncio
async def test_wrapper_checks_readiness_and_triggers_fallback():
    """
    Test that FallbackDriverWrapper explicitly checks readiness and triggers fallback
    if the view is not ready (closing the loop with the checker bypass).
    """
    mock_driver = MagicMock()
    # Direct assignment so iscoroutinefunction returns True
    called = False
    async def real_get_data_view(*args, **kwargs):
        nonlocal called
        called = True
        return {"status": "ok"}
    mock_driver.get_data_view = real_get_data_view
    
    wrapper = FallbackDriverWrapper(mock_driver, "view-1")
    
    # Setup: Snapshot INCOMPLETE
    # The wrapper imports view_state_manager internally
    with patch.dict("sys.modules", {"fustor_fusion.view_state_manager": mock_vsm_module}):
        mock_vsm.is_snapshot_complete.return_value = False
        
        # Scenario 1: Fallback ENABLED -> Should Trigger Fallback (because wrapper detects not ready and raises error)
        mock_fallback = AsyncMock(return_value={"fallback": "data"})
        # We need to mock on_command_fallback to verify it gets called
        with patch.object(runtime_objects, "on_command_fallback", mock_fallback):
            result = await wrapper.get_data_view(path="/")
            
            # Verify fallback was called
            mock_fallback.assert_called_once()
            # Verify result matches fallback data
            assert result == {"fallback": "data"}
            
            # Verify driver was NOT called (failed fast)
            assert not called
