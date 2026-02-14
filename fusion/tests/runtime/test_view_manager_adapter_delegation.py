import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.runtime.view_handler_adapter import ViewManagerAdapter
from fustor_fusion.view_manager.manager import ViewManager

@pytest.mark.asyncio
async def test_view_manager_adapter_delegation_regression():
    """
    Test that ViewManagerAdapter correctly delegates (or fails to delegate)
    lifecycle and role resolution methods to underlying drivers.
    """
    # 1. Setup Manager and Mock Driver
    manager = ViewManager("test-view")
    mock_driver = AsyncMock()
    mock_driver.resolve_session_role.return_value = {"role": "leader", "election_key": "scoped-key"}
    
    # Inject driver into manager
    manager.driver_instances["d1"] = mock_driver
    
    # 2. Creating Adapter
    adapter = ViewManagerAdapter(manager)
    
    # 3. Test resolve_session_role delegation
    # logic in a9c0b62 removed the loop over drivers, so we expect this to NOT call mock_driver
    # and instead try global election (which we'll mock)
    
    with patch("fustor_fusion.view_state_manager.view_state_manager") as mock_vsm:
        mock_vsm.try_become_leader = AsyncMock(return_value=True)
        mock_vsm.set_authoritative_session = AsyncMock()
        
        result = await adapter.resolve_session_role("sess-1", pipe_id="p1")
        
        # Check if driver was called
        # If regression exists, this assert likely fails (call count 0)
        # We verify what actually happens to confirm regression
        if mock_driver.resolve_session_role.call_count == 0:
            print("\n[CONFIRMED] resolve_session_role was NOT delegated to driver")
        else:
            print("\n[UNEXPECTED] resolve_session_role WAS delegated to driver")

        assert result["role"] == "leader" # Global election succeeded

    # 4. Test on_snapshot_complete delegation
    # ViewManagerAdapter does not implement on_snapshot_complete, so it should be a no-op (from base)
    if hasattr(adapter, 'on_snapshot_complete'):
        await adapter.on_snapshot_complete(session_id="sess-1", metadata={})
    else:
        # Base class has it but maybe not in mro if import issues? No, generic base has it
        # But if adapter doesn't override, it calls base which passes.
        # We need to check if driver was called.
        pass
        
    if mock_driver.on_snapshot_complete.call_count == 0:
        print("[CONFIRMED] on_snapshot_complete was NOT delegated to driver")
    else:
         print("[UNEXPECTED] on_snapshot_complete WAS delegated to driver")
         
    # Assertion to FAIL the test if regression is present, so we can see it clearly
    assert mock_driver.resolve_session_role.call_count > 0, "Regression: resolve_session_role not delegated"
    assert mock_driver.on_snapshot_complete.call_count > 0, "Regression: on_snapshot_complete not delegated"
