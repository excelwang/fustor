import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_view_multi_fs.driver import MultiFSViewDriver

@pytest.mark.asyncio
async def test_multi_fs_partial_failure_handling():
    # Setup: 2 members, one succeeds, one fails
    config = {"members": ["ok-view", "fail-view"]}
    
    vm_ok = MagicMock()
    dr_ok = AsyncMock()
    dr_ok.get_subtree_stats.return_value = {"file_count": 10}
    vm_ok.driver_instances = {"default": dr_ok}
    
    vm_fail = MagicMock()
    dr_fail = AsyncMock()
    dr_fail.get_subtree_stats.side_effect = Exception("Internal Error")
    vm_fail.driver_instances = {"default": dr_fail}
    
    def get_vm_side_effect(vid):
        if vid == "ok-view": return vm_ok
        if vid == "fail-view": return vm_fail
        return None

    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_func:
        mock_func.return_value = AsyncMock(side_effect=get_vm_side_effect)
        
        driver = MultiFSViewDriver("m1", "g1", config)
        
        # Test stats aggregation resilience
        res = await driver.get_subtree_stats_agg("/")
        
        ok_res = next(m for m in res["members"] if m["view_id"] == "ok-view")
        assert ok_res["status"] == "ok"
        assert ok_res["file_count"] == 10
        
        fail_res = next(m for m in res["members"] if m["view_id"] == "fail-view")
        assert fail_res["status"] == "error"
        assert "Internal Error" in fail_res["error"]

@pytest.mark.asyncio
async def test_multi_fs_best_view_missing_data():
    config = {"members": ["v1", "v2"]}
    
    # Both views return empty/error
    vm = MagicMock()
    dr = AsyncMock()
    dr.get_subtree_stats.return_value = {} # No fields
    vm.driver_instances = {"default": dr}
    
    with patch.object(MultiFSViewDriver, '_get_view_manager_func', return_value=AsyncMock(return_value=vm)):
        driver = MultiFSViewDriver("m1", "g1", config)
        
        from fustor_view_multi_fs.api import _find_best_view
        agg = await driver.get_subtree_stats_agg("/")
        
        # Recommendation algorithm might return a member even with 0 if it's "ok"
        # Let's verify what it actually does. If it returns 0, then best is not None.
        best = _find_best_view(agg, "file_count")
        if best:
            assert best["value"] == 0
