import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_view_multi_fs.driver import MultiFSViewDriver

@pytest.fixture
def mock_member_vm():
    vm = MagicMock()
    driver = AsyncMock()
    driver.get_subtree_stats.return_value = {
        "file_count": 10, "dir_count": 2, "total_size": 100, "latest_mtime": 1000.0
    }
    driver.get_directory_tree.return_value = {"name": "root"}
    vm.driver_instances = {"default": driver}
    return vm, driver

@pytest.mark.asyncio
async def test_multi_fs_driver_stats(mock_member_vm):
    vm, driver = mock_member_vm
    config = {"members": ["v1"]}
    
    # In MultiFSViewDriver, get_cached_view_manager is imported inside _get_view_manager_func
    # So we need to patch the return value of _get_view_manager_func
    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_func:
        mock_func.return_value = AsyncMock(return_value=vm)
        
        multi_driver = MultiFSViewDriver("m1", "g1", config)
        # MultiFSViewDriver has no initialize override currently
        
        stats = await multi_driver.get_subtree_stats_agg("/")
        assert stats["path"] == "/"
        assert stats["members"][0]["view_id"] == "v1"
        assert stats["members"][0]["file_count"] == 10

@pytest.mark.asyncio
async def test_multi_fs_driver_tree(mock_member_vm):
    vm, driver = mock_member_vm
    config = {"members": ["v1"]}
    
    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_func:
        mock_func.return_value = AsyncMock(return_value=vm)
        
        multi_driver = MultiFSViewDriver("m1", "g1", config)
        
        # Test basic tree aggregation
        tree = await multi_driver.get_directory_tree("/")
        assert "v1" in tree["members"]
        assert tree["members"]["v1"]["data"] == {"name": "root"}
        
        # Test best_view optimization
        tree2 = await multi_driver.get_directory_tree("/", best_view="v1")
        assert len(tree2["members"]) == 1
        assert "v1" in tree2["members"]

@pytest.mark.asyncio
async def test_multi_fs_driver_timeout(mock_member_vm):
    vm, driver = mock_member_vm
    config = {"members": ["v1", "v2"], "query_timeout": 0.1}
    
    # Mock v1 as fast, v2 as slow
    async def slow_fetch(*args, **kwargs):
        await asyncio.sleep(0.5)
        return {"name": "slow"}
        
    driver.get_directory_tree.side_effect = slow_fetch
    
    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_func:
        mock_func.return_value = AsyncMock(return_value=vm)
        
        multi_driver = MultiFSViewDriver("m1", "g1", config)
        
        # Test tree aggregation with one slow member
        tree = await multi_driver.get_directory_tree("/")
        assert "v1" in tree["members"]
        assert "v2" in tree["members"]
        assert tree["members"]["v2"]["status"] == "error"
        assert tree["members"]["v2"]["error"] == "Timeout"
