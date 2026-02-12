import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from fustor_view_multi_fs.driver import MultiFSViewDriver
from fustor_view_multi_fs.api import _find_best_view

@pytest.fixture
def member_drivers():
    d1 = AsyncMock()
    d1.get_subtree_stats.return_value = {
        "file_count": 100, "dir_count": 10, "total_size": 1000, "latest_mtime": 200.0
    }
    d1.get_directory_tree.return_value = {"name": "root", "children": ["a"]}

    d2 = AsyncMock()
    d2.get_subtree_stats.return_value = {
        "file_count": 50, "dir_count": 5, "total_size": 500, "latest_mtime": 100.0
    }
    d2.get_directory_tree.return_value = {"name": "root", "children": ["b"]}
    
    return {"v1": d1, "v2": d2}

@pytest.fixture
def mock_get_manager(member_drivers):
    async def _get_manager(view_id):
        manager = MagicMock()
        if view_id in member_drivers:
            manager.driver_instances = {"default": member_drivers[view_id]}
        else:
            manager.driver_instances = {}
        return manager
    return _get_manager

@pytest.mark.asyncio
async def test_driver_aggregation(mock_get_manager):
    config = {"driver_params": {"members": ["v1", "v2", "missing"]}}
    
    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_method:
        mock_method.return_value = mock_get_manager
        driver = MultiFSViewDriver("test-multi", "view-grp", config)
        
        # Test stats aggregation
        stats_result = await driver.get_subtree_stats_agg("/")
        assert stats_result["path"] == "/"
        assert len(stats_result["members"]) == 3
        
        v1_res = next(m for m in stats_result["members"] if m.get("view_id") == "v1")
        assert v1_res["status"] == "ok"
        assert v1_res["file_count"] == 100
        
        v3_res = next(m for m in stats_result["members"] if m.get("view_id") == "missing")
        assert v3_res["status"] == "error"
        
        # Test tree aggregation
        tree_result = await driver.get_directory_tree("/")
        members = tree_result["members"]
        assert "v1" in members
        assert members["v1"]["data"]["children"] == ["a"]
        assert "v2" in members
        assert members["v2"]["data"]["children"] == ["b"]
        assert "missing" in members
        assert members["missing"]["status"] == "error"

def test_find_best_view():
    agg = {
        "members": [
            {"view_id": "v1", "status": "ok", "file_count": 100, "total_size": 1000},
            {"view_id": "v2", "status": "ok", "file_count": 200, "total_size": 500},
            {"view_id": "v3", "status": "error"}
        ]
    }
    
    # Best file count -> v2
    best = _find_best_view(agg, "file_count")
    assert best["view_id"] == "v2"
    assert best["value"] == 200
    
    # Best total size -> v1
    best = _find_best_view(agg, "total_size")
    assert best["view_id"] == "v1"
    assert best["value"] == 1000

@pytest.mark.asyncio
async def test_driver_tree_best_view(mock_get_manager, member_drivers):
    config = {"driver_params": {"members": ["v1", "v2"]}}
    
    with patch.object(MultiFSViewDriver, '_get_view_manager_func') as mock_method:
        mock_method.return_value = mock_get_manager
        driver = MultiFSViewDriver("test-multi", "view-grp", config)
        
        # Query only best view v2
        res = await driver.get_directory_tree("/", best_view="v2")
        members = res["members"]
        assert "v2" in members
        assert "v1" not in members # Should prioritize efficiency
        
        # Ensure v1 was NOT called
        member_drivers["v1"].get_directory_tree.assert_not_called()
        member_drivers["v2"].get_directory_tree.assert_called()
