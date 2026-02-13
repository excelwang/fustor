import pytest
from unittest.mock import AsyncMock, patch
from fustor_fusion.view_manager.services import get_view_status
from fustor_fusion.view_manager.background import task_status, _extract_timestamp
from datetime import datetime

@pytest.mark.asyncio
async def test_get_view_status_success():
    mock_manager = AsyncMock()
    mock_manager.get_aggregated_stats.return_value = {"total_volume": 100}
    
    # Correct patching target: where it's used or where it's imported
    with patch("fustor_fusion.view_manager.manager.get_cached_view_manager", return_value=mock_manager):
        status = await get_view_status("v1")
        assert status["view_id"] == "v1"
        assert status["status"] == "active"
        assert status["total_items"] == 100

@pytest.mark.asyncio
async def test_get_view_status_error():
    with patch("fustor_fusion.view_manager.manager.get_cached_view_manager", side_effect=Exception("Failed")):
        status = await get_view_status("v1")
        assert status["status"] == "error"
        assert "Failed" in status["error"]

def test_background_task_status():
    # task_status is a global instance
    task_status.update_status("v1", "task1", "running", {"detail": 1})
    s = task_status.get_status("v1", "task1")
    assert s["status"] == "running"
    assert s["details"] == {"detail": 1}
    
    assert task_status.get_status("v2") is None
    assert "task1" in task_status.get_all_status()["v1"]

def test_extract_timestamp():
    # Float
    dt = _extract_timestamp({"modified_time": 1700000000.0})
    assert isinstance(dt, datetime)
    assert dt.timestamp() == 1700000000.0
    
    # ISO String
    dt2 = _extract_timestamp({"created_time": "2024-01-01T12:00:00Z"})
    assert dt2.year == 2024
    
    # Invalid
    assert _extract_timestamp({"modified_time": "invalid"}) is None
    assert _extract_timestamp({}) is None
