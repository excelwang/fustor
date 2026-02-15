
import pytest
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException
from fustor_fusion_mgmt.monitoring import list_pipes, get_pipe_info, get_global_stats
from fustor_fusion import runtime_objects

@pytest.mark.asyncio
async def test_monitoring_returns_initializing_when_not_ready():
    """
    Test that monitoring endpoints return 200 OK with 'initializing' status 
    when runtime_objects.pipe_manager is None (instead of 500/503).
    """
    # Ensure pipe_manager is None
    with patch.object(runtime_objects, "pipe_manager", None):
        
        # 1. list_pipes
        result = await list_pipes()
        assert result["meta"]["status"] == "initializing"
        assert result["data"] == {}
        
        # 2. get_pipe_info
        result_info = await get_pipe_info("any-id")
        assert result_info["meta"]["status"] == "initializing"
        
        # 3. get_global_stats
        result_stats = await get_global_stats(view_id="view-1")
        assert result_stats["meta"]["status"] == "initializing"
