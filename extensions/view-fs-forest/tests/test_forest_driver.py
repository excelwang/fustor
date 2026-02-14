import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_view_fs_forest.driver import ForestFSViewDriver

@pytest.fixture
def forest_driver():
    config = {"hot_file_threshold": 30.0}
    return ForestFSViewDriver("forest-1", "global-view", config)

@pytest.mark.asyncio
async def test_process_event_routing(forest_driver):
    """Test that events are routed to the correct internal tree based on pipe_id."""
    # Mock the internal _get_or_create_tree method to return a mock tree
    mock_tree = AsyncMock()
    mock_tree.process_event.return_value = True
    
    with patch.object(forest_driver, "_get_or_create_tree", return_value=mock_tree) as mock_get_tree:
        # Event with pipe_id
        event = MagicMock()
        event.metadata = {"pipe_id": "pipe-A"}
        
        # 1. Process event
        result = await forest_driver.process_event(event)
        
        # Verify routing
        assert result is True
        mock_get_tree.assert_called_with("pipe-A")
        mock_tree.process_event.assert_called_with(event)

@pytest.mark.asyncio
async def test_process_event_no_pipe_id(forest_driver):
    """Test that events without pipe_id are dropped."""
    event = MagicMock()
    event.metadata = {}  # Missing pipe_id
    
    result = await forest_driver.process_event(event)
    assert result is False

@pytest.mark.asyncio
async def test_aggregation_stats(forest_driver):
    """Test aggregation of stats from multiple trees."""
    # Setup two mock trees
    tree_a = AsyncMock()
    tree_a.get_subtree_stats.return_value = {"file_count": 100, "status": "ok"}
    
    tree_b = AsyncMock()
    tree_b.get_subtree_stats.return_value = {"file_count": 50, "status": "ok"}
    
    forest_driver._trees = {"pipe-A": tree_a, "pipe-B": tree_b}
    
    # 1. Get aggregated stats
    result = await forest_driver.get_subtree_stats_agg("/")
    
    # Verify structure
    assert result["path"] == "/"
    assert len(result["members"]) == 2
    
    member_ids = [m["view_id"] for m in result["members"]]
    assert "pipe-A" in member_ids
    assert "pipe-B" in member_ids
    
    # Verify best selection (default file_count)
    assert result["best"]["view_id"] == "pipe-A"
    assert result["best"]["value"] == 100

@pytest.mark.asyncio
async def test_aggregation_tree(forest_driver):
    """Test full tree retrieval aggregation."""
    # Setup mock trees
    tree_a = AsyncMock()
    tree_a.get_directory_tree.return_value = {"name": "root-A"}
    
    tree_b = AsyncMock()
    tree_b.get_directory_tree.return_value = {"name": "root-B"}
    
    forest_driver._trees = {"pipe-A": tree_a, "pipe-B": tree_b}
    
    # 1. Get all trees
    result = await forest_driver.get_directory_tree("/")
    
    assert "pipe-A" in result["members"]
    assert result["members"]["pipe-A"]["data"]["name"] == "root-A"
    assert "pipe-B" in result["members"]
