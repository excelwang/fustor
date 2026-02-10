import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi import HTTPException, status
from fustor_fusion.api.views import make_metadata_limit_checker
from fustor_fusion.config.unified import fusion_config, ViewConfig

@pytest.mark.asyncio
async def test_metadata_limit_checker_below_limit():
    """Test that checker passes when item count is within limits."""
    view_name = "test-view"
    limit = 100
    current_count = 50
    
    # Mock view config
    mock_view_config = MagicMock(spec=ViewConfig)
    mock_view_config.driver_params = {"max_tree_items": limit}
    
    # Mock driver instance
    mock_driver = AsyncMock()
    mock_driver.get_directory_stats.return_value = {"item_count": current_count}
    
    # Mock view manager
    mock_manager = MagicMock()
    mock_manager.driver_instances = {"fs": mock_driver}
    
    with patch("fustor_fusion.api.views.fusion_config") as mock_config_module, \
         patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get_manager:
        
        # Setup mocks
        mock_config_module.reload = MagicMock()
        mock_config_module.get_view.return_value = mock_view_config
        mock_get_manager.return_value = mock_manager
        
        # Create checker
        checker = make_metadata_limit_checker(view_name)
        
        # execution
        result = await checker(view_id="test-id")
        
        # Verification
        assert result is True
        mock_config_module.reload.assert_called_once()
        mock_driver.get_directory_stats.assert_awaited_once()

@pytest.mark.asyncio
async def test_metadata_limit_checker_exceeds_limit():
    """Test that checker raises HTTPException when item count exceeds limit."""
    view_name = "test-view"
    limit = 100
    current_count = 101
    
    # Mock view config (using driver_params per new implementation)
    mock_view_config = MagicMock(spec=ViewConfig)
    mock_view_config.driver_params = {"max_tree_items": limit}
    
    # Mock driver instance
    mock_driver = AsyncMock()
    mock_driver.get_directory_stats.return_value = {"item_count": current_count}
    
    # Mock view manager
    mock_manager = MagicMock()
    mock_manager.driver_instances = {"fs": mock_driver}
    
    with patch("fustor_fusion.api.views.fusion_config") as mock_config_module, \
         patch("fustor_fusion.api.views.get_cached_view_manager", new_callable=AsyncMock) as mock_get_manager:
        
        # Setup mocks
        mock_config_module.reload = MagicMock()
        mock_config_module.get_view.return_value = mock_view_config
        mock_get_manager.return_value = mock_manager
        
        # Create checker
        checker = make_metadata_limit_checker(view_name)
        
        # execution & verification
        with pytest.raises(HTTPException) as exc_info:
            await checker(view_id="test-id")
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        detail = exc_info.value.detail
        assert detail["error"] == "Metadata retrieval limit exceeded"
        assert detail["limit"] == limit
        assert detail["current_count"] == current_count

@pytest.mark.asyncio
async def test_metadata_limit_checker_no_limit_configured():
    """Test that checker passes when no limit is configured (default behavior or 0)."""
    view_name = "test-view"
    
    # Case 1: No param set (default 100000)
    mock_view_config_default = MagicMock(spec=ViewConfig)
    mock_view_config_default.driver_params = {} # Empty params
    
    with patch("fustor_fusion.api.views.fusion_config") as mock_config_module:
        mock_config_module.reload = MagicMock()
        mock_config_module.get_view.return_value = mock_view_config_default
        
        # We don't even need to mock the driver here because if limit is high enough it won't be hit easily,
        # BUT wait, make_metadata_limit_checker gets the default 100000 if key missing.
        # So it WILL try to check stats.
        
        # Let's test explicit unlimited (0)
        mock_view_config_unlimited = MagicMock(spec=ViewConfig)
        mock_view_config_unlimited.driver_params = {"max_tree_items": 0}
        mock_config_module.get_view.return_value = mock_view_config_unlimited
        
        checker = make_metadata_limit_checker(view_name)
        result = await checker(view_id="test-id")
        assert result is True

@pytest.mark.asyncio
async def test_metadata_limit_checker_view_not_found():
    """Test that checker passes safely if view config is missing (fail open)."""
    view_name = "unknown-view"
    
    with patch("fustor_fusion.api.views.fusion_config") as mock_config_module:
        mock_config_module.reload = MagicMock()
        mock_config_module.get_view.return_value = None
        
        checker = make_metadata_limit_checker(view_name)
        result = await checker(view_id="test-id")
        assert result is True
