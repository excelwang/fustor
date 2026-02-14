import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import time
from fustor_fusion.runtime.audit_supervisor import check_audit_timeout

@pytest.fixture
def mock_runtime_objects():
    with patch("fustor_fusion.runtime.audit_supervisor.runtime_objects") as mock_ro:
        mock_pm = MagicMock()
        mock_ro.pipe_manager = mock_pm
        
        # Setup mocks
        mock_pipe = MagicMock()
        mock_pipe.id = "pipe-1"
        mock_pipe.config = {"audit_interval_sec": 1.0} # Short interval for test
        # mock_pipe.audit_interval_sec override
        mock_pipe.audit_interval_sec = 1.0
        
        mock_handler = AsyncMock() # ViewHandler
        mock_handler.id = "handler-1"
        
        # Initially no start time
        mock_handler.audit_start_time = None
        
        mock_pipe._view_handlers = {"h1": mock_handler}
        mock_pm.get_pipes.return_value = {"p1": mock_pipe}
        
        yield mock_pm, mock_pipe, mock_handler

@pytest.mark.asyncio
async def test_audit_no_timeout(mock_runtime_objects):
    """Test that audit is not cancelled if time is within limit."""
    mock_pm, mock_pipe, mock_handler = mock_runtime_objects
    
    # Start time is recent
    mock_handler.audit_start_time = time.time() - 0.5
    # Threshold is 1.0 * 2 = 2.0s
    
    await check_audit_timeout()
    
    mock_handler.handle_audit_end.assert_not_called()

@pytest.mark.asyncio
async def test_audit_timeout_detected(mock_runtime_objects):
    """Test that audit is cancelled if time exceeds limit."""
    mock_pm, mock_pipe, mock_handler = mock_runtime_objects
    
    # Start time is old (3s ago > 2s limit with default 2x multiplier)
    mock_handler.audit_start_time = time.time() - 3.0
    
    await check_audit_timeout()
    
    mock_handler.handle_audit_end.assert_called_once()
    mock_handler.reset_audit_tracking.assert_called_once()
    
@pytest.mark.asyncio
async def test_audit_timeout_manager_adapter(mock_runtime_objects):
    """Test timeout for ViewManagerAdapter structure."""
    mock_pm, mock_pipe, mock_handler = mock_runtime_objects
    
    mock_handler.audit_start_time = time.time() - 3.0
    
    await check_audit_timeout()
    
    mock_handler.handle_audit_end.assert_called_once()
    mock_handler.reset_audit_tracking.assert_called_once()
