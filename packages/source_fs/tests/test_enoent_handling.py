"""
Test case to verify the error handling for ENOENT (No such file or directory) 
in the filesystem source driver.
"""
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from fustor_source_fs.components import _WatchManager

class TestENOENTHandling:
    """Tests for handling ENOENT (No such file or directory) errors."""
    
    def test_schedule_nonexistent_path(self):
        """Test that schedule method handles non-existent paths gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock event handler
            mock_event_handler = MagicMock()
            
            # Create a WatchManager instance
            watch_manager = _WatchManager(
                root_path=temp_dir,
                event_handler=mock_event_handler,
                min_monitoring_window_days=30.0
            )
            
            # Create a path that doesn't exist
            nonexistent_path = os.path.join(temp_dir, "nonexistent", "path", "that", "does", "not", "exist")
            
            # Verify the path doesn't exist before scheduling
            assert not os.path.exists(nonexistent_path)
            
            # Call schedule method - this should currently raise FileNotFoundError (bug reproduction)
            watch_manager.schedule(nonexistent_path)