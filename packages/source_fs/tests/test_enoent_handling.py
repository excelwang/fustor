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
        """
        Test the race condition where a directory is identified as 'hot' during the pre-scan phase,
        but is deleted before the watch can be established.
        
        Scenario:
        1. Pre-scan identifies '/path/to/hot/dir'.
        2. External process deletes '/path/to/hot/dir'.
        3. Agent attempts to call schedule('/path/to/hot/dir').
        4. inotify.add_watch fails with ENOENT.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock event handler
            mock_event_handler = MagicMock()
            
            # Create a WatchManager instance
            watch_manager = _WatchManager(
                root_path=temp_dir,
                event_handler=mock_event_handler,
                min_monitoring_window_days=30.0
            )
            
            # Simulate a path that was found during pre-scan but now doesn't exist
            hot_dir_path = os.path.join(temp_dir, "hot_directory_from_prescan")
            
            # Verify the path doesn't exist (simulating deletion after pre-scan)
            assert not os.path.exists(hot_dir_path)
            
            # Call schedule method - this simulates the agent attempting to watch the identified hot dir.
            # This should currently raise FileNotFoundError (bug reproduction).
            watch_manager.schedule(hot_dir_path)