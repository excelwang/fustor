"""
Test case to reproduce the surrogate character handling bug in the filesystem source driver.
"""
import os
import tempfile
import pytest
from unittest.mock import MagicMock

from fustor_source_fs.components import _WatchManager

def test_schedule_surrogate_path_raises_unicode_error():
    """
    Test that scheduling a watch with a surrogate path raises UnicodeEncodeError
    in the current implementation (reproducing the bug).
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
        
        # Path with surrogate character
        path_with_surrogate = f"{temp_dir}/\udca3test"
        
        # This call is expected to raise UnicodeEncodeError (bug reproduction).
        watch_manager.schedule(path_with_surrogate)