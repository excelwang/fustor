"""
Test case to verify the fix for the surrogate character handling bug in the filesystem source driver.
"""
import os
import tempfile
import pytest
from unittest.mock import MagicMock

from fustor_source_fs.components import _WatchManager, safe_path_encode, safe_path_handling

def test_schedule_surrogate_path_is_watched_successfully():
    """
    Test that scheduling a watch with a surrogate path no longer raises UnicodeEncodeError,
    and the watch is successfully established.
    The path is first created on disk to simulate an existing 'hot' directory.
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
        
        # Define a path with a surrogate character.
        # Use os.fsencode to create a byte representation of a problematic filename if needed.
        # On Linux, Python's os.fsdecode can turn non-UTF-8 filenames into strings with surrogates.
        # So we simulate such a path.
        problematic_filename_bytes = b"test_dir\xed\xa0\x80" # Example: an invalid UTF-8 sequence, becomes a surrogate in str
        
        # Create the directory on disk using the byte representation.
        # This part assumes the underlying filesystem and OS can handle such names.
        # If not, this test might need to be skipped or mocked more heavily.
        surrogate_dir_path_bytes = os.path.join(os.fsencode(temp_dir), problematic_filename_bytes)
        os.mkdir(surrogate_dir_path_bytes)
        
        # Get the Python string representation of this path, which will contain surrogates
        path_with_surrogate = os.fsdecode(surrogate_dir_path_bytes)
        
        # Ensure the path really exists
        assert os.path.exists(path_with_surrogate)
        
        # Call schedule method. It should now handle the surrogate path without crashing.
        # Since the path exists, it should be successfully added to the watch manager.
        try:
            watch_manager.schedule(path_with_surrogate)
            # Assert that the path is now in the LRU cache (i.e., successfully scheduled)
            assert path_with_surrogate in watch_manager.lru_cache
            # Assert that inotify.add_watch was called with the safely encoded path
            # (assuming mock on add_watch if we don't want to use real inotify events)
            # For now, just checking it doesn't crash is sufficient for bug fix.
            # No explicit assertion about mock add_watch needed unless we mock it.
        except UnicodeEncodeError:
            pytest.fail("Bug NOT fixed: watch_manager.schedule raised UnicodeEncodeError on surrogate path.")
        except Exception as e:
            pytest.fail(f"watch_manager.schedule raised an unexpected exception: {type(e).__name__}: {e}")