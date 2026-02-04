"""
Test cases to verify that the touch operation, os.walk calls, and event handlers
handle exceptions properly, especially when encountering permission errors or inaccessible paths.
"""
import os
import tempfile
import pytest
import shutil
from unittest.mock import patch, MagicMock

from watchdog.events import FileSystemEvent
from fustor_source_fs.components import _WatchManager
from fustor_source_fs.event_handler import OptimizedWatchEventHandler, get_file_metadata
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.event import UpdateEvent, DeleteEvent
import queue


def test_touch_handles_permission_error():
    """Test that touch method handles permission errors gracefully without crashing."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a subdirectory and restrict permissions
        restricted_dir = os.path.join(temp_dir, "restricted")
        os.makedirs(restricted_dir)
        
        # Make the directory unreadable (on Unix systems)
        original_mode = os.stat(restricted_dir).st_mode
        os.chmod(restricted_dir, 0o000)  # Remove all permissions
        
        try:
            # Create WatchManager instance directly (no need for full config for this test)
            watch_manager = _WatchManager(temp_dir, event_handler=None)
            
            # Mock the schedule method to isolate the touch method test
            with patch.object(watch_manager, 'schedule') as mock_schedule:
                # Call touch on the restricted directory - this should not crash
                watch_manager.touch(restricted_dir)
                
                # Verify that no exception was raised and the method completed
                # The schedule method might not be called due to permission error
                # but the touch method should complete without exception
                pass
                
        finally:
            # Restore permissions to allow cleanup
            os.chmod(restricted_dir, original_mode)


def test_touch_with_nonexistent_path():
    """Test that touch method handles nonexistent paths gracefully."""
    with tempfile.TemporaryDirectory() as temp_dir:
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Path that doesn't exist
        nonexistent_path = os.path.join(temp_dir, "nonexistent", "path")
        
        # This should not raise an exception
        watch_manager.touch(nonexistent_path)


def test_touch_with_file_instead_of_dir():
    """Test that touch method handles regular files properly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create a regular file
        test_file = os.path.join(temp_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test content")
        
        # This should not raise an exception
        watch_manager.touch(test_file)


def test_os_path_isdir_exception_handling():
    """Test direct exception handling in os.path.isdir calls."""
    with tempfile.TemporaryDirectory() as temp_dir:
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Test with patched os.path.isdir that raises an exception
        with patch('os.path.isdir', side_effect=PermissionError("Permission denied")):
            # This should not raise an exception due to our error handling
            watch_manager.touch(temp_dir)


def test_event_handler_on_created_exception_handling():
    """Test that on_created method handles exceptions without terminating the program."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Create a mock event
        mock_event = MagicMock()
        mock_event.is_directory = True
        mock_event.src_path = os.path.join(temp_dir, "test_dir")
        
        # Create the test directory
        os.makedirs(mock_event.src_path, exist_ok=True)
        
        # Patch get_file_metadata to raise an exception
        with patch('fustor_source_fs.event_handler.get_file_metadata', side_effect=PermissionError("Permission denied")):
            # This should not raise an exception due to our error handling
            handler.on_created(mock_event)
            
            # Verify that no events were added due to the exception
            # (though touch operation should still run for directories)
            # The method should complete without crashing


def test_event_handler_on_deleted_exception_handling():
    """Test that on_deleted method handles exceptions without terminating the program."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Create a mock event
        mock_event = MagicMock()
        mock_event.is_directory = False  # file, not directory
        mock_event.src_path = os.path.join(temp_dir, "test_file.txt")
        
        # Create the test file
        with open(mock_event.src_path, 'w') as f:
            f.write("test")
        
        # Patch watch_manager methods to raise exceptions
        with patch.object(watch_manager, 'touch', side_effect=PermissionError("Permission denied")):
            # This should not raise an exception due to our error handling
            handler.on_deleted(mock_event)
            
            # Method should complete without crashing


def test_event_handler_on_moved_exception_handling():
    """Test that on_moved method handles exceptions without terminating the program."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Create mock event with source and destination paths
        mock_event = MagicMock()
        mock_event.is_directory = False  # file, not directory
        mock_event.src_path = os.path.join(temp_dir, "old_file.txt")
        mock_event.dest_path = os.path.join(temp_dir, "new_file.txt")
        
        # Create the source file
        with open(mock_event.src_path, 'w') as f:
            f.write("test")
        
        # Create the destination file
        with open(mock_event.dest_path, 'w') as f:
            f.write("test")
        
        # Patch get_file_metadata to raise an exception
        with patch('fustor_source_fs.event_handler.get_file_metadata', side_effect=PermissionError("Permission denied")):
            # This should not raise an exception due to our error handling
            handler.on_moved(mock_event)
            
            # Method should complete without crashing


def test_event_handler_on_closed_exception_handling():
    """Test that on_closed method handles exceptions without terminating the program."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Create a mock event
        mock_event = MagicMock()
        mock_event.is_directory = False  # file, not directory
        mock_event.src_path = os.path.join(temp_dir, "test_file.txt")
        
        # Create the test file
        with open(mock_event.src_path, 'w') as f:
            f.write("test")
        
        # Patch get_file_metadata to raise an exception
        with patch('fustor_source_fs.event_handler.get_file_metadata', side_effect=PermissionError("Permission denied")):
            # This should not raise an exception due to our error handling
            handler.on_closed(mock_event)
            
            # Method should complete without crashing


def test_get_file_metadata_exception_handling():
    """Test that get_file_metadata handles exceptions properly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # Test with non-existent file
        result = get_file_metadata(os.path.join(temp_dir, "nonexistent.txt"))
        assert result is None
        
        # The function should handle file metadata retrieval gracefully
        # with proper exception handling internally
        result = get_file_metadata(test_file)
        assert result is not None  # Should return metadata for existing file


def test_touch_recursive_bottom_up_with_real_exception():
    """Test touch recursive bottom up method handles real file system exceptions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create nested directory structure
        nested_dir = os.path.join(temp_dir, "level1", "level2", "level3")
        os.makedirs(nested_dir)
        
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Test that the method completes without exception
        # The method already has error handling for os.walk via the onerror callback
        handler._touch_recursive_bottom_up(nested_dir)
        # Method should complete without crashing


def test_generate_move_events_recursive_with_real_exception():
    """Test generate move events recursive method handles real file system exceptions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create directory structure
        source_dir = os.path.join(temp_dir, "source")
        dest_dir = os.path.join(temp_dir, "dest")
        os.makedirs(source_dir)
        os.makedirs(dest_dir)
        
        # Create event queue and watch manager
        event_queue = queue.Queue()
        watch_manager = _WatchManager(temp_dir, event_handler=None)
        
        # Create event handler
        handler = OptimizedWatchEventHandler(event_queue, watch_manager)
        
        # Create a test file in dest directory
        test_file = os.path.join(dest_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # Test that the method completes without exception
        # Even if some files can't be processed, it should continue
        handler._generate_move_events_recursive(source_dir, dest_dir)
        # Method should complete without crashing even if some files fail


def test_get_file_metadata_with_real_exception():
    """Test get_file_metadata handles real file system exceptions."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_file = os.path.join(temp_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # Test with non-existent file - should return None
        result = get_file_metadata(os.path.join(temp_dir, "nonexistent.txt"))
        assert result is None
        
        # Test with valid file - should return metadata
        result = get_file_metadata(test_file)
        assert result is not None
        assert result["path"] == test_file


if __name__ == "__main__":
    pytest.main([__file__, "-v"])