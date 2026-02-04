"""
Test case to verify that the FS driver properly handles OSError: [Errno 22] Invalid argument
"""
import os
import tempfile
from unittest.mock import patch, MagicMock
import pytest
from pathlib import Path
from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver
from fustor_core.exceptions import DriverError


def test_schedule_handles_errno_22_gracefully(monkeypatch):
    """
    Test that when inotify.add_watch raises OSError with errno 22,
    the schedule method handles it gracefully by logging a warning and returning.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a test directory
        test_path = os.path.join(tmp_dir, "test_dir")
        os.makedirs(test_path, exist_ok=True)
        
        fs_config = SourceConfig(
            driver="fs", 
            uri=tmp_dir, 
            credential=PasswdCredential(user="test")
        )
        
        # Create the driver
        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = 100  # Set lower limit for testing
        watch_manager.min_monitoring_window_days = 0  # Disable min window for testing
        
        # Mock the inotify.add_watch to raise OSError with errno 22
        def mock_add_watch(path):
            import errno
            raise OSError(errno.EINVAL, "Invalid argument")
        
        monkeypatch.setattr(watch_manager.inotify, 'add_watch', mock_add_watch)
        
        # Start the watch manager
        watch_manager.start()
        
        try:
            # This should not raise an exception, but should log a warning
            # and return without adding the path to the cache
            watch_manager.schedule(test_path, 1000000000)  # Using fixed timestamp
            
            # The path should not be in the cache since scheduling failed
            assert test_path not in watch_manager.lru_cache
        finally:
            watch_manager.stop()


def test_schedule_handles_errno_22_with_different_paths():
    """
    Test that errno 22 is handled for different types of paths.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create different test paths
        paths = []
        for i in range(3):
            test_path = os.path.join(tmp_dir, f"test_dir_{i}")
            os.makedirs(test_path, exist_ok=True)
            paths.append(test_path)
        
        fs_config = SourceConfig(
            driver="fs", 
            uri=tmp_dir, 
            credential=PasswdCredential(user="test")
        )
        
        # Create the driver
        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = 100  # Set lower limit for testing
        watch_manager.min_monitoring_window_days = 0  # Disable min window for testing

        # Mock the inotify.add_watch to raise OSError with errno 22
        import errno
        original_add_watch = watch_manager.inotify.add_watch
        def mock_add_watch(path):
            raise OSError(errno.EINVAL, "Invalid argument")
        
        watch_manager.inotify.add_watch = mock_add_watch
        
        # Start the watch manager
        watch_manager.start()
        
        try:
            # Schedule multiple paths - all should fail gracefully
            for path in paths:
                watch_manager.schedule(path, 1000000000)  # Using fixed timestamp
                # Each path should not be in the cache
                assert path not in watch_manager.lru_cache
        finally:
            watch_manager.inotify.add_watch = original_add_watch  # Restore original
            watch_manager.stop()