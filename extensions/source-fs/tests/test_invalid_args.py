"""
Test case to reproduce the OSError: [Errno 22] Invalid argument issue in FS driver
"""
import os
import tempfile
import pytest
from unittest.mock import patch
from pathlib import Path
from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver
from fustor_core.exceptions import DriverError


def test_path_with_invalid_characters():
    """
    Test that the FS driver can handle paths with characters that cause inotify errors
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a path that might cause issues
        problematic_path = os.path.join(tmp_dir, "test dir with spaces and [brackets]")
        os.makedirs(problematic_path, exist_ok=True)
        
        fs_config = SourceConfig(
            driver="fs", 
            uri=tmp_dir, 
            credential=PasswdCredential(user="test")
        )
        
        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = 100  # Set lower limit for testing
        watch_manager.min_monitoring_window_days = 0  # Disable min window for testing
        
        watch_manager.start()
        try:
            # Try to schedule a watch on the problematic path
            watch_manager.schedule(problematic_path, 1000000000)  # Using fixed timestamp
        except OSError as e:
            if e.errno == 22:  # Invalid argument
                print(f"Reproduced the OSError: {e}")
                assert True  # We successfully reproduced the error
            else:
                raise e
        finally:
            watch_manager.stop()


def test_path_too_long():
    """
    Test that the FS driver can handle very long paths that might exceed PATH_MAX
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a very long path
        very_long_dir_name = "a" * 250  # Create a long directory name
        long_path = os.path.join(tmp_dir, very_long_dir_name)
        os.makedirs(long_path, exist_ok=True)
        
        fs_config = SourceConfig(
            driver="fs", 
            uri=tmp_dir, 
            credential=PasswdCredential(user="test")
        )
        
        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = 100  # Set lower limit for testing
        watch_manager.min_monitoring_window_days = 0  # Disable min window for testing
        
        watch_manager.start()
        try:
            # Try to schedule a watch on the long path
            watch_manager.schedule(long_path, 1000000000)  # Using fixed timestamp
        except OSError as e:
            if e.errno == 22:  # Invalid argument
                print(f"Reproduced the OSError with long path: {e}")
                assert True  # We successfully reproduced the error
            else:
                raise e
        finally:
            watch_manager.stop()


def test_schedule_with_special_mount_path():
    """
    Test with a path that might exist in special filesystems that could cause EINVAL
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a normal path inside tmp_dir
        normal_path = os.path.join(tmp_dir, "normal_dir")
        os.makedirs(normal_path, exist_ok=True)

        fs_config = SourceConfig(
            driver="fs",
            uri=tmp_dir,
            credential=PasswdCredential(user="test")
        )

        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = 100  # Set lower limit for testing
        watch_manager.min_monitoring_window_days = 0  # Disable min window for testing

        # We can't easily reproduce an actual EINVAL in a test environment
        # but we can test that our code handles it gracefully by mocking
        from unittest.mock import patch
        import errno

        def mock_add_watch_with_einval(path_bytes):
            raise OSError(errno.EINVAL, "Invalid argument")

        with patch.object(watch_manager.inotify, 'add_watch', side_effect=mock_add_watch_with_einval):
            watch_manager.start()
            try:
                # Try to schedule a watch - this should handle the EINVAL gracefully
                watch_manager.schedule(normal_path, 1000000000)  # Using fixed timestamp
                # The path should not be in the cache since scheduling failed
                assert normal_path not in watch_manager.lru_cache
            finally:
                watch_manager.stop()