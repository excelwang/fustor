import time
import logging
import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver
from fustor_source_fs.components import _WatchManager
from fustor_core.exceptions import DriverError


@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    """Provides a SourceConfig pointing to a temporary directory."""
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))


def test_lru_pruning_and_cascading_unschedule(fs_config: SourceConfig, tmp_path: Path):
    """Tests that the WatchManager correctly prunes the least recently used watch
    when the watch limit is reached and that unscheduling is recursive.
    """
    # Arrange
    # Set a very low watch limit to easily test pruning
    watch_limit = 5
    driver = FSDriver('test-fs-id', fs_config)
    # Manually set the watch_limit on the manager for this test, since it's no longer a constructor param
    driver.watch_manager.watch_limit = watch_limit
    driver.watch_manager.min_monitoring_window_days = 0
    driver.watch_manager.start()

    try:
        # Create more directories than the limit
        dirs = [tmp_path / f"dir{i}" for i in range(watch_limit + 2)]
        for d in dirs:
            d.mkdir()
            (d / "nested").mkdir()  # Add nested dirs for cascading test

        # Use a real observer but mock the queue to check what's being put
        with patch.object(driver.watch_manager.inotify, 'remove_watch') as mock_remove_watch:
            # Act
            # 1. Fill the watch manager up to its limit
            for i in range(watch_limit):
                driver.watch_manager.schedule(str(dirs[i]), time.time())
                time.sleep(0.1)

            # 2. Access some to change LRU order. 0 is now the most recent.
            driver.watch_manager.touch(str(dirs[0]))

            # 3. Add one more directory, which should trigger pruning of dir[1]
            lru_path_to_be_evicted = str(dirs[1])
            driver.watch_manager.schedule(str(dirs[watch_limit]), time.time())

            # Give event processing thread time to process the event
            time.sleep(0.5)

            # Assert
            # Check that inotify.remove_watch was called with the evicted path (encoded)
            remove_calls = [call.args[0] for call in mock_remove_watch.call_args_list]
            assert lru_path_to_be_evicted.encode('utf-8') in remove_calls

            # Check that the new directory was added and the old one removed
            assert str(dirs[watch_limit]) in driver.watch_manager.lru_cache.cache
            assert lru_path_to_be_evicted not in driver.watch_manager.lru_cache.cache
    finally:
        driver.watch_manager.stop()

def test_directory_creation_and_deletion_updates_watches(fs_config: SourceConfig, tmp_path: Path):
    """Tests that creating and deleting directories dynamically updates the watches."""
    driver = FSDriver('test-fs-id', fs_config)
    driver.watch_manager.start()

    try:
        # Arrange
        # Schedule a watch on the root temp path to receive events
        driver.watch_manager.schedule(str(tmp_path), time.time())

        new_dir = tmp_path / "new_dynamic_dir"
        nested_dir = new_dir / "nested"

        # Act & Assert: Directory Creation (Step 1)
        new_dir.mkdir()
        # Wait for watchdog to process the creation of new_dir
        timeout = 5
        start_time = time.time()
        while str(new_dir) not in driver.watch_manager.lru_cache and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        assert str(new_dir) in driver.watch_manager.lru_cache

        # Act & Assert: Directory Creation (Step 2)
        # Now that new_dir is watched, create the nested directory
        nested_dir.mkdir()
        # Wait for watchdog to process the creation of nested_dir
        start_time = time.time()
        while str(nested_dir) not in driver.watch_manager.lru_cache and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        assert str(nested_dir) in driver.watch_manager.lru_cache

        # Act & Assert: Directory Deletion (recursive)
        import shutil
        shutil.rmtree(new_dir)
        # Wait for watches to be unscheduled
        start_time = time.time()
        while (str(new_dir) in driver.watch_manager.lru_cache or str(nested_dir) in driver.watch_manager.lru_cache) and (time.time() - start_time) < timeout:
            time.sleep(0.1)

        assert str(new_dir) not in driver.watch_manager.lru_cache
        assert str(nested_dir) not in driver.watch_manager.lru_cache

    finally:
        # Ensure the watch manager is stopped, even if assertions fail
        driver.watch_manager.stop()

def test_dynamic_watch_limit_adjustment_on_error(fs_config: SourceConfig, tmp_path: Path, caplog):
    """
    Tests that the watch_limit is dynamically adjusted down when an OSError
    with errno 28 (inotify limit reached) is caught, and that the schedule is retried.
    """
    # Arrange
    caplog.set_level(logging.INFO) # Capture INFO for eviction log and WARNING for limit log
    driver = FSDriver('test-fs-id', fs_config)
    watch_manager = driver.watch_manager
    watch_manager.min_monitoring_window_days = 0
    initial_limit = watch_manager.watch_limit
    assert initial_limit > 10

    successful_watch = Mock()
    error_to_raise = OSError(28, "No space left on device")
    
    # Use a mutable object (list) to track call count inside the closure
    call_count = []

    def schedule_side_effect(*args, **kwargs):
        call_count.append(1)
        # The 6th call should fail
        if len(call_count) == 6:
            raise error_to_raise
        # All other calls (including the 7th recursive one) should succeed
        else:
            return successful_watch

    with patch.object(watch_manager.inotify, 'add_watch') as mock_add_watch:
        mock_add_watch.side_effect = schedule_side_effect

        # Act
        # Schedule 5 watches successfully
        for i in range(5):
            (tmp_path / f"dir{i}").mkdir()
            watch_manager.schedule(str(tmp_path / f"dir{i}"), time.time())
        
        assert len(watch_manager.lru_cache.cache) == 5
        
        # The 6th call will trigger the OSError, then the recursive call will be the 7th.
        (tmp_path / "dir5").mkdir()
        watch_manager.schedule(str(tmp_path / "dir5"), time.time())

    # Assert
    # The limit should be adjusted down to 5.
    assert watch_manager.watch_limit == 5
    # After the recursive call and LRU eviction, the number of watches should still be 5.
    assert len(watch_manager.lru_cache.cache) == 5
    # The new watch for dir5 should be present.
    assert str(tmp_path / "dir5") in watch_manager.lru_cache
    
    # Check that a warning was logged for the limit adjustment
    assert "System inotify watch limit hit" in caplog.text
    # Check that the eviction log was also created during the retry
    assert "Watch limit reached. Evicting watch" in caplog.text

def test_eviction_log_shows_correct_age(fs_config: SourceConfig, tmp_path: Path, caplog):
    """
    Tests that the INFO log for an evicted watch shows its correct inactive age.
    """

    try:
        # Create dirs and manually set their mtime to create a predictable age
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir3 = tmp_path / "dir3"
        dir1.mkdir()
        dir2.mkdir()
        dir3.mkdir()
        # Arrange
        caplog.set_level(logging.INFO)
        watch_limit = 2
        driver = FSDriver('test-fs-id', fs_config)
        watch_manager = driver.watch_manager
        watch_manager.watch_limit = watch_limit
        watch_manager.min_monitoring_window_days = 0
        watch_manager.start()

        # Set dir1's mtime to be 5 days in the past
        five_days_ago = time.time() - (5 * 86400)
        os.utime(dir1, (five_days_ago, five_days_ago))
        # Update dir2's mtime to be newer than dir1
        os.utime(dir2, (five_days_ago + 1, five_days_ago + 1))

        # Act
        # 1. Schedule the two watches. dir1 is now the LRU item.
        watch_manager.schedule(str(dir1), five_days_ago)
        watch_manager.schedule(str(dir2), five_days_ago + 1)

        # 2. Schedule a third watch to trigger eviction of the oldest one (dir1).
        watch_manager.schedule(str(dir3), time.time())

        # Assert
        # Check that the eviction log was created and contains the correct age.
        assert "Watch limit reached. Evicting watch" in caplog.text
        assert "(relative age: 5.00 days)" in caplog.text
        assert str(dir1) in caplog.text

    finally:
        watch_manager.stop()

def test_min_monitoring_window_raises_error(fs_config: SourceConfig, tmp_path: Path):
    """
    Tests that a DriverError is raised if an evicted watch is newer than the
    min_monitoring_window_days threshold.
    """
    # Arrange
    watch_limit = 2
    min_window_days = 10

    driver = FSDriver('test-fs-id', fs_config)
    watch_manager = driver.watch_manager
    watch_manager.watch_limit = watch_limit
    watch_manager.min_monitoring_window_days = min_window_days

    with patch('time.time') as mock_time:
        # Use distinct timestamps to ensure heap ordering is unambiguous
        base_time = 1000000000
        mock_time.return_value = base_time

        # Timestamps for the test scenario
        newest_mtime = base_time - 86400  # 1 day ago
        evicted_mtime = base_time - (5 * 86400)  # 5 days ago
        trigger_mtime = base_time  # Now

        dir1 = tmp_path / "dir1"  # This will be evicted (5 days old)
        dir2 = tmp_path / "dir2"  # This is the newest in the initial set (1 day old)
        dir3 = tmp_path / "dir3"  # This triggers the eviction (now)
        dir1.mkdir()
        dir2.mkdir()
        dir3.mkdir()

        watch_manager.start()

        # Schedule the watches. dir1 is the oldest.
        watch_manager.schedule(str(dir2), newest_mtime)
        watch_manager.schedule(str(dir1), evicted_mtime)

        # Act & Assert
        with pytest.raises(DriverError) as excinfo:
            # This schedule will be newer than dir2, triggering the eviction of dir1.
            # The relative age of dir1 is (base_time - evicted_mtime) = 5 days.
            # Since 5 < 10 (min_window_days), it should raise an error.
            watch_manager.schedule(str(dir3), trigger_mtime)

    # Check the exception message for details
    assert f"(relative age: 5.00 days)" in str(excinfo.value)
    assert f"This is below the configured min_monitoring_window_days ({min_window_days} days)" in str(excinfo.value)

    watch_manager.stop()