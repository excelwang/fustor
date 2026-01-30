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
        # Give some time for background mkdir events to settle
        time.sleep(1.0)
        
        # Use a real observer but mock the queue to check what's being put
        with patch.object(driver.watch_manager.inotify, 'remove_watch') as mock_remove_watch:
            # Clear any background-triggered entries to start clean
            driver.watch_manager.lru_cache.cache.clear()
            driver.watch_manager.lru_cache.min_heap = []
            driver.watch_manager.lru_cache.removed_from_heap.clear()
            
            # Act
            # 1. Fill the watch manager up to its limit
            base_ts = driver._logical_clock.get_watermark() + 1000
            for i in range(watch_limit):
                timestamp = base_ts + i # Logical time
                driver._logical_clock.update(timestamp)
                driver.watch_manager.schedule(str(dirs[i]), timestamp)

            # 2. Access some to change LRU order. 0 is now the most recent.
            driver.watch_manager.touch(str(dirs[0]))

            # 3. Add one more directory, which should trigger pruning of dir[1]
            lru_path_to_be_evicted = str(dirs[1])
            trigger_ts = base_ts + 4000 # Much larger to ensure scheduled
            driver._logical_clock.update(trigger_ts)
            driver.watch_manager.schedule(str(dirs[watch_limit]), trigger_ts)

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

        # Use consistent timestamps.
        base_time = 1000000000.0
        with patch.object(watch_manager, '_get_current_time', return_value=base_time):
            # Act
            # Schedule 5 watches successfully
            for i in range(5):
                (tmp_path / f"dir{i}").mkdir()
                watch_manager.schedule(str(tmp_path / f"dir{i}"), base_time - 100.0) # 100s old
            
            assert len(watch_manager.lru_cache.cache) == 5
            
            # The 6th call will trigger the OSError, then the recursive call will be the 7th.
            # We use a timestamp (-50s) that is NEWER than the existing ones (-100s)
            # This ensures that when the limit restricts (or LRU kicks in), this one survives
            # and potentially evicts an older one (like dir0).
            (tmp_path / "dir5").mkdir()
            watch_manager.schedule(str(tmp_path / "dir5"), base_time - 50.0)

    # Assert
    # The limit should be adjusted down to 5.
    assert watch_manager.watch_limit == 5
    # After the recursive call and LRU eviction, the number of watches should still be 5.
    assert len(watch_manager.lru_cache.cache) == 5
    # The new watch for dir5 should be present because it is newer (-50 vs -100).
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

        # Use monotonic-compatible values. Let's assume current time is 1000000.
        current_monotonic = 1000000.0
        five_days_seconds = 5 * 86400
        
        # Mock _get_current_time to return our controlled monotonic time
        with patch.object(watch_manager, '_get_current_time', return_value=current_monotonic):
            
            # 1. Schedule the two watches. dir1 is now the LRU item (5 days old relative to current).
            watch_manager.schedule(str(dir1), current_monotonic - five_days_seconds)
            watch_manager.schedule(str(dir2), current_monotonic - five_days_seconds + 1)
    
            # 2. Schedule a third watch to trigger eviction of the oldest one (dir1).
            watch_manager.schedule(str(dir3), current_monotonic)

        # Assert
        # Check that the eviction log was created and contains the correct age.
        assert "Watch limit reached. Evicting watch" in caplog.text
        assert "(relative age: 5.00 days)" in caplog.text
        assert str(dir1) in caplog.text

    finally:
        watch_manager.stop()

def test_min_monitoring_window_raises_error(fs_config: SourceConfig, tmp_path: Path, caplog):
    """
    Tests that if an evicted watch is newer than the min_monitoring_window_days threshold,
    the driver logs an error and sets the stop_driver_event.
    """
    # Arrange
    caplog.set_level(logging.ERROR) # Capture ERROR logs
    watch_limit = 2
    min_window_days = 10

    driver = FSDriver('test-fs-id', fs_config)
    watch_manager = driver.watch_manager
    watch_manager.watch_limit = watch_limit
    watch_manager.min_monitoring_window_days = min_window_days
    watch_manager.start()

    base_time = 1000000000.0
    with patch.object(watch_manager, '_get_current_time', return_value=base_time):
        # We also need to patch add_watch to trigger the OSError(28)
        with patch.object(watch_manager.inotify, 'add_watch', side_effect=OSError(28, "No space")):
            newest_mtime = base_time - 86400  # 1 day ago
            evicted_mtime = base_time - (5 * 86400)  # 5 days ago
            trigger_mtime = base_time  # Now

            dir1 = tmp_path / "dir1"  # This will be evicted (5 days old)
            dir2 = tmp_path / "dir2"  # This is the newest in the initial set (1 day old)
            dir3 = tmp_path / "dir3"  # This triggers the eviction (now)
            dir1.mkdir()
            dir2.mkdir()
            dir3.mkdir()

            # Pre-fill cache (these won't trigger error because we manually put them or assumption is they are already there)
            # Actually, since we patched add_watch to ALWAYS fail, we must manually populate the cache
            # to simulate the state BEFORE the failure.
            watch_manager.lru_cache.put(str(dir2), Mock(timestamp=newest_mtime))
            watch_manager.lru_cache.put(str(dir1), Mock(timestamp=evicted_mtime))
            
            # Act & Assert
            # This schedule will fail with errno 28.
            # The code will check relative age of the NEW watch (dir3).
            # relative_age = (base_time - base_time) = 0 days.
            # Since 0 < 10 (min_window_days), it should trigger the error path.
            with pytest.raises(DriverError):
                watch_manager.schedule(str(dir3), trigger_mtime)

        # Check that the stop_event was set (meaning the driver was told to stop)
        assert driver.watch_manager.stop_driver_event.is_set()

        # Check for the specific error message in the logs
        error_log_found = False
        expected_error_substring = (
            f"Watch limit reached and an active watch for {str(dir1)} "
            f"(relative age: 5.00 days) is about to be evicted. "
            f"This is below the configured min_monitoring_window_days ({min_window_days} days). "
            f"Stopping driver to prevent data loss."
        )
        for record in caplog.records:
            if record.levelno == logging.ERROR and expected_error_substring in record.message:
                error_log_found = True
                break
        assert error_log_found, f"Expected error log not found. Logs: {caplog.text}"

        watch_manager.stop()