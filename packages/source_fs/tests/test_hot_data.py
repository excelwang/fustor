import os
import time
import threading
import queue
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver
from fustor_event_model.models import UpdateEvent

@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))

def test_snapshot_postpones_hot_files(tmp_path: Path, fs_config: SourceConfig):
    # Create an old file
    old_file = tmp_path / "old.txt"
    old_file.write_text("old")
    # Set mtime to 1 hour ago
    os.utime(old_file, (time.time() - 3600, time.time() - 3600))

    # Create a hot file
    hot_file = tmp_path / "hot.txt"
    hot_file.write_text("hot")
    # Current time (will be hot if cooloff is 61s)

    driver_config = fs_config
    driver_config.driver_params["hot_data_cooloff_seconds"] = 10.0
    driver = FSDriver('test-fs', driver_config)

    # Act
    iterator = driver.get_snapshot_iterator()
    events = list(iterator)

    # Assert
    all_rows = []
    for event in events:
        if isinstance(event, UpdateEvent):
            all_rows.extend(event.rows)
    
    paths = {r['file_path'] for r in all_rows}
    assert str(old_file) in paths
    assert str(hot_file) not in paths # Should have been postponed and still hot during look-back

def test_snapshot_lookback_picks_up_cooled_files(tmp_path: Path, fs_config: SourceConfig, mocker):
    # Create a file
    file_path = tmp_path / "maybe_hot.txt"
    file_path.write_text("content")
    
    start_time = time.time()
    # Set file mtime to exactly start_time
    os.utime(file_path, (start_time, start_time))

    # Mock time.time() and hybrid_now()
    # Note: hybrid_now() uses time.time() internally if logical clock is 0
    # But FSDriver.get_snapshot_iterator uses self._logical_clock.hybrid_now()
    
    # We want hybrid_now to return start_time during scanning, 
    # and start_time + 10 during lookback.
    
    # Let's mock hybrid_now directly on the driver's clock instance
    driver_config = fs_config
    driver_config.driver_params["hot_data_cooloff_seconds"] = 5.0
    driver = FSDriver('test-fs-lookback', driver_config)
    
    # Custom side effect
    call_history = []
    def hybrid_now_side_effect(*args, **kwargs):
        # Call 0: Worker check (returns start_time) -> Hot
        # Call 1: Look-back check (returns start_time + 10) -> Cool
        val = start_time if len(call_history) < 1 else start_time + 10.0
        call_history.append(val)
        return val

    mocker.patch.object(driver._logical_clock, 'hybrid_now', side_effect=hybrid_now_side_effect)

    # Act
    iterator = driver.get_snapshot_iterator()
    events = list(iterator)

    # Assert
    all_rows = []
    for event in events:
        if isinstance(event, UpdateEvent):
            all_rows.extend(event.rows)
    
    paths = {r['file_path'] for r in all_rows}
    assert str(file_path) in paths 

def test_audit_postpones_hot_files_and_marks_skipped(tmp_path: Path, fs_config: SourceConfig, mocker):
    # Create a file
    file_path = tmp_path / "still_hot.txt"
    file_path.write_text("content")
    
    start_time = time.time()
    os.utime(file_path, (start_time, start_time))

    driver_config = fs_config
    driver_config.driver_params["hot_data_cooloff_seconds"] = 5.0
    driver = FSDriver('test-fs-audit', driver_config)
    
    # Mock hybrid_now to ALWAYS return start_time (even in look-back)
    # This simulates a file that is still hot even after look-back finishes.
    mocker.patch.object(driver._logical_clock, 'hybrid_now', return_value=start_time)

    # Act
    iterator = driver.get_audit_iterator()
    events_tuples = list(iterator)

    # Assert
    all_rows = []
    for event, _ in events_tuples:
        if isinstance(event, UpdateEvent):
            all_rows.extend(event.rows)
    
    target_row = next((r for r in all_rows if r['file_path'] == str(file_path)), None)
    assert target_row is not None
    assert target_row.get('audit_skipped') is True # Rule: Still hot after look-back? mark skipped.
